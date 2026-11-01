"""Deterministic in-memory adapter, also usable from sync test threads.

publish/fail/end set a single wakeup per session, never one scheduled callback
per record. Each session has independent bounded buffers and cursor.
"""

from __future__ import annotations

import asyncio
from collections import deque
import threading
import time
from typing import AsyncIterator

from ..errors import (
    TransportBackpressureError,
    TransportError,
    TransportSubscriptionError,
    UnsupportedCapabilityError,
)
from ..journal import raw_size
from .harness import PublishReceipt
from ..models import (
    ObservationBoundary,
    OpenContext,
    RawEvent,
    StartPosition,
    TransportCapabilities,
    TransportReadiness,
    TransportSource,
)


class FakeTransport:
    capabilities = TransportCapabilities()

    def __init__(
        self,
        events: tuple[RawEvent, ...] = (),
        *,
        ready: threading.Event | None = None,
        close_gate: threading.Event | None = None,
    ) -> None:
        self._history = list(events)
        self._lock = threading.RLock()
        self.sessions: list[FakeSession] = []
        self.ready_gate = ready
        self.close_gate = close_gate
        self.open_count = 0
        self.close_count = 0

    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> "FakeSession":
        if start.mode.value not in self.capabilities.start_modes:
            raise UnsupportedCapabilityError("FakeTransport supports latest and earliest")
        while self.ready_gate is not None and not self.ready_gate.is_set():
            await asyncio.sleep(0.001)
        with self._lock:
            session = FakeSession(self, source, context)
            self.sessions.append(session)
            self.open_count += 1
            if start.mode.value == "earliest":
                for event in self._history:
                    if event.source == source.name:
                        session._push(event)
            now = context.clock.monotonic()
            session.readiness = TransportReadiness(
                ObservationBoundary(source.name, start.mode.value, {0: len(self._history)}, now),
                self.capabilities,
                {},
                now,
            )
        return session

    def publish(
        self,
        payload: bytes | None | RawEvent,
        *,
        source: str = "events",
        key: bytes | None = None,
        headers: tuple[tuple[str, bytes | None], ...] = (),
        metadata=None,
    ) -> None:
        raw = (
            payload
            if isinstance(payload, RawEvent)
            else RawEvent(payload, source, headers, key, metadata or {})
        )
        with self._lock:
            self._history.append(raw)
            for session in self.sessions:
                if not session.closed and session.source.name == raw.source:
                    session._push(raw)

    def fail(self, error: TransportError | None = None) -> None:
        with self._lock:
            for session in self.sessions:
                if not session.closed:
                    session._failure = error or TransportError("Injected transport failure")
                    session._notify()

    def end(self) -> None:
        with self._lock:
            for session in self.sessions:
                session.ended = True
                session._notify()


class FakeSession:
    def __init__(self, adapter: FakeTransport, source: TransportSource, context: OpenContext) -> None:
        self.adapter = adapter
        self.source = source
        self.context = context
        self.loop = asyncio.get_running_loop()
        self._wake = asyncio.Event()
        self._pending_wakeup = False
        self._buffer: deque[tuple[RawEvent, int]] = deque()
        self._bytes = 0
        self._failure: TransportError | None = None
        self.closed = False
        self.ended = False
        self._receive_started = False
        self.readiness: TransportReadiness

    def _notify(self) -> None:
        if not self._pending_wakeup:
            self._pending_wakeup = True
            self.loop.call_soon_threadsafe(self._deliver_wakeup)

    def _deliver_wakeup(self) -> None:
        with self.adapter._lock:
            self._pending_wakeup = False
            self._wake.set()

    def _push(self, raw: RawEvent) -> None:
        size = raw_size(raw)
        if (
            len(self._buffer) >= self.context.buffers.max_events
            or self._bytes + size > self.context.buffers.max_bytes
        ):
            self._failure = TransportBackpressureError("Fake handoff capacity exhausted")
        else:
            self._buffer.append((raw, size))
            self._bytes += size
        self._notify()

    def receive(self) -> AsyncIterator[RawEvent]:
        if self._receive_started:
            raise TransportSubscriptionError("A fake session permits one receive iterator")
        self._receive_started = True
        return self._receive()

    async def _receive(self) -> AsyncIterator[RawEvent]:
        while True:
            with self.adapter._lock:
                if self._failure is not None:
                    raise self._failure
                if self.closed or self.ended:
                    return
                if self._buffer:
                    raw, size = self._buffer.popleft()
                    self._bytes -= size
                else:
                    self._wake.clear()
                    raw = None
            if raw is None:
                await self._wake.wait()
            else:
                yield raw

    async def wait_failure(self) -> TransportError | None:
        # Independent signal, receive never consumes this state.
        while True:
            with self.adapter._lock:
                if self._failure is not None:
                    return self._failure
                if self.closed:
                    return None
            await asyncio.sleep(0.001)

    async def close(self, *, deadline: float) -> None:
        while self.adapter.close_gate is not None and not self.adapter.close_gate.is_set():
            await asyncio.sleep(0.001)
        with self.adapter._lock:
            if not self.closed:
                self.closed = True
                self.adapter.close_count += 1
                self._buffer.clear()
                self._bytes = 0
                self._notify()


class FakeAdapterHarness:
    def __init__(self) -> None:
        self.adapter = FakeTransport()

    async def create_source(self, *, case_id: str) -> TransportSource:
        return TransportSource(case_id)

    async def publish(
        self, source: TransportSource, payload: bytes | None, *, key=None, headers=()
    ) -> PublishReceipt:
        self.adapter.publish(payload, source=source.name, key=key, headers=headers)
        return PublishReceipt()

    async def cleanup(self) -> None:
        for session in self.adapter.sessions:
            await session.close(deadline=time.monotonic() + 1)
