"""Deterministic in-memory adapter, also usable from sync test threads.

publish/fail/end set a single wakeup per session, never one scheduled callback
per record. Each session has independent bounded buffers and cursor.
"""

from __future__ import annotations

import asyncio
from collections import deque
import threading
import time
from typing import AsyncIterator, TYPE_CHECKING

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

if TYPE_CHECKING:
    from .sync_fake import SyncFakeSession


class FakeTransport:
    """Provide a thread-safe deterministic test adapter with independent bounded session handoffs.

    :ivar capabilities: Declared latest/earliest, raw-envelope preservation, and independent-reader
        semantics.
    :ivar _history: Synthetic publication history retained for explicit earliest replay.
    :ivar _lock: Reentrant lock protecting publication, session state, and cross-thread wakeups.
    :ivar sessions: Sessions created by this adapter, retained for test inspection and harness cleanup.
    :ivar ready_gate: Optional thread event delaying open until a test releases readiness.
    :ivar close_gate: Optional thread event delaying session close for cleanup fault tests.
    :ivar open_count: Number of sessions successfully constructed.
    :ivar close_count: Number of sessions closed for the first time.
    """

    capabilities = TransportCapabilities()

    def __init__(
        self,
        events: tuple[RawEvent, ...] = (),
        *,
        ready: threading.Event | None = None,
        close_gate: threading.Event | None = None,
    ) -> None:
        """Initialize synthetic history and optional externally controlled lifecycle barriers.

        :param events: Initial raw history available to earliest readers.
        :param ready: Optional thread event that must be set before open completes.
        :param close_gate: Optional thread event that must be set before close completes.
        """
        self._history = list(events)
        self._lock = threading.RLock()
        self.sessions: list[FakeSession | SyncFakeSession] = []
        self.ready_gate = ready
        self.close_gate = close_gate
        self.open_count = 0
        self.close_count = 0

    def sync_adapter(self):
        """Create a lazy blocking provider over the same synthetic publications.

        :returns: A blocking FakeTransport adapter with independent session buffers.
        """
        from .sync_fake import SyncFakeTransport

        return SyncFakeTransport(self)

    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> "FakeSession":
        """Create an independent session and optionally enqueue matching retained history.

        :param source: Exact synthetic source name to observe.
        :param start: Latest or earliest start policy.
        :param context: Owner-loop clock and bounded handoff settings.
        :returns: A prepared FakeSession with resolved readiness.
        :raises UnsupportedCapabilityError: The requested start mode is not declared by this fake adapter.
        """
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
        """Append synthetic history and deliver to every active session for the same source.

        :param payload: Bytes, null, or a complete RawEvent whose own envelope fields take precedence.
        :param source: Source name used when constructing an envelope from a bare payload.
        :param key: Optional native key used for a newly constructed envelope.
        :param headers: Ordered header pairs used for a newly constructed envelope.
        :param metadata: Optional scalar metadata used for a newly constructed envelope.
        """
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
        """Inject a persistent independent transport failure into every active session.

        :param error: Typed failure to expose, or None to create the default synthetic transport error.
        """
        with self._lock:
            for session in self.sessions:
                if not session.closed:
                    session._failure = error or TransportError("Injected transport failure")
                    session._notify()

    def end(self) -> None:
        """End active receive streams without manufacturing a transport failure signal."""
        with self._lock:
            for session in self.sessions:
                session.ended = True
                session._notify()


class FakeSession:
    """Own one synthetic handoff and independent failure state on its event loop.

    :ivar adapter: Transport supplying history, lifecycle barriers, and synchronization.
    :ivar source: Exact source selected by this session.
    :ivar context: Clock and buffer limits captured during opening.
    :ivar loop: Owner event loop receiving coalesced thread-safe wakeups.
    :ivar _wake: Async event notifying the receiver about data, failure, or closure.
    :ivar _pending_wakeup: Whether one cross-thread wakeup callback is already scheduled.
    :ivar _buffer: Bounded deque of raw envelopes and accounted sizes.
    :ivar _bytes: Accounted bytes currently buffered.
    :ivar _failure: Persistent typed injected or backpressure failure.
    :ivar closed: Whether resource cleanup completed.
    :ivar ended: Whether the test intentionally ended the receive stream.
    :ivar _receive_started: Whether the single permitted receive iterator has been requested.
    :ivar readiness: Preparation result assigned by the adapter before returning from open.
    """

    def __init__(self, adapter: FakeTransport, source: TransportSource, context: OpenContext) -> None:
        """Capture the owner loop and initialize empty bounded handoff state.

        :param adapter: Owning synthetic transport and synchronization lock.
        :param source: Source this session receives.
        :param context: Per-watch limits and clock.
        """
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
        """Schedule at most one pending thread-safe wakeup while the adapter lock is held."""
        if not self._pending_wakeup:
            self._pending_wakeup = True
            self.loop.call_soon_threadsafe(self._deliver_wakeup)

    def _deliver_wakeup(self) -> None:
        """Clear the coalescing flag and wake the owner-loop receiver under the adapter lock."""
        with self.adapter._lock:
            self._pending_wakeup = False
            self._wake.set()

    def _push(self, raw: RawEvent) -> None:
        """Enqueue one envelope or persist a typed backpressure failure when capacity is exhausted.

        :param raw: Synthetic raw delivery for this session; the adapter lock protects the handoff.
        """
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
        """Claim the session single receive iterator without starting a separate coroutine.

        :returns: An asynchronous iterator over this session handoff.
        :raises TransportSubscriptionError: A receive iterator was already requested.
        """
        if self._receive_started:
            raise TransportSubscriptionError("A fake session permits one receive iterator")
        self._receive_started = True
        return self._receive()

    async def _receive(self) -> AsyncIterator[RawEvent]:
        """Drain buffered events, waiting on coalesced notifications when the handoff is empty.

        :raises TransportError: The session has a persistent injected or backpressure failure.
        :yields: Raw events in synthetic delivery order until close or explicit end.
        """
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
        """Observe persistent failure independently of receive consumption or waiter cancellation.

        :returns: The typed failure, or None after clean close.
        """
        while True:
            with self.adapter._lock:
                if self._failure is not None:
                    return self._failure
                if self.closed:
                    return None
            await asyncio.sleep(0.001)

    async def close(self, *, deadline: float) -> None:
        """Honor the optional close barrier, clear the handoff, and count cleanup only once.

        :param deadline: Protocol-compatible absolute deadline; fault barriers deliberately rely on caller
            cancellation.
        """
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
    """Implement reusable adapter contracts using explicitly controlled synthetic publication.

    :ivar adapter: Fresh FakeTransport owned by this harness.
    """

    def __init__(self) -> None:
        """Create an empty synthetic adapter without network or background resources."""
        self.adapter = FakeTransport()

    async def create_source(self, *, case_id: str) -> TransportSource:
        """Name a synthetic source without creating external infrastructure.

        :param case_id: Unique contract case identifier used as the source name.
        :returns: A TransportSource for the supplied case.
        """
        return TransportSource(case_id)

    async def publish(
        self, source: TransportSource, payload: bytes | None, *, key=None, headers=()
    ) -> PublishReceipt:
        """Publish through the synthetic test-side path and confirm immediate acceptance.

        :param source: Synthetic source to publish to.
        :param payload: Bytes or null payload to publish.
        :param key: Optional native key.
        :param headers: Ordered header pairs, including duplicates and null values.
        :returns: An accepted PublishReceipt.
        """
        self.adapter.publish(payload, source=source.name, key=key, headers=headers)
        return PublishReceipt()

    async def cleanup(self) -> None:
        """Close every session created by the owned synthetic adapter."""
        for session in self.adapter.sessions:
            if not session.closed:
                result = session.close(deadline=time.monotonic() + 1)
                if result is not None:
                    await result
