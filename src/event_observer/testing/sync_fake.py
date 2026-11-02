"""Blocking synthetic readers sharing publications with the async FakeTransport."""

from __future__ import annotations

from collections import deque
import threading
import time
from typing import TYPE_CHECKING

from ..errors import (
    ObserverStartupTimeoutError,
    ObserverCleanupTimeoutError,
    TransportBackpressureError,
    TransportError,
    UnexpectedStreamEndError,
    UnsupportedCapabilityError,
)
from ..journal import raw_size
from ..transports.sync_base import validate_poll_timeout
from ..models import (
    ObservationBoundary,
    OpenContext,
    RawEvent,
    StartPosition,
    TransportReadiness,
    TransportSource,
)

if TYPE_CHECKING:
    from .fake import FakeTransport


class SyncFakeTransport:
    """Create blocking sessions over one thread-safe synthetic publication source.

    :ivar owner: Original fake supplying publications, failures, and lifecycle gates.
    :ivar transport_name: Original adapter class name used for report context.
    :ivar capabilities: Capabilities declared by the original adapter.
    """

    def __init__(self, owner: FakeTransport) -> None:
        """Retain the source without opening a reader.

        :param owner: Fake whose history and sessions are protected by one shared lock.
        """
        self.owner = owner
        self.transport_name = type(owner).__name__
        self.capabilities = owner.capabilities

    def open(self, source: TransportSource, start: StartPosition, *, context: OpenContext) -> SyncFakeSession:
        """Open a latest or retained-history reader under the startup deadline.

        :param source: Synthetic source to observe.
        :param start: Latest or earliest boundary.
        :param context: Shared clock, identity, and handoff limits.
        :returns: A ready blocking reader.
        :raises UnsupportedCapabilityError: If the start mode is unsupported.
        :raises ObserverStartupTimeoutError: If the readiness gate does not open in time.
        """
        if start.mode.value not in self.capabilities.start_modes:
            raise UnsupportedCapabilityError("FakeTransport supports latest and earliest")
        remaining = max(0.0, context.startup_deadline - context.clock.monotonic())
        deadline = time.monotonic() + remaining
        while self.owner.ready_gate is not None and not self.owner.ready_gate.is_set():
            if time.monotonic() >= deadline:
                raise ObserverStartupTimeoutError("Fake readiness deadline expired")
            self.owner.ready_gate.wait(min(0.01, max(0.0, deadline - time.monotonic())))
        with self.owner._lock:
            session = SyncFakeSession(self.owner, source, context)
            self.owner.sessions.append(session)
            self.owner.open_count += 1
            if start.mode.value == "earliest":
                for event in self.owner._history:
                    if event.source == source.name:
                        session._push(event)
            now = context.clock.monotonic()
            session.readiness = TransportReadiness(
                ObservationBoundary(source.name, start.mode.value, {0: len(self.owner._history)}, now),
                self.capabilities,
                {},
                now,
            )
            return session


class SyncFakeSession:
    """Provide a finite blocking handoff without creating tasks or an event loop.

    :ivar adapter: Synthetic source owning the publication lock.
    :ivar source: Normalized source selection.
    :ivar context: Runtime clock and buffer budgets.
    :ivar _condition: Publication/failure notification sharing the source lock.
    :ivar _buffer: Bounded pending raw events with accounted sizes.
    :ivar _bytes: Accounted bytes in the handoff.
    :ivar _failure: Persistent injected or handoff failure.
    :ivar closed: Whether intentional cleanup completed.
    :ivar ended: Whether the source ended unexpectedly.
    :ivar readiness: Confirmed source boundary and capabilities.
    """

    def __init__(self, adapter: FakeTransport, source: TransportSource, context: OpenContext) -> None:
        """Initialize a session on its owning runtime thread.

        :param adapter: Shared synthetic source.
        :param source: Selected source.
        :param context: Clock and bounded buffer settings.
        """
        self.adapter, self.source, self.context = adapter, source, context
        self._condition = threading.Condition(adapter._lock)
        self._buffer: deque[tuple[RawEvent, int]] = deque()
        self._bytes = 0
        self._failure: TransportError | None = None
        self.closed = self.ended = False
        self.readiness: TransportReadiness

    def _notify(self) -> None:
        """Wake finite pollers while the publication lock is held."""
        self._condition.notify_all()

    def _push(self, raw: RawEvent) -> None:
        """Publish a detached envelope or record explicit handoff exhaustion.

        :param raw: Synthetic publication delivered under the source lock.
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

    def failure(self):
        """Inspect continuity independently of queued business messages.

        :returns: Persistent failure, unexpected end, or None.
        """
        with self.adapter._lock:
            if self._failure is not None:
                return self._failure
            if self.ended and not self.closed:
                return UnexpectedStreamEndError("Fake receive stream ended unexpectedly")
            return None

    def poll(self, timeout: float) -> RawEvent | None:
        """Wait for at most one event while preserving failure-before-data priority.

        :param timeout: Finite nonnegative receive budget in seconds.
        :returns: One queued event, or None when idle/closed.
        :raises TransportBackpressureError: If the handoff overflowed.
        :raises UnexpectedStreamEndError: If the source ended unexpectedly.
        """
        validate_poll_timeout(timeout)
        with self._condition:
            if not self._buffer and self.failure() is None and not self.closed:
                self._condition.wait(max(0.0, timeout))
            error = self.failure()
            if error is not None:
                raise error
            if self.closed or not self._buffer:
                return None
            raw, size = self._buffer.popleft()
            self._bytes -= size
            return raw

    def close(self, *, deadline: float) -> None:
        """Close once after a controlled gate, bounded even with an injected clock.

        :param deadline: Absolute cleanup deadline in the shared clock domain.
        :raises ObserverCleanupTimeoutError: If the cleanup gate remains closed.
        """
        remaining = max(0.0, deadline - self.context.clock.monotonic())
        end = time.monotonic() + remaining
        while self.adapter.close_gate is not None and not self.adapter.close_gate.is_set():
            if time.monotonic() >= end:
                raise ObserverCleanupTimeoutError("Fake cleanup deadline expired")
            self.adapter.close_gate.wait(min(0.01, max(0.0, end - time.monotonic())))
        with self._condition:
            if not self.closed:
                self.closed = True
                self.adapter.close_count += 1
                self._buffer.clear()
                self._bytes = 0
                self._notify()
