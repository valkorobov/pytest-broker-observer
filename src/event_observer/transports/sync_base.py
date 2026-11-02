"""Blocking transport extension interfaces without an asynchronous runtime.

Native clients belong to their I/O worker. The observer actor consumes detached
deliveries and receives failures independently of a full delivery handoff.

:ivar __all__: Public structural blocking adapter, session, and settlement interfaces.
"""

from __future__ import annotations

import math
from typing import Protocol

from ..errors import ConfigurationError, TransportError
from ..models import (
    OpenContext,
    RawEvent,
    StartPosition,
    TransportCapabilities,
    TransportReadiness,
    TransportSource,
)
from .base import DeliveryOutcome


def validate_poll_timeout(timeout: float) -> None:
    """Require a finite, nonnegative relative blocking read budget.

    :param timeout: Maximum seconds the caller permits this poll to block.
    :returns: None when timeout is valid; zero requests an immediate poll.
    :raises ConfigurationError: If timeout is negative, nonnumeric, boolean, or nonfinite.
    """
    try:
        valid = (
            not isinstance(timeout, bool)
            and isinstance(timeout, (float, int))
            and math.isfinite(timeout)
            and timeout >= 0
        )
    except OverflowError:
        valid = False
    if not valid:
        raise ConfigurationError("Transport poll timeout must be finite and nonnegative")


class SyncTransportSession(Protocol):
    """One READY blocking reader with an independent terminal failure signal.

    :ivar readiness: Resolved source boundary, capabilities, and startup metadata.
    """

    @property
    def readiness(self) -> TransportReadiness:
        """Return the successfully resolved startup contract.

        :returns: The independent reader's immutable READY boundary and metadata.
        :raises TransportReadinessError: If startup has not completed successfully.
        """
        ...

    def poll(self, timeout: float) -> RawEvent | None:
        """Return at most one detached delivery within a finite blocking budget.

        Known failure takes precedence over buffered deliveries. An empty poll
        does not imply source exhaustion. Native I/O and failure monitoring
        continue while the caller performs application work.

        :param timeout: Finite nonnegative maximum wait in seconds; zero never waits.
        :returns: One normalized delivery, or None when no delivery is currently available.
        :raises TransportError: If this reader has detected a terminal failure.
        :raises ConfigurationError: If the relative timeout is invalid.
        """
        ...

    def failure(self) -> TransportError | None:
        """Inspect terminal failure without waiting for delivery capacity.

        :returns: The first terminal transport error, or None while healthy or intentionally closed.
        """
        ...

    def close(self, *, deadline: float) -> None:
        """Request idempotent cleanup and confirm owned worker termination.

        :param deadline: Absolute cleanup deadline in the supplied runtime clock domain.
        :returns: None after the owned native resources have been released.
        :raises ObserverCleanupError: If native cleanup fails.
        :raises ObserverCleanupTimeoutError: If owned resources outlive the cleanup deadline.
        """
        ...


class SyncTransportAdapter(Protocol):
    """Lazy factory of blocking sessions with no hidden asynchronous runtime.

    :ivar capabilities: Supported independent read, replay, flow-control, and value semantics.
    """

    @property
    def capabilities(self) -> TransportCapabilities:
        """Describe supported transport semantics before opening resources.

        :returns: Immutable static capabilities; readiness may refine them.
        """
        ...

    def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> SyncTransportSession:
        """Open an independent reader and synchronously confirm READY.

        :param source: Concrete existing source selected for this observation.
        :param start: Explicit requested start policy and transport-specific cursor.
        :param context: Owner clock, startup deadline, observation identity, and handoff limits.
        :returns: A fully READY blocking session whose native client has one I/O owner.
        :raises TransportError: If source resolution, connection, or readiness fails.
        :raises TimeoutError: If the startup deadline expires.
        """
        ...


class SyncAdapterProvider(Protocol):
    """Offer an explicit lazy blocking adapter alongside another execution model."""

    def sync_adapter(self) -> SyncTransportAdapter:
        """Construct a blocking adapter without opening native resources.

        :returns: An adapter whose sessions are owned by the blocking runtime thread.
        """
        ...


class SyncSettlementSession(Protocol):
    """Optional blocking acknowledgement extension for isolated deliveries."""

    def settle(self, delivery_id: str, outcome: DeliveryOutcome) -> None:
        """Settle one delivery after Core commits its safe admission outcome.

        :param delivery_id: Opaque session-local token accompanying the original RawEvent.
        :param outcome: Committed admission, explicit filtering, or explicit Journal discard.
        :returns: None after native acknowledgement is accepted by the owning driver.
        :raises TransportError: If the token is invalid or native settlement fails.
        """
        ...


__all__ = ["SyncTransportAdapter", "SyncTransportSession", "SyncSettlementSession", "SyncAdapterProvider"]
