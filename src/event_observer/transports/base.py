"""Public transport interfaces and the optional delivery-settlement extension.

Adapters own native resources and create loop-bound state inside ``open``.
Core consumes these structural protocols without importing a broker client.

:ivar __all__: Explicit public extension interfaces and shared transport models.
"""

from typing import AsyncIterator, Protocol
from enum import Enum

from ..errors import TransportError
from ..models import (
    ObservationBoundary,
    OpenContext,
    RawEvent,
    StartPosition,
    TransportCapabilities,
    TransportReadiness,
    TransportSource,
)


class TransportSession(Protocol):
    """One independent, runtime-owned reader with a separate failure signal.

    :ivar readiness: Resolved boundary and capabilities, available only after successful startup.
    """

    @property
    def readiness(self) -> TransportReadiness:
        """Return the resolved startup contract for this reader.

        :returns: Boundary, resolved capabilities, source metadata, and READY time.
        :raises TransportReadinessError: If startup has not completed successfully.
        """
        ...

    def receive(self) -> AsyncIterator[RawEvent]:
        """Create the session's single receive iterator.

        :returns: An asynchronous iterator of detached normalized deliveries.
        :raises TransportSubscriptionError: If another receive iterator has already been requested.
        """
        ...

    async def wait_failure(self) -> TransportError | None:
        """Wait for a terminal failure independently of delivery backpressure.

        :returns: The terminal transport error, or None after intentional closure.
        :raises asyncio.CancelledError: If this waiter is cancelled; the shared failure signal remains
            intact.
        """
        ...

    async def close(self, *, deadline: float) -> None:
        """Join idempotent cleanup of this session's owned resources.

        :param deadline: Absolute cleanup deadline in the runtime monotonic clock domain.
        :returns: None after cleanup finishes.
        :raises ObserverCleanupError: If resources cannot be released cleanly.
        :raises ObserverCleanupTimeoutError: If the cleanup deadline expires.
        """
        ...


class TransportAdapter(Protocol):
    """Factory of independent sessions; construction must not open connections.

    :ivar capabilities: Static supported start, isolation, flow-control, and data semantics.
    """

    @property
    def capabilities(self) -> TransportCapabilities:
        """Describe the semantics this adapter can provide.

        :returns: Immutable static transport capabilities; session readiness may refine them.
        """
        ...

    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> TransportSession:
        """Resolve the requested source and return an independently READY reader.

        :param source: Concrete existing source selected for this observation.
        :param start: Requested start policy and its transport-specific boundary.
        :param context: Runtime clock, startup deadline, observation identity, and buffer limits.
        :returns: A session whose receive path is ready before the application action.
        :raises TransportError: If connection, authorization, source resolution, or readiness fails.
        :raises UnsupportedCapabilityError: If the requested semantics are unsupported.
        """
        ...


class DeliveryOutcome(str, Enum):
    """Outcomes Core may commit before settling an isolated delivery.

    :ivar ADMITTED: The event was successfully admitted to the Journal.
    :ivar FILTERED: The decoding policy explicitly excluded an unknown message type.
    :ivar DISCARDED_BY_EXPLICIT_JOURNAL_POLICY: A DROP policy recorded an explicit loss instead of retaining
        the event.
    """

    ADMITTED = "admitted"
    FILTERED = "filtered"
    DISCARDED_BY_EXPLICIT_JOURNAL_POLICY = "discarded_by_explicit_journal_policy"


class SettlementSession(Protocol):
    """Optional acknowledgement interface, independent of the base reader protocol."""

    async def settle(self, delivery_id: str, outcome: DeliveryOutcome) -> None:
        """Acknowledge one delivery only after Core commits its safe outcome.

        :param delivery_id: Opaque session-local token returned with the original delivery.
        :param outcome: Committed admission, explicit filtering, or explicit Journal discard outcome.
        :returns: None after that delivery has been settled.
        :raises TransportError: If the token is invalid or native acknowledgement fails.
        """
        ...


__all__ = [
    "TransportAdapter",
    "TransportSession",
    "SettlementSession",
    "DeliveryOutcome",
    "RawEvent",
    "TransportSource",
    "StartPosition",
    "OpenContext",
    "TransportCapabilities",
    "TransportReadiness",
    "ObservationBoundary",
]
