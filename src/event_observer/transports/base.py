"""Public extension protocol. Construct loop-bound resources inside open()."""

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
    @property
    def readiness(self) -> TransportReadiness: ...
    def receive(self) -> AsyncIterator[RawEvent]: ...
    async def wait_failure(self) -> TransportError | None: ...
    async def close(self, *, deadline: float) -> None: ...


class TransportAdapter(Protocol):
    @property
    def capabilities(self) -> TransportCapabilities: ...
    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> TransportSession: ...


class DeliveryOutcome(str, Enum):
    ADMITTED = "admitted"
    FILTERED = "filtered"
    DISCARDED_BY_EXPLICIT_JOURNAL_POLICY = "discarded_by_explicit_journal_policy"


class SettlementSession(Protocol):
    async def settle(self, delivery_id: str, outcome: DeliveryOutcome) -> None: ...


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
