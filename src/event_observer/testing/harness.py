"""Structural test-harness interfaces with independent publication and ownership.

Receipts confirm broker acceptance instead of local enqueueing. Fault,
position, and topology controls are separate optional protocols; implementing
an adapter never requires production message publication.

:ivar __all__: Public harness protocols and the immutable publication receipt.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol

from ..errors import TransportError
from ..models import MetadataScalar, TransportSource, frozen_mapping
from ..transports.base import TransportAdapter


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    """Detached result of a broker-confirmed test publication.

    :ivar accepted: Whether the broker API confirmed acceptance, not durability or exactly-once delivery.
    :ivar metadata: Detached immutable native receipt values such as offsets or stream IDs.
    """

    accepted: bool = True
    metadata: Mapping[str, MetadataScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Detach publication metadata from the publisher's mutable mapping.

        :returns: None after metadata is copied into an immutable representation.
        """
        object.__setattr__(self, "metadata", frozen_mapping(self.metadata))


class AdapterTestHarness(Protocol):
    """Independent source ownership and publisher contract used by reusable adapter tests.

    :ivar adapter: Adapter under test, with capabilities determining optional test applicability.
    """

    @property
    def adapter(self) -> TransportAdapter:
        """Return the independently configured adapter under test.

        :returns: An adapter exposing only its implemented transport semantics.
        """
        ...

    async def create_source(self, *, case_id: str) -> TransportSource:
        """Create a unique source registered for cleanup by this fixture.

        :param case_id: Nonempty test label used in a unique owned source name.
        :returns: The created source, ready to be opened by the observer.
        :raises ConfigurationError: If case_id is invalid or the harness is already closed.
        :raises TransportError: If source creation cannot be confirmed.
        """
        ...

    async def publish(
        self,
        source: TransportSource,
        payload: bytes | None,
        *,
        key: bytes | None = None,
        headers: tuple[tuple[str, bytes | None], ...] = (),
    ) -> PublishReceipt:
        """Publish through a code path independent of the observing session.

        :param source: Concrete existing source selected for this observation.
        :param payload: Raw message body; null is accepted only when the adapter advertises it.
        :param key: Optional message key, rejected by transports that do not support one.
        :param headers: Ordered normalized header pairs, subject to the transport capabilities.
        :returns: A receipt confirming broker acceptance with detached native metadata.
        :raises UnsupportedCapabilityError: If the transport cannot represent a supplied body, key, or
            header.
        :raises TransportError: If publication is not confirmed.
        """
        ...

    async def cleanup(self) -> None:
        """Remove only fixture-owned sources, including after partial startup failure.

        :returns: None after owned resources have been released.
        :raises TransportError: If cleanup cannot remove all owned resources.
        """
        ...


class FailureHarness(Protocol):
    """Optional fault control for a harness that can break known continuity."""

    async def inject_failure(self, source: TransportSource, error: TransportError) -> None:
        """Trigger a known terminal transport failure for the selected test source.

        :param source: Concrete existing source selected for this observation.
        :param error: Failure to normalize or publish without copying its message into safe diagnostics.
        :returns: None after the failure mechanism is triggered.
        :raises TransportError: If the controlled fault cannot be applied.
        """
        ...


class TopologyHarness(Protocol):
    """Optional source-topology control, separate from mandatory transport contracts."""

    async def add_partition(self, source: TransportSource) -> None:
        """Add a partition to the selected source for topology-change checks.

        :param source: Concrete existing source selected for this observation.
        :returns: None after the new partition is confirmed.
        :raises TransportError: If the topology change cannot be completed.
        """
        ...


class PositionHarness(Protocol):
    """Optional history-retention control for position-capable test brokers."""

    async def expire_before(self, source: TransportSource, position: int) -> None:
        """Expire fixture-owned history below a native position.

        :param source: Concrete existing source selected for this observation.
        :param position: Native lower position below which the fixture should expire history.
        :returns: None after the controlled expiration completes.
        :raises TransportError: If the retention change cannot be applied.
        """
        ...


__all__ = ["AdapterTestHarness", "PublishReceipt", "FailureHarness", "TopologyHarness", "PositionHarness"]
