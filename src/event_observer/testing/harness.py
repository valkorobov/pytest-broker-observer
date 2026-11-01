"""Harness interfaces for testing third-party transport adapters.

Publishing belongs to test infrastructure, not to the observer runtime. A
receipt confirms acceptance by the broker API, rather than local enqueueing.
Fault, position and topology controls are optional, separate extensions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol

from ..errors import TransportError
from ..models import MetadataScalar, TransportSource, frozen_mapping
from ..transports.base import TransportAdapter


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    accepted: bool = True
    metadata: Mapping[str, MetadataScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", frozen_mapping(self.metadata))


class AdapterTestHarness(Protocol):
    @property
    def adapter(self) -> TransportAdapter: ...

    async def create_source(self, *, case_id: str) -> TransportSource: ...

    async def publish(
        self,
        source: TransportSource,
        payload: bytes | None,
        *,
        key: bytes | None = None,
        headers: tuple[tuple[str, bytes | None], ...] = (),
    ) -> PublishReceipt: ...

    async def cleanup(self) -> None: ...


class FailureHarness(Protocol):
    """An adapter harness that can trigger a known continuity failure."""

    async def inject_failure(self, source: TransportSource, error: TransportError) -> None: ...


class TopologyHarness(Protocol):
    """An adapter harness that can change source topology deliberately."""

    async def add_partition(self, source: TransportSource) -> None: ...


class PositionHarness(Protocol):
    """An adapter harness that can remove records below a native position."""

    async def expire_before(self, source: TransportSource, position: int) -> None: ...


__all__ = ["AdapterTestHarness", "PublishReceipt", "FailureHarness", "TopologyHarness", "PositionHarness"]
