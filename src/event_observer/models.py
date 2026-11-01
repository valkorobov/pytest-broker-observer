"""Transport neutral immutable envelopes and observation boundaries."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
import re
from types import MappingProxyType
from typing import Any, Generic, Mapping, TypeVar

from .config import ObservationIdentity, TransportBufferLimits
from ._runtime.clock import Clock, SystemClock
from .errors import ConfigurationError

T = TypeVar("T")
MetadataScalar = str | int | float | bool | bytes | None


def frozen_mapping(value: Mapping) -> Mapping:
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class RawEvent:
    payload: bytes | None
    source: str
    headers: tuple[tuple[str, bytes | None], ...] = ()
    key: bytes | None = None
    metadata: Mapping[str, MetadataScalar] = field(default_factory=dict)
    delivery_id: str | None = None

    def __post_init__(self) -> None:
        if self.payload is not None and not isinstance(self.payload, bytes):
            raise ConfigurationError("Raw payload must be bytes or None")
        if not isinstance(self.source, str) or not self.source:
            raise ConfigurationError("Raw source must be a nonempty string")
        if self.key is not None and not isinstance(self.key, bytes):
            raise ConfigurationError("Raw key must be bytes or None")
        headers = tuple(tuple(pair) for pair in self.headers)
        if any(
            len(p) != 2 or not isinstance(p[0], str) or (p[1] is not None and not isinstance(p[1], bytes))
            for p in headers
        ):
            raise ConfigurationError("Headers must preserve string/bytes-or-null pairs")
        if any(
            not isinstance(k, str) or not isinstance(v, (str, int, float, bool, bytes, type(None)))
            for k, v in self.metadata.items()
        ):
            raise ConfigurationError("Metadata must contain scalar values")
        object.__setattr__(self, "headers", headers)
        object.__setattr__(self, "metadata", frozen_mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class ObservedEvent(Generic[T]):
    observer_id: str
    sequence: int
    payload: T
    raw: RawEvent
    recorded_at: datetime
    recorded_monotonic: float
    message_type: str | None = None
    trace_context: Any = None


@dataclass(frozen=True, slots=True)
class ObservationMark:
    """A watcher-local Journal boundary; timestamps are informational only."""

    observer_id: str
    sequence: int
    recorded_at: datetime
    recorded_monotonic: float

    def __post_init__(self) -> None:
        if not isinstance(self.observer_id, str) or not self.observer_id:
            raise ConfigurationError("Observation mark requires a watcher identity")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
            raise ConfigurationError("Observation mark sequence must be a nonnegative integer")
        if not isinstance(self.recorded_at, datetime) or self.recorded_at.utcoffset() is None:
            raise ConfigurationError("Observation mark timestamp must be timezone-aware")
        if isinstance(self.recorded_monotonic, bool) or not isinstance(self.recorded_monotonic, (int, float)):
            raise ConfigurationError("Observation mark monotonic time must be finite")
        try:
            if not math.isfinite(self.recorded_monotonic):
                raise ConfigurationError("Observation mark monotonic time must be finite")
            normalized_at = self.recorded_at.astimezone(timezone.utc)
        except (OverflowError, ValueError) as exc:
            raise ConfigurationError("Observation mark times must be representable") from exc
        object.__setattr__(self, "recorded_at", normalized_at)


class StartMode(str, Enum):
    LATEST = "latest"
    EARLIEST = "earliest"
    OFFSET = "offset"
    TIMESTAMP = "timestamp"
    STREAM_ID = "stream_id"


@dataclass(frozen=True, slots=True)
class EpochMilliseconds:
    value: int

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or not isinstance(self.value, int) or self.value < 0:
            raise ConfigurationError("Epoch milliseconds must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class StartPosition:
    mode: StartMode = StartMode.LATEST
    offsets: Mapping[int, int] = field(default_factory=dict)
    timestamp_ms: int | None = None
    require_existing: bool = False
    stream_id: str | None = None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "mode", StartMode(self.mode))
        except ValueError as exc:
            raise ConfigurationError("Invalid start mode") from exc
        if any(
            isinstance(k, bool)
            or not isinstance(k, int)
            or k < 0
            or isinstance(v, bool)
            or not isinstance(v, int)
            or v < 0
            for k, v in self.offsets.items()
        ):
            raise ConfigurationError("Offsets must map nonnegative partitions to offsets")
        object.__setattr__(self, "offsets", frozen_mapping(self.offsets))
        if self.mode == StartMode.OFFSET and not self.offsets:
            raise ConfigurationError("Explicit offsets cannot be empty")
        if self.mode != StartMode.OFFSET and self.offsets:
            raise ConfigurationError("Offsets require offset start mode")
        if self.mode == StartMode.TIMESTAMP:
            if self.timestamp_ms is None:
                raise ConfigurationError("Timestamp start requires epoch milliseconds")
            EpochMilliseconds(self.timestamp_ms)
        elif self.timestamp_ms is not None:
            raise ConfigurationError("Timestamp requires timestamp start mode")
        if self.mode == StartMode.STREAM_ID:
            if (
                not isinstance(self.stream_id, str)
                or re.fullmatch(r"(?:0|[1-9][0-9]{0,19})-(?:0|[1-9][0-9]{0,19})", self.stream_id) is None
                or any(int(part) > 2**64 - 1 for part in self.stream_id.split("-"))
            ):
                raise ConfigurationError("Stream ID must contain two unsigned 64-bit integers")
        elif self.stream_id is not None:
            raise ConfigurationError("Stream ID requires stream_id start mode")

    @classmethod
    def latest(cls) -> "StartPosition":
        return cls(StartMode.LATEST)

    @classmethod
    def earliest(cls) -> "StartPosition":
        return cls(StartMode.EARLIEST)

    @classmethod
    def after_stream_id(cls, stream_id: str) -> "StartPosition":
        """Read strictly after a native Redis Streams ID, including its sequence."""
        return cls(StartMode.STREAM_ID, stream_id=stream_id)

    @classmethod
    def at_offset(cls, offsets: Mapping[int, int]) -> "StartPosition":
        return cls(StartMode.OFFSET, offsets=offsets)

    @classmethod
    def at_timestamp(
        cls, timestamp: datetime | EpochMilliseconds, *, require_existing: bool = False
    ) -> "StartPosition":
        if isinstance(timestamp, datetime) and timestamp.utcoffset() is not None:
            milliseconds = int(timestamp.timestamp() * 1000)
        elif isinstance(timestamp, EpochMilliseconds):
            milliseconds = timestamp.value
        else:
            raise ConfigurationError("Timestamp requires aware datetime or EpochMilliseconds")
        return cls(StartMode.TIMESTAMP, timestamp_ms=milliseconds, require_existing=require_existing)


class ScopeStartMode(str, Enum):
    ALL_RETAINED = "all_retained"
    FROM_NOW = "from_now"
    FROM_SEQUENCE = "from_sequence"


@dataclass(frozen=True, slots=True)
class ScopeStart:
    mode: ScopeStartMode = ScopeStartMode.ALL_RETAINED
    sequence: int | None = None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "mode", ScopeStartMode(self.mode))
        except ValueError as exc:
            raise ConfigurationError("Invalid scope start mode") from exc
        if self.mode == ScopeStartMode.FROM_SEQUENCE:
            if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
                raise ConfigurationError("Sequence must be a nonnegative integer")
        elif self.sequence is not None:
            raise ConfigurationError("Sequence requires from_sequence mode")

    @classmethod
    def all_retained(cls) -> "ScopeStart":
        return cls()

    @classmethod
    def from_now(cls) -> "ScopeStart":
        return cls(ScopeStartMode.FROM_NOW)

    @classmethod
    def from_sequence(cls, sequence: int) -> "ScopeStart":
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise ConfigurationError("Sequence must be a nonnegative integer")
        return cls(ScopeStartMode.FROM_SEQUENCE, sequence)


@dataclass(frozen=True, slots=True)
class TransportSource:
    name: str
    partitions: tuple[int, ...] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ConfigurationError("Source must be nonempty")
        if self.partitions is not None:
            values = tuple(self.partitions)
            if (
                not values
                or len(set(values)) != len(values)
                or any(isinstance(p, bool) or not isinstance(p, int) or p < 0 for p in values)
            ):
                raise ConfigurationError("Partitions must be unique nonnegative integers")
            object.__setattr__(self, "partitions", values)


@dataclass(frozen=True, slots=True)
class TransportCapabilities:
    start_modes: frozenset[str] = frozenset({"latest", "earliest"})
    isolation_modes: frozenset[str] = frozenset({"independent_reader"})
    flow_control_modes: frozenset[str] = frozenset({"fetch_limit"})
    ordering_scope: str = "source"
    supports_headers: bool = True
    supports_key: bool = True
    supports_source_timestamps: bool = False
    supports_position_tracking: bool = False
    supports_topology_detection: bool = False
    supports_settlement: bool = False
    supports_null_payload: bool = True
    supports_duplicate_headers: bool = True
    supports_null_header_values: bool = True

    def __post_init__(self) -> None:
        for name in ("start_modes", "isolation_modes", "flow_control_modes"):
            object.__setattr__(self, name, frozenset(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class ObservationBoundary:
    source: str
    start_mode: str
    positions: Mapping[int, int] = field(default_factory=dict)
    captured_monotonic: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "positions", frozen_mapping(self.positions))


@dataclass(frozen=True, slots=True)
class TransportReadiness:
    boundary: ObservationBoundary
    resolved_capabilities: TransportCapabilities
    source_metadata: Mapping[str, MetadataScalar] = field(default_factory=dict)
    ready_monotonic: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_metadata", frozen_mapping(self.source_metadata))


@dataclass(frozen=True, slots=True)
class OpenContext:
    observer_id: str
    startup_deadline: float
    buffers: TransportBufferLimits
    identity: ObservationIdentity = field(default_factory=ObservationIdentity)
    diagnostics: Any = None
    clock: Clock = field(default_factory=SystemClock, repr=False)

    @property
    def deadline(self) -> float:
        return self.startup_deadline
