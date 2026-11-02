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
    """Copy a mapping and expose a read-only view of its top-level contents.

    :param value: Mapping whose current key/value associations are captured.
    :returns: A mapping proxy over a new dictionary; nested values are not recursively copied.
    """
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class RawEvent:
    """Represent one transport delivery without decoding or collapsing header pairs.

    :ivar payload: Raw bytes, or None for a transport-supported null payload; empty bytes remain distinct.
    :ivar source: Nonempty topic, exchange, stream, or other transport source name.
    :ivar headers: Ordered name/value pairs preserving duplicates and null values where supported.
    :ivar key: Optional native message key as bytes.
    :ivar metadata: Copied read-only mapping of transport-specific scalar metadata.
    :ivar delivery_id: Optional opaque session-local settlement token, not a business identifier.
    """

    payload: bytes | None
    source: str
    headers: tuple[tuple[str, bytes | None], ...] = ()
    key: bytes | None = None
    metadata: Mapping[str, MetadataScalar] = field(default_factory=dict)
    delivery_id: str | None = None

    def __post_init__(self) -> None:
        """Validate raw field types and freeze headers and transport metadata.

        :raises ConfigurationError: Payload, source, key, header pairs, or metadata violate the raw
            envelope contract.
        """
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
    """Pair a decoded payload with its authoritative Journal admission coordinates.

    :ivar observer_id: Identity of the watcher that admitted the event.
    :ivar sequence: Watcher-local Journal sequence, independent of broker positions.
    :ivar payload: Decoded payload owned by the runtime or detached for the caller.
    :ivar raw: Immutable original transport envelope.
    :ivar recorded_at: Wall-clock admission timestamp used for diagnostics.
    :ivar recorded_monotonic: Admission time in the watcher clock domain, used for deadline eligibility.
    :ivar message_type: Optional decoded schema or message-type name.
    :ivar trace_context: Passive extracted trace metadata; it is not installed as an active span.
    """

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
    """Capture a watcher-local Journal boundary; timestamps are informational only.

    :ivar observer_id: Identity of the owning watcher.
    :ivar sequence: Next Journal admission position captured by mark().
    :ivar recorded_at: Timezone-aware capture time normalized to UTC.
    :ivar recorded_monotonic: Finite capture time in the watcher monotonic clock domain.
    """

    observer_id: str
    sequence: int
    recorded_at: datetime
    recorded_monotonic: float

    def __post_init__(self) -> None:
        """Validate mark coordinates and normalize its representable wall-clock time to UTC.

        :raises ConfigurationError: Identity, sequence, timezone awareness, or time representability is
            invalid.
        """
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
    """Identify a transport-supported strategy for the initial reader position.

    :ivar LATEST: Resolve a new-events boundary once during startup.
    :ivar EARLIEST: Replay history still available at startup.
    :ivar OFFSET: Use explicit Kafka-style partition offsets.
    :ivar TIMESTAMP: Resolve initial transport positions from an epoch timestamp.
    :ivar STREAM_ID: Read strictly after a complete native Redis Streams ID.
    """

    LATEST = "latest"
    EARLIEST = "earliest"
    OFFSET = "offset"
    TIMESTAMP = "timestamp"
    STREAM_ID = "stream_id"


@dataclass(frozen=True, slots=True)
class EpochMilliseconds:
    """Disambiguate integer epoch milliseconds from other timestamp units.

    :ivar value: Nonnegative count of milliseconds since the Unix epoch.
    """

    value: int

    def __post_init__(self) -> None:
        """Reject booleans, negative values, and noninteger epoch milliseconds.

        :raises ConfigurationError: The epoch millisecond value is invalid.
        """
        if isinstance(self.value, bool) or not isinstance(self.value, int) or self.value < 0:
            raise ConfigurationError("Epoch milliseconds must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class StartPosition:
    """Describe one explicit initial transport reading policy.

    :ivar mode: Normalized start-mode discriminator.
    :ivar offsets: Copied partition-to-offset mapping used only by OFFSET.
    :ivar timestamp_ms: Epoch milliseconds used only by TIMESTAMP.
    :ivar require_existing: Require a timestamp lookup to resolve existing history instead of a permitted
        fallback.
    :ivar stream_id: Canonical unsigned ms-sequence ID used only by STREAM_ID.
    """

    mode: StartMode = StartMode.LATEST
    offsets: Mapping[int, int] = field(default_factory=dict)
    timestamp_ms: int | None = None
    require_existing: bool = False
    stream_id: str | None = None

    def __post_init__(self) -> None:
        """Normalize the mode, freeze offsets, and reject conflicting or malformed position fields.

        :raises ConfigurationError: The mode or any mode-specific position is invalid or conflicts with
            another mode.
        """
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
        """Request a startup boundary for new events rather than retained broker history.

        :returns: A LATEST start position.
        """
        return cls(StartMode.LATEST)

    @classmethod
    def earliest(cls) -> "StartPosition":
        """Request the earliest history still available to the transport.

        :returns: An EARLIEST start position; deleted history is not restored.
        """
        return cls(StartMode.EARLIEST)

    @classmethod
    def after_stream_id(cls, stream_id: str) -> "StartPosition":
        """Read strictly after a native Redis Streams ID, including its sequence component.

        :param stream_id: Canonical ms-sequence pair of unsigned 64-bit integers.
        :returns: An exclusive STREAM_ID start position.
        :raises ConfigurationError: The ID is not a canonical unsigned 64-bit pair.
        """
        return cls(StartMode.STREAM_ID, stream_id=stream_id)

    @classmethod
    def at_offset(cls, offsets: Mapping[int, int]) -> "StartPosition":
        """Request exact starting offsets for explicitly named partitions.

        :param offsets: Nonempty mapping of nonnegative partition numbers to nonnegative offsets.
        :returns: An OFFSET start position containing a copied mapping.
        :raises ConfigurationError: The partition/offset mapping is empty or invalid.
        """
        return cls(StartMode.OFFSET, offsets=offsets)

    @classmethod
    def at_timestamp(
        cls, timestamp: datetime | EpochMilliseconds, *, require_existing: bool = False
    ) -> "StartPosition":
        """Resolve a transport starting position from an unambiguous epoch timestamp.

        :param timestamp: Timezone-aware datetime or an EpochMilliseconds wrapper.
        :param require_existing: Require an existing position for every timestamp lookup.
        :returns: A TIMESTAMP start position using integer epoch milliseconds.
        :raises ConfigurationError: The timestamp is naive, uses an unsupported type, or precedes the
            epoch.
        """
        if isinstance(timestamp, datetime) and timestamp.utcoffset() is not None:
            milliseconds = int(timestamp.timestamp() * 1000)
        elif isinstance(timestamp, EpochMilliseconds):
            milliseconds = timestamp.value
        else:
            raise ConfigurationError("Timestamp requires aware datetime or EpochMilliseconds")
        return cls(StartMode.TIMESTAMP, timestamp_ms=milliseconds, require_existing=require_existing)


class ScopeStartMode(str, Enum):
    """Choose the logical lower boundary of a new scoped view.

    :ivar ALL_RETAINED: Inherit the parent logical range and its known completeness.
    :ivar FROM_NOW: Capture the runtime next-admission position when creating the scope.
    :ivar FROM_SEQUENCE: Use an explicit watcher-local Journal lower bound.
    """

    ALL_RETAINED = "all_retained"
    FROM_NOW = "from_now"
    FROM_SEQUENCE = "from_sequence"


@dataclass(frozen=True, slots=True)
class ScopeStart:
    """Describe how a scope narrows its parent Journal range.

    :ivar mode: Normalized scope boundary strategy.
    :ivar sequence: Nonnegative explicit lower bound used only by FROM_SEQUENCE.
    """

    mode: ScopeStartMode = ScopeStartMode.ALL_RETAINED
    sequence: int | None = None

    def __post_init__(self) -> None:
        """Normalize the boundary mode and validate its optional sequence.

        :raises ConfigurationError: The mode is unsupported or the sequence conflicts with the selected
            mode.
        """
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
        """Inherit the parent logical history without hiding known retention gaps.

        :returns: An ALL_RETAINED scope-start definition.
        """
        return cls()

    @classmethod
    def from_now(cls) -> "ScopeStart":
        """Capture a fresh admission boundary when the owner runtime creates the scope.

        :returns: A FROM_NOW scope-start definition.
        """
        return cls(ScopeStartMode.FROM_NOW)

    @classmethod
    def from_sequence(cls, sequence: int) -> "ScopeStart":
        """Select an explicit inclusive Journal lower bound within the same watcher.

        :param sequence: Nonnegative watcher-local sequence; availability is checked when used.
        :returns: A FROM_SEQUENCE scope-start definition.
        :raises ConfigurationError: The sequence is a boolean, negative, or noninteger.
        """
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise ConfigurationError("Sequence must be a nonnegative integer")
        return cls(ScopeStartMode.FROM_SEQUENCE, sequence)


@dataclass(frozen=True, slots=True)
class TransportSource:
    """Name a transport source and optional explicitly selected partitions.

    :ivar name: Nonempty transport-specific source name.
    :ivar partitions: Optional nonempty tuple of unique nonnegative partition numbers.
    """

    name: str
    partitions: tuple[int, ...] | None = None

    def __post_init__(self) -> None:
        """Validate the source name and freeze its optional partition selection.

        :raises ConfigurationError: The name is empty or the partition selection is invalid.
        """
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
    """Declare supported semantics without promising unsupported broker guarantees.

    :ivar start_modes: Supported StartMode string values.
    :ivar isolation_modes: Available independent-reader or dedicated-subscription strategies.
    :ivar flow_control_modes: Implemented bounded handoff or native flow-control mechanisms.
    :ivar ordering_scope: Domain within which native event order is comparable.
    :ivar supports_headers: Whether native headers can be preserved.
    :ivar supports_key: Whether native message keys are supported.
    :ivar supports_source_timestamps: Whether native source timestamps are exposed.
    :ivar supports_position_tracking: Whether native position tracking is implemented.
    :ivar supports_topology_detection: Whether the selected source topology is monitored.
    :ivar supports_settlement: Whether admission outcomes require explicit delivery settlement.
    :ivar supports_null_payload: Whether a null payload can be distinguished from empty bytes.
    :ivar supports_duplicate_headers: Whether repeated header names can be preserved.
    :ivar supports_null_header_values: Whether null header values are representable.
    """

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
        """Copy capability collections into immutable frozensets."""
        for name in ("start_modes", "isolation_modes", "flow_control_modes"):
            object.__setattr__(self, name, frozenset(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class ObservationBoundary:
    """Describe the source and resolved startup cut without imposing a global broker order.

    :ivar source: Observed source name.
    :ivar start_mode: Selected start strategy as a stable string.
    :ivar positions: Copied mapping of native integer partition positions when applicable.
    :ivar captured_monotonic: Boundary capture time in the watcher clock domain.
    """

    source: str
    start_mode: str
    positions: Mapping[int, int] = field(default_factory=dict)
    captured_monotonic: float = 0.0

    def __post_init__(self) -> None:
        """Freeze the resolved position mapping without changing its native coordinates."""
        object.__setattr__(self, "positions", frozen_mapping(self.positions))


@dataclass(frozen=True, slots=True)
class TransportReadiness:
    """Confirm the prepared reader and the guarantees resolved during startup.

    :ivar boundary: Resolved observation starting boundary.
    :ivar resolved_capabilities: Capabilities actually available for this session.
    :ivar source_metadata: Copied read-only mapping of safe scalar source preparation metadata.
    :ivar ready_monotonic: Time readiness was confirmed in the watcher clock domain.
    """

    boundary: ObservationBoundary
    resolved_capabilities: TransportCapabilities
    source_metadata: Mapping[str, MetadataScalar] = field(default_factory=dict)
    ready_monotonic: float = 0.0

    def __post_init__(self) -> None:
        """Freeze source metadata so later adapter changes cannot mutate readiness."""
        object.__setattr__(self, "source_metadata", frozen_mapping(self.source_metadata))


@dataclass(frozen=True, slots=True)
class OpenContext:
    """Provide adapter startup dependencies and limits without global runtime state.

    :ivar observer_id: Identity of the watch being opened.
    :ivar startup_deadline: Absolute startup deadline in the supplied clock domain.
    :ivar deadline: Read-only convenience alias for startup_deadline.
    :ivar buffers: Bounded transport handoff limits.
    :ivar identity: Optional run, worker, and test identities supplied by the caller.
    :ivar diagnostics: Optional collector for sanitized adapter diagnostics.
    :ivar clock: Clock shared with the owner runtime for deadline calculations.
    """

    observer_id: str
    startup_deadline: float
    buffers: TransportBufferLimits
    identity: ObservationIdentity = field(default_factory=ObservationIdentity)
    diagnostics: Any = None
    clock: Clock = field(default_factory=SystemClock, repr=False)

    @property
    def deadline(self) -> float:
        """Expose the startup deadline under the adapter convenience name.

        :returns: The same absolute value as startup_deadline.
        """
        return self.startup_deadline
