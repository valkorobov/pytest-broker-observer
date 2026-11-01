"""Immutable resource policies; sizes are accounted budgets, not process RSS limits."""

from dataclasses import dataclass, field, fields
from enum import Enum
import math

from .errors import ConfigurationError

MiB = 1024 * 1024


def positive(value: object, name: str, *, integer: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{name} must be positive and finite")
    if integer and not isinstance(value, int):
        raise ConfigurationError(f"{name} must be an integer")
    if value <= 0 or not math.isfinite(value):
        raise ConfigurationError(f"{name} must be positive and finite")


class OverflowPolicy(str, Enum):
    FAIL = "fail"
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


class DecodeErrorPolicy(str, Enum):
    FAIL_FAST = "fail_fast"
    SKIP_UNKNOWN_TYPE = "skip_unknown_type"


@dataclass(frozen=True, slots=True)
class JournalLimits:
    max_events: int = 10_000
    max_bytes: int = 64 * MiB
    max_event_bytes: int = 16 * MiB
    max_loss_ranges: int = 1024

    def __post_init__(self) -> None:
        for f in fields(self):
            positive(getattr(self, f.name), f.name, integer=True)
        if self.max_event_bytes > self.max_bytes:
            raise ConfigurationError("max_event_bytes cannot exceed max_bytes")


@dataclass(frozen=True, slots=True)
class JournalPolicy:
    limits: JournalLimits = field(default_factory=JournalLimits)
    overflow: OverflowPolicy = OverflowPolicy.FAIL

    def __post_init__(self) -> None:
        if not isinstance(self.limits, JournalLimits):
            raise ConfigurationError("limits must be JournalLimits")
        try:
            object.__setattr__(self, "overflow", OverflowPolicy(self.overflow))
        except ValueError as exc:
            raise ConfigurationError("Invalid overflow policy") from exc


@dataclass(frozen=True, slots=True)
class RuntimeLimits:
    max_active_waiters: int = 128
    max_scopes: int = 256
    max_result_events: int = 10_000
    max_result_bytes: int = 64 * MiB
    scan_batch_events: int = 64

    def __post_init__(self) -> None:
        for f in fields(self):
            positive(getattr(self, f.name), f.name, integer=True)


@dataclass(frozen=True, slots=True)
class TransportBufferLimits:
    max_events: int = 256
    max_bytes: int = 16 * MiB

    def __post_init__(self) -> None:
        for f in fields(self):
            positive(getattr(self, f.name), f.name, integer=True)


@dataclass(frozen=True, slots=True)
class LifecyclePolicy:
    startup_timeout: float = 15.0
    shutdown_timeout: float = 5.0
    sync_control_timeout: float = 5.0
    cancellation_grace: float = 1.0

    def __post_init__(self) -> None:
        for f in fields(self):
            positive(getattr(self, f.name), f.name)


@dataclass(frozen=True, slots=True)
class DecodingPolicy:
    error_policy: DecodeErrorPolicy = DecodeErrorPolicy.FAIL_FAST

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "error_policy", DecodeErrorPolicy(self.error_policy))
        except ValueError as exc:
            raise ConfigurationError("Invalid decoding policy") from exc


@dataclass(frozen=True, slots=True)
class ObservationIdentity:
    run_id: str | None = None
    worker_id: str | None = None
    test_id: str | None = None
