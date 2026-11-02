"""Immutable resource policies; sizes are accounted budgets, not process RSS limits."""

from dataclasses import dataclass, field, fields
from enum import Enum
import math

from .errors import ConfigurationError

MiB = 1024 * 1024


def positive(value: object, name: str, *, integer: bool = False) -> None:
    """Validate a positive finite policy value, rejecting booleans.

    :param value: Numeric setting to validate.
    :param name: Setting name used in a configuration error.
    :param integer: Require an integer rather than any real-valued setting.
    :raises ConfigurationError: The value is nonnumeric, nonpositive, nonfinite, or has the wrong integer
        type.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{name} must be positive and finite")
    if integer and not isinstance(value, int):
        raise ConfigurationError(f"{name} must be an integer")
    if value <= 0 or not math.isfinite(value):
        raise ConfigurationError(f"{name} must be positive and finite")


class OverflowPolicy(str, Enum):
    """Choose how aggregate Journal retention pressure is handled.

    :ivar FAIL: Fail the observer when an append exceeds retention limits.
    :ivar DROP_OLDEST: Evict retained events until the new event fits, recording known loss.
    :ivar DROP_NEWEST: Discard the incoming event and record its missing Journal sequence.
    """

    FAIL = "fail"
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


class DecodeErrorPolicy(str, Enum):
    """Choose the treatment of messages whose declared type cannot be resolved.

    :ivar FAIL_FAST: Treat decoding errors, including unknown types, as terminal observer failures.
    :ivar SKIP_UNKNOWN_TYPE: Skip only UnknownMessageTypeError; other decoding failures remain terminal.
    """

    FAIL_FAST = "fail_fast"
    SKIP_UNKNOWN_TYPE = "skip_unknown_type"


@dataclass(frozen=True, slots=True)
class JournalLimits:
    """Bound retained Journal records, accounted bytes, and loss-ledger entries.

    :ivar max_events: Maximum number of retained events.
    :ivar max_bytes: Maximum aggregate accounted bytes retained by the Journal.
    :ivar max_event_bytes: Hard raw-event limit checked before decoding.
    :ivar max_loss_ranges: Maximum explicit loss intervals before conservative coarsening.
    """

    max_events: int = 10_000
    max_bytes: int = 64 * MiB
    max_event_bytes: int = 16 * MiB
    max_loss_ranges: int = 1024

    def __post_init__(self) -> None:
        """Validate positive integer limits and ensure one raw event can fit the aggregate budget.

        :raises ConfigurationError: A limit is invalid or max_event_bytes exceeds max_bytes.
        """
        for f in fields(self):
            positive(getattr(self, f.name), f.name, integer=True)
        if self.max_event_bytes > self.max_bytes:
            raise ConfigurationError("max_event_bytes cannot exceed max_bytes")


@dataclass(frozen=True, slots=True)
class JournalPolicy:
    """Combine Journal retention limits with an explicit overflow decision.

    :ivar limits: Immutable event, byte, and loss-ledger limits.
    :ivar overflow: Overflow policy normalized to an OverflowPolicy member.
    """

    limits: JournalLimits = field(default_factory=JournalLimits)
    overflow: OverflowPolicy = OverflowPolicy.FAIL

    def __post_init__(self) -> None:
        """Validate the limits object and normalize the configured overflow policy.

        :raises ConfigurationError: Limits have the wrong type or the overflow value is unsupported.
        """
        if not isinstance(self.limits, JournalLimits):
            raise ConfigurationError("limits must be JournalLimits")
        try:
            object.__setattr__(self, "overflow", OverflowPolicy(self.overflow))
        except ValueError as exc:
            raise ConfigurationError("Invalid overflow policy") from exc


@dataclass(frozen=True, slots=True)
class RuntimeLimits:
    """Bound live expectation state and the amount of work performed per scan.

    :ivar max_active_waiters: Maximum simultaneous expectations on one watcher.
    :ivar max_scopes: Maximum scope definitions created during a watch.
    :ivar max_result_events: Maximum requested count in one expectation result.
    :ivar max_result_bytes: Maximum accounted bytes reserved by unfinished expectation results.
    :ivar scan_batch_events: Maximum events scanned before yielding to other runtime tasks.
    """

    max_active_waiters: int = 128
    max_scopes: int = 256
    max_result_events: int = 10_000
    max_result_bytes: int = 64 * MiB
    scan_batch_events: int = 64

    def __post_init__(self) -> None:
        """Require every runtime limit to be a positive integer.

        :raises ConfigurationError: A runtime limit is invalid.
        """
        for f in fields(self):
            positive(getattr(self, f.name), f.name, integer=True)


@dataclass(frozen=True, slots=True)
class TransportBufferLimits:
    """Bound the transport-to-runtime handoff using an accounted memory model.

    :ivar max_events: Maximum pending handoff records, including adapter-owned transfers.
    :ivar max_bytes: Maximum accounted bytes allowed in the handoff.
    """

    max_events: int = 256
    max_bytes: int = 16 * MiB

    def __post_init__(self) -> None:
        """Require positive integer handoff limits.

        :raises ConfigurationError: A record or byte limit is invalid.
        """
        for f in fields(self):
            positive(getattr(self, f.name), f.name, integer=True)


@dataclass(frozen=True, slots=True)
class LifecyclePolicy:
    """Configure finite startup, shutdown, and synchronous bridge budgets in seconds.

    :ivar startup_timeout: Overall observer startup budget.
    :ivar shutdown_timeout: Best-effort observer cleanup budget.
    :ivar sync_control_timeout: Budget for synchronous snapshot, scope, mark, and control calls.
    :ivar cancellation_grace: Additional interval allowed for a cancelled bridge operation to acknowledge
        completion.
    """

    startup_timeout: float = 15.0
    shutdown_timeout: float = 5.0
    sync_control_timeout: float = 5.0
    cancellation_grace: float = 1.0

    def __post_init__(self) -> None:
        """Require every lifecycle duration to be positive and finite.

        :raises ConfigurationError: A duration is invalid.
        """
        for f in fields(self):
            positive(getattr(self, f.name), f.name)


@dataclass(frozen=True, slots=True)
class DecodingPolicy:
    """Select whether explicitly unknown message types may be filtered.

    :ivar error_policy: Normalized unknown-type decoding policy.
    """

    error_policy: DecodeErrorPolicy = DecodeErrorPolicy.FAIL_FAST

    def __post_init__(self) -> None:
        """Normalize the selected decode error policy.

        :raises ConfigurationError: The error policy is unsupported.
        """
        try:
            object.__setattr__(self, "error_policy", DecodeErrorPolicy(self.error_policy))
        except ValueError as exc:
            raise ConfigurationError("Invalid decoding policy") from exc


@dataclass(frozen=True, slots=True)
class ObservationIdentity:
    """Carry optional test-run identities without consulting pytest global state.

    :ivar run_id: Optional identity of the enclosing test run.
    :ivar worker_id: Optional process or pytest worker identity.
    :ivar test_id: Optional test identity supplied by the caller.
    """

    run_id: str | None = None
    worker_id: str | None = None
    test_id: str | None = None
