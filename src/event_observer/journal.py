"""Single-loop bounded history with conservative range-aware loss accounting."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from .config import JournalPolicy, OverflowPolicy
from .errors import EventTooLargeError, JournalOverflowError
from .models import ObservedEvent, RawEvent


def raw_size(raw: RawEvent) -> int:
    """Account for the raw envelope, payload, key, headers, and scalar metadata.

    The model adds 128 envelope bytes, UTF-8 source/metadata sizes, and 32 bytes
    per header or metadata pair. Decoded payload is accounted separately through
    PayloadAccess. These estimates do not measure native or process RSS.

    :param raw: Validated transport envelope to measure.
    :returns: Deterministic accounted raw bytes, including the envelope overhead.
    """
    total = 128 + len(raw.payload or b"") + len(raw.key or b"") + len(raw.source.encode("utf-8"))
    for name, value in raw.headers:
        total += 32 + len(name.encode("utf-8")) + len(value or b"")
    for name, value in raw.metadata.items():
        total += 32 + len(name.encode("utf-8"))
        total += len(value) if isinstance(value, bytes) else len(str(value).encode("utf-8"))
    return total


@dataclass(frozen=True, slots=True)
class LossRange:
    """Describe an inclusive interval of known or conservatively coarsened Journal loss.

    :ivar first_sequence: First potentially missing sequence in the inclusive interval.
    :ivar last_sequence: Last potentially missing sequence in the inclusive interval.
    :ivar reason: Retention loss code, or conservative when several intervals were merged.
    :ivar first_recorded_monotonic: Earliest admission time represented by the interval, used for
        deadline-aware completeness.
    """

    first_sequence: int
    last_sequence: int
    reason: str = "retention"
    first_recorded_monotonic: float = 0.0


@dataclass(frozen=True, slots=True)
class JournalSnapshot:
    """Expose detached retained events and immutable Journal accounting at one point in time.

    :ivar observer_id: Identity of the owning watcher.
    :ivar events: Retained events in sequence order, copied through the caller-supplied isolation helper.
    :ivar next_sequence: Position that will be assigned to the next admission or explicit filter.
    :ivar version: Journal mutation version used by the notification protocol.
    :ivar retained_bytes: Accounted bytes retained by the Journal.
    :ivar losses: Inclusive known or conservative loss ranges.
    :ivar complete: Whether the complete logical range has no known loss.
    :ivar filtered: Number of deliveries explicitly skipped by the decoding policy.
    """

    observer_id: str
    events: tuple[ObservedEvent[Any], ...]
    next_sequence: int
    version: int
    retained_bytes: int
    losses: tuple[LossRange, ...]
    complete: bool
    filtered: int


class EventJournal:
    """Own single-loop retained history and bounded conservative loss accounting.

    :ivar observer_id: Identity shared by events and snapshots in this Journal.
    :ivar policy: Retention limits and overflow behavior.
    :ivar events: Insertion-ordered mapping from sequence to canonical event and accounted size.
    :ivar losses: Bounded deque of inclusive missing sequence intervals.
    :ivar next_sequence: Next admission position, also advanced for explicitly filtered deliveries.
    :ivar version: Mutation version incremented on append and explicit filtering.
    :ivar retained_bytes: Aggregate accounted bytes of currently retained events.
    :ivar filtered: Total deliveries explicitly skipped without a retention-loss gap.
    """

    def __init__(self, observer_id: str, policy: JournalPolicy) -> None:
        """Create an empty Journal without opening transport or background resources.

        :param observer_id: Identity to place on detached snapshots.
        :param policy: Immutable retention and overflow policy.
        """
        self.observer_id = observer_id
        self.policy = policy
        self.events: dict[int, tuple[ObservedEvent[Any], int]] = {}
        self.losses: deque[LossRange] = deque()
        self.next_sequence = 0
        self.version = 0
        self.retained_bytes = 0
        self.filtered = 0

    def check_raw(self, raw: RawEvent) -> int:
        """Measure a raw delivery and enforce the pre-decode hard event limit.

        :param raw: Transport envelope about to be decoded.
        :returns: Accounted raw bytes.
        :raises EventTooLargeError: The raw envelope exceeds max_event_bytes.
        """
        size = raw_size(raw)
        if size > self.policy.limits.max_event_bytes:
            raise EventTooLargeError("Raw event exceeds the hard single-event budget")
        return size

    def _loss(self, sequence: int, recorded_monotonic: float) -> None:
        """Record a missing admission and coarsen excess intervals without erasing incompleteness.

        :param sequence: Missing watcher-local Journal position.
        :param recorded_monotonic: Admission time associated with the missing event.
        """
        if self.losses and self.losses[-1].last_sequence + 1 == sequence:
            last = self.losses.pop()
            self.losses.append(
                LossRange(
                    last.first_sequence,
                    sequence,
                    last.reason,
                    min(last.first_recorded_monotonic, recorded_monotonic),
                )
            )
        else:
            self.losses.append(LossRange(sequence, sequence, first_recorded_monotonic=recorded_monotonic))
        if len(self.losses) > self.policy.limits.max_loss_ranges:
            # Coarsen, never erase known incompleteness. Later scopes stay usable.
            first = self.losses.popleft()
            second = self.losses.popleft()
            self.losses.appendleft(
                LossRange(
                    first.first_sequence,
                    second.last_sequence,
                    "conservative",
                    min(first.first_recorded_monotonic, second.first_recorded_monotonic),
                )
            )

    def append(self, event: ObservedEvent[Any], size: int) -> bool:
        """Advance admission state and retain or explicitly discard one canonical event.

        :param event: Canonical decoded event carrying the next Journal sequence.
        :param size: Combined raw, decoded, and record-overhead accounting supplied by Core.
        :returns: True when retained; False when an explicit DROP policy discards the incoming event.
        :raises JournalOverflowError: The aggregate retention limit is exceeded under FAIL.
        """
        limits = self.policy.limits
        self.next_sequence += 1
        self.version += 1

        def overflow() -> bool:
            """Check whether retaining the pending event would exceed an aggregate limit.

            :returns: True when count or accounted bytes require an overflow decision.
            """
            return len(self.events) >= limits.max_events or self.retained_bytes + size > limits.max_bytes

        if overflow():
            if self.policy.overflow == OverflowPolicy.FAIL:
                raise JournalOverflowError("Journal retention budget exhausted")
            if self.policy.overflow == OverflowPolicy.DROP_NEWEST or size > limits.max_bytes:
                self._loss(event.sequence, event.recorded_monotonic)
                return False
            while self.events and overflow():
                first = next(iter(self.events))
                old_event, old_size = self.events.pop(first)
                self.retained_bytes -= old_size
                self._loss(first, old_event.recorded_monotonic)
        self.events[event.sequence] = (event, size)
        self.retained_bytes += size
        return True

    def skip_filtered(self) -> None:
        """Advance admission/version counters for an intentionally skipped unknown type."""
        self.next_sequence += 1
        self.version += 1
        self.filtered += 1

    def complete(self, lower: int = 0, upper: int | None = None, *, deadline: float | None = None) -> bool:
        """Check whether a half-open Journal range is free of relevant known loss.

        :param lower: Inclusive logical lower sequence.
        :param upper: Exclusive upper sequence; defaults to the current next sequence.
        :param deadline: Optional admission deadline; later loss intervals do not affect this query.
        :returns: True when no recorded loss intersects the requested range and deadline.
        """
        upper = self.next_sequence if upper is None else upper
        return not any(
            loss.first_sequence < upper
            and loss.last_sequence >= lower
            and (deadline is None or loss.first_recorded_monotonic <= deadline)
            for loss in self.losses
        )

    def batch(self, lower: int, limit: int, upper: int | None = None) -> list[tuple[ObservedEvent[Any], int]]:
        """Pin a bounded stable selection of retained canonical events for one scan.

        :param lower: Inclusive sequence from which to scan.
        :param limit: Maximum number of retained entries to return.
        :param upper: Optional exclusive upper sequence.
        :returns: Sequence-ordered event/size pairs; the caller must isolate payloads before user
            callbacks.
        """
        result = []
        for sequence, entry in self.events.items():
            if sequence >= lower and (upper is None or sequence < upper):
                result.append(entry)
                if len(result) == limit:
                    break
        return result

    def snapshot(self, copy_event: Callable) -> JournalSnapshot:
        """Copy every retained event into a detached point-in-time view.

        :param copy_event: Isolation helper applied to each canonical event.
        :returns: A JournalSnapshot containing copied events and immutable accounting.
        :raises PayloadIsolationError: The supplied event copier cannot safely detach a payload.
        """
        return JournalSnapshot(
            self.observer_id,
            tuple(copy_event(e) for e, _ in self.events.values()),
            self.next_sequence,
            self.version,
            self.retained_bytes,
            tuple(self.losses),
            self.complete(),
            self.filtered,
        )
