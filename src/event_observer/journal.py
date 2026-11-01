"""Single-loop bounded history with conservative range-aware loss accounting."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from .config import JournalPolicy, OverflowPolicy
from .errors import EventTooLargeError, JournalOverflowError
from .models import ObservedEvent, RawEvent


def raw_size(raw: RawEvent) -> int:
    """128 bytes envelope + raw data + UTF-8 source/metadata and 32 bytes per pair.

    Decoded payload uses PayloadAccess.estimate_bytes plus 128 record overhead.
    These deterministic estimates are deliberately distinct from total native RSS.
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
    first_sequence: int
    last_sequence: int
    reason: str = "retention"
    first_recorded_monotonic: float = 0.0


@dataclass(frozen=True, slots=True)
class JournalSnapshot:
    observer_id: str
    events: tuple[ObservedEvent[Any], ...]
    next_sequence: int
    version: int
    retained_bytes: int
    losses: tuple[LossRange, ...]
    complete: bool
    filtered: int


class EventJournal:
    def __init__(self, observer_id: str, policy: JournalPolicy) -> None:
        self.observer_id = observer_id
        self.policy = policy
        self.events: dict[int, tuple[ObservedEvent[Any], int]] = {}
        self.losses: deque[LossRange] = deque()
        self.next_sequence = 0
        self.version = 0
        self.retained_bytes = 0
        self.filtered = 0

    def check_raw(self, raw: RawEvent) -> int:
        size = raw_size(raw)
        if size > self.policy.limits.max_event_bytes:
            raise EventTooLargeError("Raw event exceeds the hard single-event budget")
        return size

    def _loss(self, sequence: int, recorded_monotonic: float) -> None:
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
        limits = self.policy.limits
        self.next_sequence += 1
        self.version += 1

        def overflow() -> bool:
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
        self.next_sequence += 1
        self.version += 1
        self.filtered += 1

    def complete(self, lower: int = 0, upper: int | None = None, *, deadline: float | None = None) -> bool:
        upper = self.next_sequence if upper is None else upper
        return not any(
            loss.first_sequence < upper
            and loss.last_sequence >= lower
            and (deadline is None or loss.first_recorded_monotonic <= deadline)
            for loss in self.losses
        )

    def batch(self, lower: int, limit: int, upper: int | None = None) -> list[tuple[ObservedEvent[Any], int]]:
        result = []
        for sequence, entry in self.events.items():
            if sequence >= lower and (upper is None or sequence < upper):
                result.append(entry)
                if len(result) == limit:
                    break
        return result

    def snapshot(self, copy_event: Callable) -> JournalSnapshot:
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
