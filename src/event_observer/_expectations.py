"""Scheduler-independent Journal expectation state shared by both runtimes.

The owner supplies lifecycle checks, payload copying, and result-byte accounting.
This module scans a bounded batch without waiting, scheduling, or owning threads.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

from ._runtime.clock import Clock
from .config import positive
from .diagnostics import DiagnosticCollector
from .errors import (
    ConfigurationError,
    EventAssertionError,
    EventObserverError,
    EventTimeoutError,
    ExpectationResourceLimitError,
    IncompleteObservationError,
    InvalidMatcherResultError,
    MatcherEvaluationError,
    PayloadIsolationError,
)
from .journal import EventJournal
from .matchers import EventMatcher, MatchResult, MatchStatus
from .models import ObservationMark, ObservedEvent

T = TypeVar("T")


def effective_lower_bound(
    *, observer_id: str, lower_bound: int, since: ObservationMark | None, next_sequence: int
) -> int:
    """Intersect an immutable scope range with a validated watcher-local mark.

    :param observer_id: Identity of the watcher evaluating the expectation.
    :param lower_bound: Existing inclusive scope boundary.
    :param since: Optional reusable admission mark from this watcher.
    :param next_sequence: Current next Journal admission position.
    :returns: The unchanged lower boundary or its maximum with the mark sequence.
    :raises ConfigurationError: The mark has the wrong type, ownership, or a future sequence.
    """
    if since is None:
        return lower_bound
    if not isinstance(since, ObservationMark):
        raise ConfigurationError("since must be an ObservationMark from this watcher")
    if since.observer_id != observer_id or since.sequence > next_sequence:
        raise ConfigurationError("Observation mark belongs to another watcher or a future position")
    return max(lower_bound, since.sequence)


def validate_expectation(
    count: int, timeout: float, require_complete: bool, *, max_result_events: int
) -> None:
    """Validate shared public wait arguments before allocating active runtime state.

    :param count: Requested positive number of distinct Journal records.
    :param timeout: Positive finite admission-window duration in seconds.
    :param require_complete: Whether known loss must prevent an assertion result.
    :param max_result_events: Maximum permitted count from the runtime policy.
    :raises ConfigurationError: A count, duration, or completeness flag is invalid.
    :raises ExpectationResourceLimitError: The requested count exceeds the result limit.
    """
    positive(timeout, "timeout")
    if not isinstance(require_complete, bool):
        raise ConfigurationError("require_complete must be bool")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ConfigurationError("count must be a positive integer")
    if count > max_result_events:
        raise ExpectationResourceLimitError("Expectation resource limit reached")


class ExpectationStatus(str, Enum):
    """Describe the result of one bounded scheduling-independent scan.

    :ivar PENDING: More history or a future Journal change may satisfy the wait.
    :ivar MATCHED: The requested result count passed the final state checks.
    :ivar TIMEOUT: The final eligible range was exhausted without sufficient matches.
    """

    PENDING = "pending"
    MATCHED = "matched"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class ExpectationStep:
    """Return scheduling hints without making the engine own a scheduler.

    :ivar status: Pending, matched, or timed-out expectation decision.
    :ivar scanned: Whether a nonempty batch was processed and the caller should yield or pump.
    :ivar version: Journal version captured before obtaining this scan batch.
    """

    status: ExpectationStatus
    scanned: bool
    version: int


class ExpectationState(Generic[T]):
    """Own one independent scan cursor, result reservation, and diagnostic outcome.

    All methods run on one owning runtime. Callers must finish every registered
    attempt, including when their scheduler cancels or lifecycle checks raise.

    :ivar observer_id: Identity attached to domain failures.
    :ivar expectation_id: Stable identity of this registered wait.
    :ivar scope_id: Original scope identity retained for diagnostics.
    :ivar journal: Canonical single-owner history being scanned.
    :ivar lower_bound: Effective inclusive scope/mark boundary.
    :ivar filters: Declaration-ordered inherited correlation filters.
    :ivar matcher: Schema-bound expectation matcher evaluated after scope filters.
    :ivar count: Required number of distinct matching Journal records.
    :ivar timeout: Configured public admission-window duration.
    :ivar began: Time the wait began in the shared clock domain.
    :ivar deadline: Absolute latest eligible admission time.
    :ivar require_complete: Whether relevant known loss prevents a result.
    :ivar scan_batch_events: Maximum records processed by one step.
    :ivar clock: Shared monotonic clock used for deadlines and diagnostics.
    :ivar copy_event: Owner-supplied independent payload-copy operation.
    :ivar diagnostics: Shared bounded diagnostic collector.
    :ivar check_state: Cancellation/lifecycle/terminal check supplied by the owning runtime.
    :ivar reserve_result: Callback reserving aggregate unfinished-result bytes or raising a limit error.
    :ivar release_result: Callback releasing the supplied aggregate result-byte reservation.
    :ivar cursor: Next Journal sequence this expectation has not evaluated.
    :ivar results: Detached matched event envelopes in Journal order.
    :ivar reserved: Bytes currently reserved for these unfinished results.
    :ivar evaluated: Events evaluated by the expectation matcher after correlation.
    :ivar excluded: Events rejected by an inherited scope filter.
    :ivar not_applicable: Correlated events the expectation matcher cannot evaluate.
    :ivar mismatched: Authoritative number of correlated mismatch evaluations.
    :ivar outcome: Recorded error, matched, timeout, or cancelled outcome.
    :ivar final_upper: Exclusive upper sequence captured once the deadline is reached.
    :ivar _finished: Whether reservations and final diagnostics were already finalized.
    """

    def __init__(
        self,
        *,
        observer_id: str,
        expectation_id: str,
        scope_id: str,
        journal: EventJournal,
        lower_bound: int,
        filters: tuple[EventMatcher, ...],
        matcher: EventMatcher,
        count: int,
        timeout: float,
        began: float,
        deadline: float,
        require_complete: bool,
        scan_batch_events: int,
        clock: Clock,
        copy_event: Callable[[ObservedEvent[T]], ObservedEvent[T]],
        diagnostics: DiagnosticCollector,
        check_state: Callable[[], None],
        reserve_result: Callable[[int], None],
        release_result: Callable[[int], None],
    ) -> None:
        """Capture an already validated, bound wait without scanning or scheduling.

        :param observer_id: Owning watcher identity.
        :param expectation_id: Unique identity assigned when the wait is registered.
        :param scope_id: Original scope identity, unchanged by a mark.
        :param journal: Canonical history owned by the calling runtime.
        :param lower_bound: Validated effective inclusive search boundary.
        :param filters: Bound inherited correlation filters in parent-to-child order.
        :param matcher: Bound matcher for correlated events.
        :param count: Validated positive requested result count.
        :param timeout: Validated public admission-window duration.
        :param began: Wait start time used for elapsed diagnostics.
        :param deadline: Absolute event admission deadline.
        :param require_complete: Whether relevant retention loss invalidates the wait.
        :param scan_batch_events: Positive maximum records evaluated in one step.
        :param clock: Shared owner clock.
        :param copy_event: Payload isolation helper used for callbacks and returned events.
        :param diagnostics: Bounded collector receiving samples and the final summary.
        :param check_state: Check caller cancellation, lifecycle, and terminal failure in precedence order.
        :param reserve_result: Atomically check and reserve result bytes before making a result copy.
        :param release_result: Release bytes reserved by this state, including after a copy failure.
        """
        self.observer_id = observer_id
        self.expectation_id = expectation_id
        self.scope_id = scope_id
        self.journal = journal
        self.lower_bound = lower_bound
        self.filters = filters
        self.matcher = matcher
        self.count = count
        self.timeout = timeout
        self.began = began
        self.deadline = deadline
        self.require_complete = require_complete
        self.scan_batch_events = scan_batch_events
        self.clock = clock
        self.copy_event = copy_event
        self.diagnostics = diagnostics
        self.check_state = check_state
        self.reserve_result = reserve_result
        self.release_result = release_result
        self.cursor = lower_bound
        self.results: list[ObservedEvent[T]] = []
        self.reserved = 0
        self.evaluated = self.excluded = self.not_applicable = self.mismatched = 0
        self.outcome = "error"
        self.final_upper: int | None = None
        self._finished = False

    def _check_state(self) -> None:
        """Prioritize owner cancellation/failure before checking required-range completeness.

        :raises IncompleteObservationError: Relevant known retention loss intersects this wait.
        """
        self.check_state()
        if self.require_complete and not self.journal.complete(
            self.lower_bound, self.final_upper, deadline=self.deadline
        ):
            raise IncompleteObservationError(
                "Required observation range contains known retention loss",
                observer_id=self.observer_id,
                expectation_id=self.expectation_id,
                diagnostics=self.diagnostics.snapshot(),
            )

    def _evaluate(self, matcher: EventMatcher, event: ObservedEvent[T]) -> MatchResult:
        """Evaluate one protected argument while preserving known failure precedence.

        :param matcher: Bound scope or expectation matcher to invoke.
        :param event: Canonical event requiring an independent callback copy.
        :returns: The matcher result on the isolated envelope.
        :raises MatcherEvaluationError: Evaluation fails without a higher-priority owner failure.
        :raises InvalidMatcherResultError: The callback produces an invalid result.
        :raises PayloadIsolationError: The callback argument cannot be copied safely.
        """
        try:
            return matcher.match_event(self.copy_event(event))
        except (MatcherEvaluationError, InvalidMatcherResultError, PayloadIsolationError):
            self._check_state()
            raise

    def step(self) -> ExpectationStep:
        """Scan one bounded stable Journal batch without waiting for new data.

        Capture the caller notification before invoking this method. After an
        empty pending step, wait only if its version and that notification are
        still current. A nonempty pending scan requires a cooperative yield or
        native-runtime pump before scanning again.

        :returns: A pending/matched/timeout decision plus batch and version information.
        :raises IncompleteObservationError: Required history has relevant known loss.
        :raises ExpectationResourceLimitError: The owner cannot reserve matched result bytes.
        :raises ConfigurationError: This attempt was already finalized.
        """
        if self._finished:
            raise ConfigurationError("A finalized expectation cannot be scanned")
        self._check_state()
        if self.clock.monotonic() >= self.deadline and self.final_upper is None:
            self.final_upper = self.journal.next_sequence
        version = self.journal.version
        scanned_upper = self.journal.next_sequence
        batch = self.journal.batch(
            self.cursor,
            self.scan_batch_events,
            self.final_upper if self.final_upper is not None else scanned_upper,
        )
        for event, size in batch:
            self.cursor = event.sequence + 1
            if event.recorded_monotonic > self.deadline:
                continue
            callback_start = self.clock.monotonic()
            included = True
            for scope_filter in self.filters:
                if self._evaluate(scope_filter, event).status != MatchStatus.MATCH:
                    included = False
                    self.excluded += 1
                    break
            if not included:
                continue
            result = self._evaluate(self.matcher, event)
            self.evaluated += 1
            duration = self.clock.monotonic() - callback_start
            if duration > 0.05:
                self.diagnostics.record("slow_callback", elapsed=duration)
            if result.status == MatchStatus.NOT_APPLICABLE:
                self.not_applicable += 1
            elif result.status == MatchStatus.MISMATCH:
                self.mismatched += 1
                self.diagnostics.record_mismatch(
                    self.expectation_id, event.sequence, result.mismatches, payload=event.payload
                )
            elif result.status == MatchStatus.MATCH:
                self.reserve_result(size)
                self.reserved += size
                self.results.append(self.copy_event(event))
                self.diagnostics.record_match(self.expectation_id, event.sequence, payload=event.payload)
                if len(self.results) == self.count:
                    self._check_state()
                    self.outcome = "matched"
                    return ExpectationStep(ExpectationStatus.MATCHED, True, version)
        if batch:
            batch.clear()
            return ExpectationStep(ExpectationStatus.PENDING, True, version)
        self.cursor = max(self.cursor, scanned_upper if self.final_upper is None else self.final_upper)
        if self.final_upper is not None:
            self._check_state()
            self.outcome = "timeout"
            return ExpectationStep(ExpectationStatus.TIMEOUT, False, version)
        return ExpectationStep(ExpectationStatus.PENDING, False, version)

    def timeout_error(self) -> EventTimeoutError:
        """Construct the shared timeout assertion before final diagnostics are recorded.

        :returns: An EventTimeoutError containing the currently retained diagnostic snapshot.
        """
        return EventTimeoutError(
            "No sufficient matching events within the observation deadline",
            observer_id=self.observer_id,
            expectation_id=self.expectation_id,
            diagnostics=self.diagnostics.snapshot(),
        )

    def finish(self, error: BaseException | None = None, *, cancelled: bool = False) -> None:
        """Release reservations and record the completed attempt once, including partial results.

        :param error: Failure being propagated by the owner, or None after a successful result.
        :param cancelled: Preserve the scheduler cancellation outcome without normalizing its exception.
        """
        if self._finished:
            return
        self._finished = True
        outcome_error = None if cancelled else error
        if cancelled:
            self.outcome = "cancelled"
        if isinstance(outcome_error, (EventObserverError, EventAssertionError)):
            outcome_error.observer_id = self.observer_id
            outcome_error.expectation_id = self.expectation_id
            outcome_error.sequence = self.cursor - 1 if self.cursor > self.lower_bound else None
        self.release_result(self.reserved)
        self.reserved = 0
        self.diagnostics.record_expectation(
            expectation_id=self.expectation_id,
            scope_id=self.scope_id,
            outcome=self.outcome,
            evaluated=self.evaluated,
            matched=len(self.results),
            mismatch_count=self.mismatched,
            scope_excluded=self.excluded,
            not_applicable=self.not_applicable,
            deadline=self.deadline,
            timeout=self.timeout,
            elapsed=self.clock.monotonic() - self.began,
            deadline_overrun=max(0.0, self.clock.monotonic() - self.deadline),
            requested_count=self.count,
            lower_bound=self.lower_bound,
            error_code=getattr(outcome_error, "code", None),
            error_type=type(outcome_error).__name__ if outcome_error is not None else None,
            require_complete=self.require_complete,
            complete=self.journal.complete(self.lower_bound, self.final_upper, deadline=self.deadline),
        )
        if isinstance(outcome_error, (EventObserverError, EventAssertionError)):
            outcome_error.diagnostics = self.diagnostics.snapshot()
