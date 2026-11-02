"""Scheduling-independent expectation policy and result-accounting regressions."""

import ast
import copy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from event_observer._expectations import (
    ExpectationState,
    ExpectationStatus,
    effective_lower_bound,
    validate_expectation,
)
from event_observer.config import JournalLimits, JournalPolicy, OverflowPolicy
from event_observer.diagnostics import DiagnosticCollector
from event_observer.errors import (
    ConfigurationError,
    EventTimeoutError,
    ExpectationResourceLimitError,
    IncompleteObservationError,
    ObserverFailedError,
    PayloadIsolationError,
    TransportError,
)
from event_observer.journal import EventJournal
from event_observer.matchers import MatchResult, MatchStatus, bind_matcher
from event_observer.models import ObservationMark, ObservedEvent, RawEvent


class Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def utcnow(self):
        return datetime(2026, 11, 2, tzinfo=timezone.utc)


class Cancelled(BaseException):
    pass


def journal(*, max_events=100, overflow=OverflowPolicy.FAIL):
    return EventJournal(
        "observer",
        JournalPolicy(JournalLimits(max_events=max_events, max_bytes=65536, max_event_bytes=1024), overflow),
    )


def append(history, payload=b"value", *, recorded=100.0, size=128):
    event = ObservedEvent(
        "observer",
        history.next_sequence,
        payload,
        RawEvent(b"wire", "events"),
        datetime(2026, 11, 2, tzinfo=timezone.utc),
        recorded,
    )
    history.append(event, size)
    return event


def make_state(history, *, matcher=lambda _: True, filters=(), count=1, max_bytes=65536, **kwargs):
    clock = kwargs.pop("clock", Clock())
    pool = kwargs.pop("pool", {"bytes": 0})
    collector = kwargs.pop("diagnostics", DiagnosticCollector("observer", clock=clock))

    def copier(event):
        return replace(event, payload=copy.deepcopy(event.payload))

    def reserve(size):
        if pool["bytes"] + size > max_bytes:
            raise ExpectationResourceLimitError("Synthetic result limit")
        pool["bytes"] += size

    def release(size):
        pool["bytes"] -= size

    options = dict(
        observer_id="observer",
        expectation_id="expectation",
        scope_id="scope",
        journal=history,
        lower_bound=0,
        filters=tuple(bind_matcher(value) for value in filters),
        matcher=bind_matcher(matcher),
        count=count,
        timeout=5,
        began=100,
        deadline=105,
        require_complete=True,
        scan_batch_events=2,
        clock=clock,
        copy_event=copier,
        diagnostics=collector,
        check_state=lambda: None,
        reserve_result=reserve,
        release_result=release,
    )
    options.update(kwargs)
    return ExpectationState(**options), clock, pool, collector


def test_engine_imports_no_scheduler_or_threading_module():
    path = Path(__file__).parents[1] / "src/event_observer/_expectations.py"
    tree = ast.parse(path.read_text())
    imports = [node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))]
    names = {
        alias.name.split(".")[0] for node in imports if isinstance(node, ast.Import) for alias in node.names
    }
    names.update(node.module.split(".")[0] for node in imports if isinstance(node, ast.ImportFrom))
    assert not names.intersection({"asyncio", "threading", "concurrent"})


def test_independent_cursors_reuse_history_without_consuming_records():
    history = journal()
    append(history, b"old")
    append(history, b"wanted")
    one, _, pool, _ = make_state(history, matcher=lambda value: value == b"wanted")
    two, _, _, _ = make_state(history, matcher=lambda value: value == b"wanted", pool=pool)
    assert one.step().status is ExpectationStatus.MATCHED
    assert two.step().status is ExpectationStatus.MATCHED
    assert one.results[0].sequence == two.results[0].sequence == 1
    assert pool["bytes"] == 256
    one.finish()
    two.finish()
    assert pool["bytes"] == 0 and len(history.events) == 2


def test_count_uses_distinct_records_and_preserves_scope_identity_and_effective_lower():
    history = journal()
    append(history, b"same")
    append(history, b"same")
    append(history, b"same")
    state, _, pool, collector = make_state(history, count=2, lower_bound=1, scope_id="original-scope")
    assert state.step().status is ExpectationStatus.MATCHED
    assert [event.sequence for event in state.results] == [1, 2]
    state.finish()
    summary = collector.snapshot().expectations[-1]
    assert summary.scope_id == "original-scope" and summary.lower_bound == 1
    assert summary.matched == 2 and summary.requested_count == 2 and pool["bytes"] == 0


def test_empty_pending_step_returns_notification_version_without_advancing_past_future_records():
    history = journal()
    state, _, _, _ = make_state(history)
    pending = state.step()
    assert pending.status is ExpectationStatus.PENDING and not pending.scanned
    assert pending.version == history.version and state.cursor == 0
    append(history, b"new")
    assert state.step().status is ExpectationStatus.MATCHED
    state.finish()


def test_final_scan_includes_exact_deadline_and_finishes_in_bounded_batches_after_deadline():
    history = journal()
    for recorded in (100, 104, 105):
        append(history, recorded=recorded)
    state, clock, _, _ = make_state(history, count=3, scan_batch_events=1)
    clock.now = 106
    assert state.step().status is ExpectationStatus.PENDING
    assert state.final_upper == 3
    append(history, b"too late", recorded=106)
    assert state.step().status is ExpectationStatus.PENDING
    assert state.step().status is ExpectationStatus.MATCHED
    assert state.final_upper == 3 and [event.sequence for event in state.results] == [0, 1, 2]
    state.finish()


def test_timeout_keeps_partial_count_diagnostics_and_releases_reserved_results_once():
    history = journal()
    append(history, b"matched", recorded=105)
    append(history, b"late", recorded=105.1)
    state, clock, pool, collector = make_state(history, count=2)
    clock.now = 106
    assert state.step().status is ExpectationStatus.PENDING
    assert pool["bytes"] == 128
    assert state.step().status is ExpectationStatus.TIMEOUT
    error = state.timeout_error()
    assert isinstance(error, EventTimeoutError)
    state.finish(error)
    state.finish(error)
    summary = collector.snapshot().expectations[-1]
    assert summary.outcome == "timeout" and summary.matched == 1
    assert summary.evaluated == 1 and summary.deadline_overrun == 1
    assert error.sequence == 1 and error.diagnostics.expectations[-1].outcome == "timeout"
    assert pool["bytes"] == 0 and len(collector.snapshot().expectations) == 1


def test_scope_filters_short_circuit_and_expectation_counts_are_authoritative():
    history = journal()
    for value in (b"excluded", b"na", b"mismatch", b"matched"):
        append(history, value)
    second_filter_calls = []

    def second(value):
        second_filter_calls.append(value)
        return True

    class Matcher:
        def match(self, value):
            if value == b"na":
                return MatchResult(MatchStatus.NOT_APPLICABLE)
            return MatchResult(MatchStatus.MATCH if value == b"matched" else MatchStatus.MISMATCH)

    state, _, _, collector = make_state(
        history,
        matcher=Matcher(),
        filters=(lambda value: value != b"excluded", second),
        scan_batch_events=4,
    )
    assert state.step().status is ExpectationStatus.MATCHED
    state.finish()
    assert second_filter_calls == [b"na", b"mismatch", b"matched"]
    summary = collector.snapshot().expectations[-1]
    assert (summary.scope_excluded, summary.evaluated, summary.not_applicable, summary.mismatch_count) == (
        1,
        3,
        1,
        1,
    )


def test_each_filter_matcher_and_returned_result_gets_an_independent_payload():
    history = journal()
    canonical = append(history, ["original"])

    def mutate_filter(value):
        value.append("filter mutation")
        return True

    state, _, _, _ = make_state(
        history, filters=(mutate_filter,), matcher=lambda value: value == ["original"]
    )
    assert state.step().status is ExpectationStatus.MATCHED
    state.results[0].payload.append("result mutation")
    assert canonical.payload == ["original"]
    state.finish()


@pytest.mark.parametrize("best_effort", [False, True])
def test_known_retention_loss_prevents_strict_success_but_not_explicit_best_effort(best_effort):
    history = journal(max_events=1, overflow=OverflowPolicy.DROP_OLDEST)
    append(history, b"lost")
    append(history, b"retained")
    state, _, _, collector = make_state(history, require_complete=not best_effort)
    if best_effort:
        assert state.step().status is ExpectationStatus.MATCHED
        state.finish()
    else:
        with pytest.raises(IncompleteObservationError) as caught:
            state.step()
        state.finish(caught.value)
    assert collector.snapshot().expectations[-1].complete is False


def test_drop_newest_loss_admitted_after_deadline_does_not_invalidate_eligible_success():
    history = journal(max_events=1, overflow=OverflowPolicy.DROP_NEWEST)
    append(history, b"eligible", recorded=104)
    append(history, b"outside window", recorded=110)
    state, clock, _, collector = make_state(history)
    clock.now = 111
    assert state.step().status is ExpectationStatus.MATCHED
    state.finish()
    assert collector.snapshot().expectations[-1].complete is True


def test_new_lower_boundary_excludes_earlier_loss_without_mutating_journal():
    history = journal(max_events=1, overflow=OverflowPolicy.DROP_OLDEST)
    append(history, b"lost")
    append(history, b"retained")
    state, _, _, collector = make_state(history, lower_bound=1)
    assert state.step().status is ExpectationStatus.MATCHED
    state.finish()
    assert not history.complete() and collector.snapshot().expectations[-1].complete


def test_known_terminal_failure_wins_over_matcher_exception():
    history = journal()
    append(history)
    failure = {"error": None}

    def check():
        if failure["error"] is not None:
            raise failure["error"]

    def matcher(value):
        failure["error"] = ObserverFailedError(cause=TransportError("Synthetic terminal failure"))
        raise ValueError("Synthetic callback failure")

    state, _, _, collector = make_state(history, matcher=matcher, check_state=check)
    with pytest.raises(ObserverFailedError) as caught:
        state.step()
    state.finish(caught.value)
    assert collector.snapshot().expectations[-1].error_code == "observer_failed"
    assert caught.value.sequence == 0


def test_cancellation_wins_over_known_incompleteness_and_preserves_partial_results():
    history = journal(max_events=1, overflow=OverflowPolicy.DROP_OLDEST)
    append(history)
    cancelled = False

    def check():
        if cancelled:
            raise Cancelled

    state, _, pool, collector = make_state(history, count=2, check_state=check)
    assert state.step().status is ExpectationStatus.PENDING
    append(history, b"evicts partial result")
    cancelled = True
    with pytest.raises(Cancelled) as caught:
        state.step()
    state.finish(caught.value, cancelled=True)
    summary = collector.snapshot().expectations[-1]
    assert summary.outcome == "cancelled" and summary.matched == 1 and summary.error_type is None
    assert pool["bytes"] == 0


def test_shared_result_budget_is_released_after_partial_failure():
    history = journal()
    append(history)
    append(history)
    state, _, pool, collector = make_state(history, count=2, max_bytes=128)
    with pytest.raises(ExpectationResourceLimitError) as caught:
        state.step()
    assert pool["bytes"] == 128
    state.finish(caught.value)
    assert pool["bytes"] == 0 and collector.snapshot().expectations[-1].matched == 1


def test_failed_result_copy_releases_its_prechecked_reservation():
    history = journal()
    append(history)
    copies = 0

    def copier(event):
        nonlocal copies
        copies += 1
        if copies == 2:
            raise PayloadIsolationError("Synthetic result-copy failure")
        return event

    state, _, pool, _ = make_state(history, copy_event=copier)
    with pytest.raises(PayloadIsolationError) as caught:
        state.step()
    state.finish(caught.value)
    assert pool["bytes"] == 0


def test_final_commit_rechecks_terminal_state_after_matcher_success():
    history = journal()
    append(history)
    failure = False

    def matcher(value):
        nonlocal failure
        failure = True
        return True

    def check():
        if failure:
            raise ObserverFailedError(cause=TransportError("Synthetic failure before commit"))

    state, _, pool, collector = make_state(history, matcher=matcher, check_state=check)
    with pytest.raises(ObserverFailedError) as caught:
        state.step()
    state.finish(caught.value)
    assert collector.snapshot().expectations[-1].outcome == "error" and pool["bytes"] == 0


def test_mark_intersection_rejects_wrong_ownership_type_and_future_position():
    mark = ObservationMark("observer", 3, datetime.now(timezone.utc), 100)
    assert effective_lower_bound(observer_id="observer", lower_bound=5, since=mark, next_sequence=5) == 5
    assert effective_lower_bound(observer_id="observer", lower_bound=0, since=mark, next_sequence=5) == 3
    for invalid in (3, replace(mark, observer_id="other"), replace(mark, sequence=6)):
        with pytest.raises(ConfigurationError):
            effective_lower_bound(observer_id="observer", lower_bound=0, since=invalid, next_sequence=5)


@pytest.mark.parametrize(
    "count,timeout,complete,error",
    [
        (0, 1, True, ConfigurationError),
        (True, 1, True, ConfigurationError),
        (1, 0, True, ConfigurationError),
        (1, float("inf"), True, ConfigurationError),
        (1, 1, "yes", ConfigurationError),
        (3, 1, True, ExpectationResourceLimitError),
    ],
)
def test_shared_argument_validation_precedes_runtime_registration(count, timeout, complete, error):
    with pytest.raises(error):
        validate_expectation(count, timeout, complete, max_result_events=2)
