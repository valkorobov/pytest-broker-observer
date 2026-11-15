"""Reporting is a completed-operation boundary shared by both facades."""

import asyncio
import json
import threading

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.diagnostics import DiagnosticPolicy, DisclosurePolicy, ReportingMode
from event_observer.errors import EventTimeoutError
from event_observer.testing import FakeTransport


class CollectingReporter:
    def __init__(self):
        self.calls = []

    def emit(self, report):
        self.calls.append((threading.get_ident(), report.to_dict()))


class ObservingDecoder:
    def __init__(self, reporter):
        self.reporter = reporter
        self.calls = []

    def decode(self, raw):
        self.calls.append((threading.get_ident(), len(self.reporter.calls)))
        return raw.payload


@pytest.mark.asyncio
async def test_async_report_after_predicate_not_on_receive():
    reporter = CollectingReporter()
    decoder = ObservingDecoder(reporter)
    adapter = FakeTransport()
    owner = threading.get_ident()
    async with EventObserver(
        transport=adapter,
        reporters=[reporter],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    ).watch(source="events", decoder=decoder) as watcher:
        baseline = len(reporter.calls)
        adapter.publish(b"ok")
        predicate_reports = []

        def predicate(value):
            predicate_reports.append(len(reporter.calls))
            return value == b"ok"

        assert await watcher.wait_for(predicate, timeout=1) == b"ok"
        assert decoder.calls == [(owner, baseline)]
        assert predicate_reports == [baseline]
        assert len(reporter.calls) > baseline
    assert all(thread == owner for thread, _ in reporter.calls)


def test_sync_reports_in_caller_thread_after_runtime_callbacks():
    reporter = CollectingReporter()
    decoder = ObservingDecoder(reporter)
    adapter = FakeTransport()
    owner = threading.get_ident()
    callback_threads = []
    async_policy = DiagnosticPolicy(mode=ReportingMode.SUMMARY)
    with SyncEventObserver(
        transport=adapter,
        reporters=[reporter],
        diagnostics=async_policy,
    ).watch(source="events", decoder=decoder) as watcher:
        baseline = len(reporter.calls)
        adapter.publish(b"ok")

        def predicate(value):
            callback_threads.append((threading.get_ident(), len(reporter.calls)))
            return value == b"ok"

        assert watcher.wait_for(predicate, timeout=1) == b"ok"
        assert len(reporter.calls) > baseline
        assert decoder.calls[0][0] != owner
        assert decoder.calls[0][1] == baseline
        assert callback_threads == [(decoder.calls[0][0], baseline)]
    assert all(thread == owner for thread, _ in reporter.calls)


@pytest.mark.parametrize("sync", [False, True])
def test_errors_only_success_is_quiet_and_timeout_has_current_summary(sync):
    reporter = CollectingReporter()
    adapter = FakeTransport()

    async def async_case():
        async with EventObserver(transport=adapter, reporters=[reporter]).watch(source="events") as watcher:
            adapter.publish(b"SECRET_PAYLOAD")
            assert await watcher.wait_for(lambda _: True, timeout=1) == b"SECRET_PAYLOAD"
            assert reporter.calls == []
            with pytest.raises(EventTimeoutError) as caught:
                await watcher.wait_for(lambda _: False, timeout=0.01, description="SECRET_DESCRIPTION")
            return caught.value

    def sync_case():
        with SyncEventObserver(transport=adapter, reporters=[reporter]).watch(source="events") as watcher:
            adapter.publish(b"SECRET_PAYLOAD")
            assert watcher.wait_for(lambda _: True, timeout=1) == b"SECRET_PAYLOAD"
            assert reporter.calls == []
            with pytest.raises(EventTimeoutError) as caught:
                watcher.wait_for(lambda _: False, timeout=0.01, description="SECRET_DESCRIPTION")
            return caught.value

    error = sync_case() if sync else asyncio.run(async_case())
    summary = error.diagnostics.expectations[-1]
    assert summary.outcome == "timeout"
    assert summary.evaluated == 1
    assert summary.matched == 0
    assert summary.mismatch_count == 1
    assert len(summary.samples) == 1
    assert reporter.calls
    assert "SECRET" not in json.dumps([report for _, report in reporter.calls])


@pytest.mark.parametrize("sync", [False, True])
def test_reporter_errors_preserve_successful_result(sync):
    class BrokenReporter:
        def emit(self, report):
            raise RuntimeError("SECRET_REPORTER")

    adapter = FakeTransport()
    configuration = dict(
        transport=adapter,
        reporters=[BrokenReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )

    async def async_case():
        async with EventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"ok")
            assert await watcher.wait_for(lambda _: True, timeout=1) == b"ok"
            assert await watcher.wait_for(lambda _: True, timeout=1) == b"ok"
            return watcher.diagnostics.snapshot()

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"ok")
            assert watcher.wait_for(lambda _: True, timeout=1) == b"ok"
            assert watcher.wait_for(lambda _: True, timeout=1) == b"ok"
            return watcher.diagnostics.snapshot()

    snapshot = sync_case() if sync else asyncio.run(async_case())
    errors = [record for record in snapshot.records if record.kind == "reporter_error"]
    assert [record.attributes["reporter_errors"] for record in errors] == [1, 2]
    assert all(record.attributes["error_code"] == "reporter_failed" for record in errors)
    assert "SECRET_REPORTER" not in snapshot.to_json()


@pytest.mark.parametrize("sync", [False, True])
def test_failed_context_body_emits_without_changing_primary_exception(sync):
    reporter = CollectingReporter()
    adapter = FakeTransport()

    async def async_case():
        async with EventObserver(transport=adapter, reporters=[reporter]).watch(source="events"):
            raise ValueError("SECRET_BODY_ERROR")

    def sync_case():
        with SyncEventObserver(transport=adapter, reporters=[reporter]).watch(source="events"):
            raise ValueError("SECRET_BODY_ERROR")

    with pytest.raises(ValueError, match="SECRET_BODY_ERROR"):
        sync_case() if sync else asyncio.run(async_case())
    assert reporter.calls
    assert "SECRET" not in json.dumps([report for _, report in reporter.calls])


@pytest.mark.asyncio
async def test_scope_timeout_reports_exclusion_without_repeating_predicate():
    adapter = FakeTransport()
    reporter = CollectingReporter()
    predicate_calls = []
    async with EventObserver(transport=adapter, reporters=[reporter]).watch(source="events") as watcher:
        scope = watcher.scope(lambda value: predicate_calls.append(value) or value == b"wanted")
        adapter.publish(b"other")
        # Acknowledge admission before starting the short scope timeout.
        assert await watcher.wait_for(lambda _: True, timeout=1) == b"other"
        with pytest.raises(EventTimeoutError) as caught:
            await scope.wait_for(lambda _: True, timeout=0.01)
        summary = caught.value.diagnostics.expectations[-1]
        assert summary.scope_excluded == 1
        assert summary.evaluated == 0
        snapshot = scope.snapshot()
        assert len(snapshot.expectations) == 1
        assert predicate_calls == [b"other"]


@pytest.mark.parametrize("sync", [False, True])
def test_opted_in_runtime_disclosure_preserves_canonical_payload(sync):
    reporter = CollectingReporter()
    adapter = FakeTransport()

    class JsonDecoder:
        def decode(self, raw):
            return {"secret": "SECRET_PAYLOAD", "id": "42"}

    def redact(payload):
        payload.pop("secret")
        return payload

    configuration = dict(
        transport=adapter,
        reporters=[reporter],
        diagnostics=DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD, redactor=redact),
    )

    async def async_case():
        async with EventObserver(**configuration).watch(source="events", decoder=JsonDecoder()) as watcher:
            adapter.publish(b"wire")
            await watcher.wait_for(lambda _: True, timeout=1)
            with pytest.raises(EventTimeoutError):
                await watcher.wait_for(lambda _: False, timeout=0.01)
            assert await watcher.wait_for(lambda value: value["secret"] == "SECRET_PAYLOAD", timeout=1)
            return watcher.diagnostics.snapshot()

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events", decoder=JsonDecoder()) as watcher:
            adapter.publish(b"wire")
            watcher.wait_for(lambda _: True, timeout=1)
            with pytest.raises(EventTimeoutError):
                watcher.wait_for(lambda _: False, timeout=0.01)
            assert watcher.wait_for(lambda value: value["secret"] == "SECRET_PAYLOAD", timeout=1)
            return watcher.diagnostics.snapshot()

    snapshot = sync_case() if sync else asyncio.run(async_case())
    timeout_summary = next(summary for summary in snapshot.expectations if summary.outcome == "timeout")
    sample = timeout_summary.samples[0]
    assert sample["content"]["payload"] == {"id": "42"}
    assert "SECRET" not in json.dumps([report for _, report in reporter.calls])


class MatchedContentReporter:
    def __init__(self):
        self.reports = []
        self.threads = []

    def emit(self, report):
        self.reports.append(report)
        self.threads.append(threading.get_ident())


def matched_content(report):
    return json.loads(report.attachments["message-content.json"])["messages"]


@pytest.mark.parametrize("sync", [False, True])
def test_successful_payload_reporting_keeps_one_distinct_summary_per_wait_after_close(sync):
    reporter = MatchedContentReporter()
    adapter = FakeTransport()
    caller_thread = threading.get_ident()
    configuration = dict(
        transport=adapter,
        reporters=[reporter],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY, disclosure=DisclosurePolicy.FULL_PAYLOAD),
    )

    async def async_case():
        async with EventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"completed")
            assert await watcher.wait_for(lambda _: True, timeout=1) == b"completed"
            assert (await watcher.wait_for_event(lambda _: True, timeout=1)).payload == b"completed"
            assert len(reporter.reports) == 2
        await watcher.close()

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"completed")
            assert watcher.wait_for(lambda _: True, timeout=1) == b"completed"
            assert watcher.wait_for_event(lambda _: True, timeout=1).payload == b"completed"
            assert len(reporter.reports) == 2
        watcher.close()

    sync_case() if sync else asyncio.run(async_case())
    assert len(reporter.reports) == 2
    assert reporter.threads == [caller_thread, caller_thread]
    expectation_ids = []
    for report in reporter.reports:
        [summary] = json.loads(report.attachments["expectation-result.json"])["expectations"]
        [message] = matched_content(report)
        assert summary["outcome"] == "matched"
        assert message["payload"] == "completed"
        assert message["payload_encoding"] == "utf-8"
        assert message["match_status"] == "matched"
        assert message["event_sequence"] == 0
        assert message["expectation_id"] == summary["expectation_id"]
        expectation_ids.append(summary["expectation_id"])
        assert report.attachments["expectation-result.txt"].count("Outcome: MATCHED") == 1
    assert len(set(expectation_ids)) == 2


@pytest.mark.parametrize("sync", [False, True])
def test_broken_matched_payload_redactor_preserves_results_and_reader(sync):
    reporter = MatchedContentReporter()
    adapter = FakeTransport()

    class JsonDecoder:
        def decode(self, raw):
            return {"id": "42", "secret": "SECRET_PAYLOAD"}

    def broken_redactor(payload):
        payload.pop("secret")
        raise ValueError("SECRET_REDACTOR_ERROR")

    configuration = dict(
        transport=adapter,
        reporters=[reporter],
        diagnostics=DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            disclosure=DisclosurePolicy.REDACTED_PAYLOAD,
            redactor=broken_redactor,
        ),
    )

    async def async_case():
        async with EventObserver(**configuration).watch(source="events", decoder=JsonDecoder()) as watcher:
            adapter.publish(b"wire")
            first = await watcher.wait_for(lambda _: True, timeout=1)
            second = await watcher.wait_for(lambda _: True, timeout=1)
            return first, second

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events", decoder=JsonDecoder()) as watcher:
            adapter.publish(b"wire")
            first = watcher.wait_for(lambda _: True, timeout=1)
            second = watcher.wait_for(lambda _: True, timeout=1)
            return first, second

    results = sync_case() if sync else asyncio.run(async_case())
    assert results == ({"id": "42", "secret": "SECRET_PAYLOAD"},) * 2
    assert len(reporter.reports) == 2
    for report in reporter.reports:
        [message] = matched_content(report)
        assert message["payload_omitted"]
        assert message["redaction_error"] == "redaction_failed"
        assert "payload" not in message
        assert "SECRET" not in report.to_json() + "".join(report.attachments.values())


@pytest.mark.parametrize("sync", [False, True])
def test_count_timeout_reports_partial_matched_payloads(sync):
    reporter = MatchedContentReporter()
    adapter = FakeTransport()
    configuration = dict(
        transport=adapter,
        reporters=[reporter],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY, disclosure=DisclosurePolicy.FULL_PAYLOAD),
    )

    async def async_case():
        async with EventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"first")
            adapter.publish(b"second")
            await watcher.wait_for_count(lambda _: True, 2, timeout=1)
            reporter.reports.clear()
            with pytest.raises(EventTimeoutError) as caught:
                await watcher.wait_for_count(lambda _: True, 3, timeout=0.01)
            return caught.value

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"first")
            adapter.publish(b"second")
            watcher.wait_for_count(lambda _: True, 2, timeout=1)
            reporter.reports.clear()
            with pytest.raises(EventTimeoutError) as caught:
                watcher.wait_for_count(lambda _: True, 3, timeout=0.01)
            return caught.value

    error = sync_case() if sync else asyncio.run(async_case())
    summary = error.diagnostics.expectations[-1]
    assert summary.outcome == "timeout"
    assert summary.matched == 2
    assert summary.requested_count == 3
    assert [sample["event_sequence"] for sample in summary.matched_samples] == [0, 1]
    assert len(reporter.reports) == 1
    messages = matched_content(reporter.reports[0])
    assert [message["payload"] for message in messages] == ["first", "second"]
    assert {message["match_status"] for message in messages} == {"matched"}
    assert {message["expectation_id"] for message in messages} == {summary.expectation_id}


@pytest.mark.asyncio
async def test_cancelled_count_retains_partial_match_content_and_releases_results():
    reporter = MatchedContentReporter()
    adapter = FakeTransport()
    async with EventObserver(
        transport=adapter,
        reporters=[reporter],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY, disclosure=DisclosurePolicy.FULL_PAYLOAD),
    ).watch(source="events") as watcher:
        waiting = asyncio.create_task(watcher.wait_for_count(lambda _: True, 2, timeout=5))
        adapter.publish(b"partial")
        async with asyncio.timeout(1):
            while watcher._result_bytes == 0:
                await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        summary = watcher.diagnostics.snapshot().expectations[-1]
        assert summary.outcome == "cancelled"
        assert summary.matched == 1
        assert summary.matched_samples[0]["event_sequence"] == 0
        assert watcher._result_bytes == 0
        assert not watcher._waiter_tasks
        assert len(reporter.reports) == 1
        [message] = matched_content(reporter.reports[0])
        assert message["payload"] == "partial"
        assert message["match_status"] == "matched"
        assert await watcher.wait_for(lambda _: True, timeout=1) == b"partial"
    assert len(reporter.reports) == 2


@pytest.mark.parametrize("sync", [False, True])
def test_result_budget_rejection_does_not_capture_a_match(sync):
    from event_observer import RuntimeLimits
    from event_observer.errors import ExpectationResourceLimitError

    reporter = MatchedContentReporter()
    adapter = FakeTransport()
    configuration = dict(
        transport=adapter,
        reporters=[reporter],
        runtime_limits=RuntimeLimits(max_result_bytes=1),
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY, disclosure=DisclosurePolicy.FULL_PAYLOAD),
    )

    async def async_case():
        async with EventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"cannot-reserve")
            with pytest.raises(ExpectationResourceLimitError):
                await watcher.wait_for(lambda _: True, timeout=1)
            return watcher.diagnostics.snapshot()

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"cannot-reserve")
            with pytest.raises(ExpectationResourceLimitError):
                watcher.wait_for(lambda _: True, timeout=1)
            return watcher.diagnostics.snapshot()

    snapshot = sync_case() if sync else asyncio.run(async_case())
    assert snapshot.expectations[-1].matched == 0
    assert not getattr(snapshot.expectations[-1], "matched_samples", ())
    assert len(reporter.reports) == 1
    assert "message-content.json" not in reporter.reports[0].attachments


@pytest.mark.parametrize("sync", [False, True])
def test_custom_payload_access_and_clock_also_apply_to_opted_in_diagnostics(sync):
    from datetime import datetime, timezone

    class OpaquePayload:
        def __init__(self, values):
            self.values = values

        def __deepcopy__(self, memo):
            raise AssertionError("This payload requires its supplied PayloadAccess")

    class Decoder:
        def __init__(self):
            self.value = OpaquePayload(["SECRET"])

        def decode(self, raw):
            return self.value

    class Access:
        def clone_for_user(self, value):
            return OpaquePayload(list(value.values))

        def estimate_bytes(self, value):
            return 128

    class FixedClock:
        def monotonic(self):
            return 100.0

        def utcnow(self):
            return datetime(2026, 11, 2, tzinfo=timezone.utc)

    redacted = []

    def redact(payload):
        redacted.append(payload)
        payload.values[:] = ["[REDACTED]"]
        return {"values": payload.values}

    transport = FakeTransport()
    reporter = CollectingReporter()
    decoder = Decoder()
    factory = SyncEventObserver if sync else EventObserver
    events = factory(
        transport=transport,
        clock=FixedClock(),
        reporters=[reporter],
        diagnostics=DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            disclosure=DisclosurePolicy.REDACTED_PAYLOAD,
            redactor=redact,
        ),
    )

    async def async_case():
        async with events.watch(source="events", decoder=decoder, payload_access=Access()) as watcher:
            mark = watcher.mark()
            transport.publish(b"wire-value")
            result = await watcher.wait_for(lambda _: True, since=mark, timeout=1)
            assert result.values == ["SECRET"]
            return watcher.diagnostics.snapshot()

    def sync_case():
        with events.watch(source="events", decoder=decoder, payload_access=Access()) as watcher:
            mark = watcher.mark()
            transport.publish(b"wire-value")
            result = watcher.wait_for(lambda _: True, since=mark, timeout=1)
            assert result.values == ["SECRET"]
            return watcher.diagnostics.snapshot()

    snapshot = sync_case() if sync else asyncio.run(async_case())
    assert len(redacted) == 1
    assert redacted[0] is not decoder.value
    assert decoder.value.values == ["SECRET"]
    [summary] = snapshot.expectations
    assert summary.matched_samples[0]["content"]["payload"]["values"] == ("[REDACTED]",)
    assert all(record.recorded_monotonic == 100 for record in snapshot.records)
    assert all(record.recorded_at == "2026-11-02T00:00:00+00:00" for record in snapshot.records)
    assert "SECRET" not in json.dumps([report for _, report in reporter.calls])
