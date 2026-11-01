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
            return await watcher.wait_for(lambda _: True, timeout=1)

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            adapter.publish(b"ok")
            return watcher.wait_for(lambda _: True, timeout=1)

    assert (sync_case() if sync else asyncio.run(async_case())) == b"ok"


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
            with pytest.raises(EventTimeoutError):
                await watcher.wait_for(lambda _: False, timeout=0.01)
            assert await watcher.wait_for(lambda value: value["secret"] == "SECRET_PAYLOAD", timeout=1)
            return watcher.diagnostics.snapshot()

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events", decoder=JsonDecoder()) as watcher:
            adapter.publish(b"wire")
            with pytest.raises(EventTimeoutError):
                watcher.wait_for(lambda _: False, timeout=0.01)
            assert watcher.wait_for(lambda value: value["secret"] == "SECRET_PAYLOAD", timeout=1)
            return watcher.diagnostics.snapshot()

    snapshot = sync_case() if sync else asyncio.run(async_case())
    sample = snapshot.expectations[0].samples[0]
    assert sample["content"]["payload"] == {"id": "42"}
    assert "SECRET" not in json.dumps([report for _, report in reporter.calls])
