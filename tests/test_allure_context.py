"""Allure display context stays local to a watcher in both facades."""

import asyncio
from contextlib import contextmanager
import json
import sys
import threading
from types import SimpleNamespace

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.diagnostics import DiagnosticExport, DiagnosticLimits, DiagnosticPolicy, ReportingMode
from event_observer.errors import ObserverCleanupError, ObserverFailedError, TransportError
from event_observer.integrations.allure import AllureReporter
from event_observer.testing import FakeTransport


@pytest.fixture
def allure_capture(monkeypatch):
    steps = []
    stack = []

    @contextmanager
    def step(title):
        captured = SimpleNamespace(title=title, attachments=[], attachment_types={})
        steps.append(captured)
        stack.append(captured)
        try:
            yield
        finally:
            assert stack.pop() is captured

    def attach(body, *, name, attachment_type):
        assert stack, "Attachments must belong to a completed summary step"
        expected_type = "text/plain" if name.endswith(".txt") else "application/json"
        assert attachment_type == expected_type
        stack[-1].attachments.append((name, body))
        stack[-1].attachment_types[name] = attachment_type

    logger = SimpleNamespace(get_test=lambda _: SimpleNamespace(uuid="current-test"))
    manager = SimpleNamespace(get_plugins=lambda: [SimpleNamespace(allure_logger=logger)])
    monkeypatch.setitem(
        sys.modules,
        "allure",
        SimpleNamespace(
            step=step,
            attach=attach,
            attachment_type=SimpleNamespace(JSON="application/json", TEXT="text/plain"),
        ),
    )
    monkeypatch.setitem(sys.modules, "allure_commons", SimpleNamespace(plugin_manager=manager))
    return steps


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("aliased", [False, True])
def test_shared_allure_prototype_keeps_each_watchers_title_and_attachments(allure_capture, sync, aliased):
    sources = ("private.alpha", "private.beta")
    aliases = {"FakeTransport": "memory", sources[0]: "orders", sources[1]: "payments"} if aliased else {}
    adapter = FakeTransport()
    prototype = AllureReporter()
    factory_type = SyncEventObserver if sync else EventObserver
    events = factory_type(
        transport=adapter,
        reporters=[prototype],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY, aliases=aliases),
    )

    def publish():
        for source in sources:
            adapter.publish(
                b"SECRET_PAYLOAD", source=source, key=b"SECRET_KEY", headers=(("token", b"SECRET_HEADER"),)
            )

    async def async_case():
        async with events.watch(source=sources[0]) as first:
            async with events.watch(source=sources[1]) as second:
                publish()
                assert await asyncio.gather(
                    first.wait_for(lambda _: True, timeout=1),
                    second.wait_for(lambda _: True, timeout=1),
                ) == [b"SECRET_PAYLOAD", b"SECRET_PAYLOAD"]
                return first.observer_id, second.observer_id

    def sync_case():
        with events.watch(source=sources[0]) as first:
            with events.watch(source=sources[1]) as second:
                publish()
                assert first.wait_for(lambda _: True, timeout=1) == b"SECRET_PAYLOAD"
                assert second.wait_for(lambda _: True, timeout=1) == b"SECRET_PAYLOAD"
                return first.observer_id, second.observer_id

    observer_ids = sync_case() if sync else asyncio.run(async_case())
    transport = aliases.get("FakeTransport", "FakeTransport")
    titles = {
        observer_id: f"Event observation summary [{transport}: {aliases.get(source, source)}]"
        for observer_id, source in zip(observer_ids, sources, strict=True)
    }
    observed = set()
    text_titles = set()
    assert allure_capture
    for captured in allure_capture:
        for name, body in captured.attachments:
            assert "SECRET" not in body
            assert not any(source in body for source in sources)
            if name == "expectation-result.txt":
                text_titles.add(captured.title)
                assert captured.attachment_types[name] == "text/plain"
            if name == "event-timeline.json":
                data = json.loads(body)
                for record in data["records"]:
                    observer_id = record["observer_id"]
                    observed.add(observer_id)
                    assert captured.title == titles[observer_id]
    assert observed == set(observer_ids)
    assert text_titles == set(titles.values())

    # Binding a watcher must not change the reusable prototype's own title.
    prototype.emit(DiagnosticExport({"schema_version": 1}))
    assert allure_capture[-1].title == "Event observation summary"


def test_legacy_reporter_binding_still_receives_each_watchers_reports():
    bound_reports = []

    class BoundReporter:
        def __init__(self):
            self.reports = []

        def emit(self, report):
            self.reports.append(report.to_dict())

    class LegacyReporter:
        def bind(self):
            bound = BoundReporter()
            bound_reports.append(bound.reports)
            return bound

        def emit(self, report):
            raise AssertionError("Use the watcher-local binding")

    adapter = FakeTransport()
    events = SyncEventObserver(
        transport=adapter,
        reporters=[LegacyReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )
    ids = []
    for source in ("first", "second"):
        with events.watch(source=source) as watcher:
            ids.append(watcher.observer_id)
            adapter.publish(b"ok", source=source)
            assert watcher.wait_for(lambda _: True, timeout=1) == b"ok"
    assert len(bound_reports) == 2
    for reports, observer_id in zip(bound_reports, ids, strict=True):
        assert reports
        assert {report["observer_id"] for report in reports} == {observer_id}


def test_standalone_reporter_keeps_generic_title_and_pretty_fallback(allure_capture):
    AllureReporter().emit(DiagnosticExport({"schema_version": 1, "counters": {"received": 2}}))
    assert len(allure_capture) == 1
    assert allure_capture[0].title == "Event observation summary"
    [(name, body)] = allure_capture[0].attachments
    assert name == "observer-summary.json"
    assert body.startswith('{\n  "schema_version": 1,\n')
    assert '\n    "received": 2\n' in body
    assert json.loads(body)["counters"]["received"] == 2


def test_allure_attaches_prepared_text_and_json_without_changing_bodies(allure_capture):
    attachments = {
        "expectation-result.json": '{\n  "schema_version": 1\n}',
        "expectation-result.txt": "Expectation result: matched\nMatched 1 of 1 required events.\n",
    }
    AllureReporter().emit(DiagnosticExport({"schema_version": 1}, attachments=attachments))
    [step] = allure_capture
    assert dict(step.attachments) == attachments
    assert step.attachment_types == {
        "expectation-result.json": "application/json",
        "expectation-result.txt": "text/plain",
    }


@pytest.mark.parametrize("sync", [False, True])
def test_startup_failure_keeps_transport_and_source_in_summary(allure_capture, sync):
    class UnavailableTransport(FakeTransport):
        async def open(self, source, start, *, context):
            raise TransportError("SECRET_TRANSPORT_ERROR")

        def sync_adapter(self):
            adapter = super().sync_adapter()

            def open(source, start, *, context):
                raise TransportError("SECRET_TRANSPORT_ERROR")

            adapter.open = open
            return adapter

    factory_type = SyncEventObserver if sync else EventObserver
    events = factory_type(
        transport=UnavailableTransport(),
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )

    async def async_case():
        async with events.watch(source="unavailable.stream"):
            pytest.fail("An unavailable transport must fail before entering the test body")

    def sync_case():
        with events.watch(source="unavailable.stream"):
            pytest.fail("An unavailable transport must fail before entering the test body")

    with pytest.raises(TransportError):
        sync_case() if sync else asyncio.run(async_case())
    assert allure_capture
    assert {step.title for step in allure_capture} == {
        "Event observation summary [UnavailableTransport: unavailable.stream]"
    }
    assert not any("SECRET" in body for step in allure_capture for _, body in step.attachments)


def test_display_labels_are_bounded_and_single_line(allure_capture):
    reporter = AllureReporter().bind_observation(
        transport="KafkaAdapter\n\t", source="events\n\t" + "x" * 10_000
    )
    reporter.emit(DiagnosticExport({"schema_version": 1}))
    [step] = allure_capture
    assert step.title.startswith("Event observation summary [KafkaAdapter: events ")
    assert "\n" not in step.title and "\t" not in step.title
    assert len(step.title) < 1100


def _reported_expectations(step):
    body = dict(step.attachments).get("expectation-result.json")
    return json.loads(body)["expectations"] if body is not None else []


@pytest.mark.parametrize("sync", [False, True])
def test_one_successful_wait_creates_one_allure_step_even_after_close(allure_capture, sync):
    adapter = FakeTransport()
    factory_type = SyncEventObserver if sync else EventObserver
    events = factory_type(
        transport=adapter,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )
    predicate_calls = []

    def matches(payload):
        predicate_calls.append(payload)
        return payload == b"completed"

    async def async_case():
        async with events.watch(source="operations") as watcher:
            assert allure_capture == []
            adapter.publish(b"completed", source="operations")
            assert await watcher.wait_for(matches, timeout=1) == b"completed"
            assert len(allure_capture) == 1
        await watcher.close()

    def sync_case():
        with events.watch(source="operations") as watcher:
            assert allure_capture == []
            adapter.publish(b"completed", source="operations")
            assert watcher.wait_for(matches, timeout=1) == b"completed"
            assert len(allure_capture) == 1
        watcher.close()

    sync_case() if sync else asyncio.run(async_case())
    assert predicate_calls == [b"completed"]
    [step] = allure_capture
    assert step.title == "Event observation summary [FakeTransport: operations]"
    assert [summary["outcome"] for summary in _reported_expectations(step)] == ["matched"]
    assert dict(step.attachments)["expectation-result.txt"].count("Outcome: MATCHED") == 1


@pytest.mark.parametrize("sync", [False, True])
def test_repeated_waits_have_distinct_reports_and_manual_exports_keep_full_history(allure_capture, sync):
    adapter = FakeTransport()
    factory_type = SyncEventObserver if sync else EventObserver
    events = factory_type(
        transport=adapter,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )
    predicate_calls = []

    def matches(payload):
        predicate_calls.append(payload)
        return payload == b"completed"

    async def async_case():
        async with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            for _ in range(2):
                assert await watcher.wait_for(matches, timeout=1) == b"completed"
            before = watcher.diagnostics.export().to_dict()
            snapshot = watcher.diagnostics.snapshot()
        await watcher.close()
        return before, watcher.diagnostics.export().to_dict(), snapshot

    def sync_case():
        with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            for _ in range(2):
                assert watcher.wait_for(matches, timeout=1) == b"completed"
            before = watcher.diagnostics.export().to_dict()
            snapshot = watcher.diagnostics.snapshot()
        watcher.close()
        return before, watcher.diagnostics.export().to_dict(), snapshot

    before, after, snapshot = sync_case() if sync else asyncio.run(async_case())
    assert predicate_calls == [b"completed", b"completed"]
    assert len(allure_capture) == 2
    summaries = [_reported_expectations(step) for step in allure_capture]
    assert [len(items) for items in summaries] == [1, 1]
    reported_ids = [items[0]["expectation_id"] for items in summaries]
    assert len(set(reported_ids)) == 2
    assert [item["expectation_id"] for item in before["expectations"]] == reported_ids
    assert [item["expectation_id"] for item in after["expectations"]] == reported_ids
    assert [item.expectation_id for item in snapshot.expectations] == reported_ids
    for index, step in enumerate(allure_capture):
        text = dict(step.attachments)["expectation-result.txt"]
        assert text.count("Outcome: MATCHED") == 1
        assert reported_ids[index] in text
        assert reported_ids[1 - index] not in text


@pytest.mark.asyncio
async def test_concurrent_waiters_each_create_one_distinct_allure_result(allure_capture):
    adapter = FakeTransport()
    events = EventObserver(
        transport=adapter,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )
    async with events.watch(source="operations") as watcher:
        assert allure_capture == []
        first = asyncio.create_task(watcher.wait_for(lambda _: True, timeout=1))
        second = asyncio.create_task(watcher.wait_for_event(lambda _: True, timeout=1))
        adapter.publish(b"completed", source="operations")
        payload, event = await asyncio.gather(first, second)
        assert payload == event.payload == b"completed"
    assert len(allure_capture) == 2
    summaries = [_reported_expectations(step) for step in allure_capture]
    assert [len(items) for items in summaries] == [1, 1]
    assert len({items[0]["expectation_id"] for items in summaries}) == 2


def _assert_failure_does_not_repeat_matching_result(steps, kind):
    assert len(steps) == 2
    assert [summary["outcome"] for summary in _reported_expectations(steps[0])] == ["matched"]
    assert _reported_expectations(steps[1]) == []
    attachments = dict(steps[1].attachments)
    assert "expectation-result.txt" not in attachments
    failures = json.loads(attachments["observer-summary.json"])["failures"]
    assert kind in {record["kind"] for record in failures}
    assert not any("SECRET" in body for step in steps for _, body in step.attachments)


@pytest.mark.parametrize("sync", [False, True])
def test_context_failure_after_success_has_its_own_report_without_matching_duplicates(allure_capture, sync):
    adapter = FakeTransport()
    factory_type = SyncEventObserver if sync else EventObserver
    events = factory_type(
        transport=adapter,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )

    async def async_case():
        async with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            assert await watcher.wait_for(lambda _: True, timeout=1) == b"completed"
            raise ValueError("SECRET_BODY_ERROR")

    def sync_case():
        with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            assert watcher.wait_for(lambda _: True, timeout=1) == b"completed"
            raise ValueError("SECRET_BODY_ERROR")

    with pytest.raises(ValueError, match="SECRET_BODY_ERROR"):
        sync_case() if sync else asyncio.run(async_case())
    _assert_failure_does_not_repeat_matching_result(allure_capture, "context_failure")


@pytest.mark.parametrize("sync", [False, True])
def test_cleanup_failure_after_success_is_still_reported_once(allure_capture, sync):
    class FailingCleanupTransport(FakeTransport):
        async def open(self, source, start, *, context):
            session = await super().open(source, start, context=context)
            original_close = session.close

            async def close(*, deadline):
                await original_close(deadline=deadline)
                raise TransportError("SECRET_CLEANUP_ERROR")

            session.close = close
            return session

        def sync_adapter(self):
            adapter = super().sync_adapter()
            original_open = adapter.open

            def open(source, start, *, context):
                session = original_open(source, start, context=context)
                original_close = session.close

                def close(*, deadline):
                    original_close(deadline=deadline)
                    raise TransportError("SECRET_CLEANUP_ERROR")

                session.close = close
                return session

            adapter.open = open
            return adapter

    adapter = FailingCleanupTransport()
    factory_type = SyncEventObserver if sync else EventObserver
    events = factory_type(
        transport=adapter,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )

    async def async_case():
        async with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            assert await watcher.wait_for(lambda _: True, timeout=1) == b"completed"

    def sync_case():
        with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            assert watcher.wait_for(lambda _: True, timeout=1) == b"completed"

    with pytest.raises(ObserverCleanupError):
        sync_case() if sync else asyncio.run(async_case())
    _assert_failure_does_not_repeat_matching_result(allure_capture, "cleanup_failure")
    assert adapter.close_count == 1


@pytest.mark.parametrize("sync", [False, True])
def test_background_failure_after_success_is_reported_on_exit(allure_capture, sync):
    failed = threading.Event()

    class SignallingTransport(FakeTransport):
        async def open(self, source, start, *, context):
            session = await super().open(source, start, context=context)
            self.record_failures(context)
            return session

        def record_failures(self, context):
            original_record = context.diagnostics.record

            def record(kind, **attributes):
                original_record(kind, **attributes)
                if kind == "terminal_failure":
                    failed.set()

            context.diagnostics.record = record

        def sync_adapter(self):
            adapter = super().sync_adapter()
            original_open = adapter.open

            def open(source, start, *, context):
                session = original_open(source, start, context=context)
                self.record_failures(context)
                return session

            adapter.open = open
            return adapter

    adapter = SignallingTransport()
    factory_type = SyncEventObserver if sync else EventObserver
    events = factory_type(
        transport=adapter,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )

    async def async_case():
        async with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            assert await watcher.wait_for(lambda _: True, timeout=1) == b"completed"
            adapter.fail(TransportError("SECRET_BACKGROUND_ERROR"))
            assert await asyncio.to_thread(failed.wait, 1)
            assert len(allure_capture) == 1

    def sync_case():
        with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            assert watcher.wait_for(lambda _: True, timeout=1) == b"completed"
            adapter.fail(TransportError("SECRET_BACKGROUND_ERROR"))
            assert failed.wait(1)
            assert len(allure_capture) == 1

    with pytest.raises(ObserverFailedError):
        sync_case() if sync else asyncio.run(async_case())
    _assert_failure_does_not_repeat_matching_result(allure_capture, "terminal_failure")


def test_sync_context_error_report_has_budget_priority_over_full_export_cache(allure_capture):
    adapter = FakeTransport()
    events = SyncEventObserver(
        transport=adapter,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            limits=DiagnosticLimits(max_export_bytes=6144),
        ),
    )
    with pytest.raises(ValueError, match="SECRET_BODY_ERROR"):
        with events.watch(source="operations") as watcher:
            adapter.publish(b"completed", source="operations")
            assert watcher.wait_for(lambda _: True, timeout=1) == b"completed"
            raise ValueError("SECRET_BODY_ERROR")
    _assert_failure_does_not_repeat_matching_result(allure_capture, "context_failure")


@pytest.mark.parametrize("sync", [False, True])
def test_observation_mark_is_quiet_and_since_summary_shows_only_the_safe_lower_bound(allure_capture, sync):
    import time

    transport = FakeTransport()
    factory = SyncEventObserver if sync else EventObserver
    events = factory(
        transport=transport,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )

    async def async_case():
        async with events.watch(source="events") as watcher:
            transport.publish(b"old")
            async with asyncio.timeout(1):
                while watcher.snapshot().next_sequence < 1:
                    await asyncio.sleep(0)
            mark = watcher.mark()
            assert allure_capture == []
            transport.publish(b"new")
            assert await watcher.wait_for(lambda _: True, since=mark, timeout=1) == b"new"
            return mark

    def sync_case():
        with events.watch(source="events") as watcher:
            transport.publish(b"old")
            deadline = time.monotonic() + 1
            while watcher.snapshot().next_sequence < 1:
                assert time.monotonic() < deadline
            mark = watcher.mark()
            assert allure_capture == []
            transport.publish(b"new")
            assert watcher.wait_for(lambda _: True, since=mark, timeout=1) == b"new"
            return mark

    mark = sync_case() if sync else asyncio.run(async_case())
    [step] = allure_capture
    [summary] = _reported_expectations(step)
    assert summary["lower_bound"] == mark.sequence == 1
    assert "since" not in summary
    assert "recorded_at" not in summary
    assert "recorded_monotonic" not in summary
    attachments = dict(step.attachments)
    assert f"Journal lower bound: {mark.sequence}" in attachments["expectation-result.txt"]
    assert "ObservationMark" not in "".join(attachments.values())
