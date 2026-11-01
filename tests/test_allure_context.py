"""Allure display context stays local to a watcher in both facades."""

import asyncio
from contextlib import contextmanager
import json
import sys
from types import SimpleNamespace

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.diagnostics import DiagnosticExport, DiagnosticPolicy, ReportingMode
from event_observer.errors import TransportError
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
