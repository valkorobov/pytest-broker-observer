import json

import pytest

from event_observer.diagnostics import (
    DiagnosticCollector,
    DiagnosticLimits,
    DiagnosticPolicy,
    DisclosurePolicy,
    ReportingMode,
)
from event_observer.errors import DiagnosticConfigurationError, DiagnosticExportError
from event_observer.trace import TraceContextStatus, extract_trace_context


PARENT = b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def test_default_diagnostics_omit_sensitive_content():
    diagnostics = DiagnosticCollector("watcher-1")
    diagnostics.record(
        "failure",
        error_type="InvalidPayloadError",
        status="failed",
        payload="SECRET_PAYLOAD",
        key="SECRET_KEY",
        headers={"secret": "SECRET_HEADER"},
        expected="SECRET_EXPECTED",
        actual="SECRET_ACTUAL",
        description="SECRET_DESCRIPTION",
        source="SECRET_TOPIC",
        cause="SECRET_CAUSE",
        credentials="SECRET_PASSWORD",
        trace_id="SECRET_TRACE",
    )
    exported = diagnostics.export().to_json()
    assert "InvalidPayloadError" in exported
    assert "SECRET" not in exported
    assert diagnostics.disclose_payload("SECRET") == {}


def test_diagnostic_records_and_snapshots_are_immutable():
    diagnostics = DiagnosticCollector("watcher-1")
    diagnostics.record("ready", count=1)
    snapshot = diagnostics.snapshot()
    with pytest.raises(TypeError):
        snapshot.records[0].attributes["count"] = 2
    diagnostics.record("closed", count=2)
    assert len(snapshot.records) == 1


def test_generated_ids_starting_with_digits_survive_sanitization():
    diagnostics = DiagnosticCollector("1234observer")
    diagnostics.record_expectation(scope_id="01234abc", expectation_id="98765fed", outcome="matched")
    summary = diagnostics.snapshot().expectations[0]
    assert summary.scope_id == "01234abc"
    assert summary.expectation_id == "98765fed"


def test_json_truncation_keeps_valid_json_and_counts():
    diagnostics = DiagnosticCollector(
        "watcher-1",
        DiagnosticPolicy(limits=DiagnosticLimits(max_records=4, max_attachment_bytes=400)),
    )
    for _ in range(20):
        diagnostics.record("ready", count=1)
    exported = diagnostics.export()
    data = json.loads(exported.to_json())
    assert exported.truncated
    assert len(exported.to_json().encode()) <= 400
    assert data["omitted_records"] > 0
    assert diagnostics.snapshot().omitted_records == 16


def test_total_export_budget_is_enforced_across_calls():
    diagnostics = DiagnosticCollector(
        "watcher-1",
        DiagnosticPolicy(limits=DiagnosticLimits(max_attachment_bytes=256, max_export_bytes=128)),
    )
    report = diagnostics.export()
    assert not report.attachments
    assert report.truncated
    assert report.to_json().startswith('{\n  "schema_version": 1,\n')
    assert len(report.to_json().encode()) <= 128
    with pytest.raises(DiagnosticExportError):
        diagnostics.export()


def test_redactor_failures_do_not_expose_original_payload():
    def fail(_):
        raise ValueError("SECRET_EXCEPTION")

    diagnostics = DiagnosticCollector(
        "watcher-1",
        DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD, redactor=fail),
    )
    result = diagnostics.disclose_payload({"secret": "SECRET_PAYLOAD"})
    assert result == {"payload_omitted": True, "redaction_error": "redaction_failed"}
    assert "SECRET" not in json.dumps(result)


def test_redactor_runs_on_detached_copy_and_size_is_bounded():
    def redact(payload):
        payload["secret"] = "[REDACTED]"
        return payload

    original = {"secret": "SECRET_PAYLOAD"}
    diagnostics = DiagnosticCollector(
        "watcher-1",
        DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD, redactor=redact),
    )
    assert diagnostics.disclose_payload(original) == {"payload": {"secret": "[REDACTED]"}}
    assert original == {"secret": "SECRET_PAYLOAD"}


def test_redacted_disclosure_requires_a_redactor():
    with pytest.raises(DiagnosticConfigurationError):
        DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD)


def test_reporter_failure_does_not_escape():
    class BrokenReporter:
        def emit(self, report):
            raise RuntimeError("SECRET")

    diagnostics = DiagnosticCollector("watcher-1")
    diagnostics.record("terminal_failure", error_code="transport_error")
    diagnostics.emit([BrokenReporter()])
    snapshot = diagnostics.snapshot().to_json()
    assert "reporter_failed" in snapshot
    assert "SECRET" not in snapshot


def test_errors_only_reporter_mode_does_not_emit_success_or_repeat_errors():
    reports = []

    class Reporter:
        def emit(self, report):
            reports.append(report)

    diagnostics = DiagnosticCollector("watcher-1")
    diagnostics.record_expectation(outcome="matched", matched=1)
    diagnostics.emit([Reporter()])
    assert not reports
    diagnostics.record_expectation(outcome="timeout", matched=0)
    diagnostics.emit([Reporter()])
    diagnostics.emit([Reporter()])
    assert len(reports) == 1


def test_high_frequency_records_are_aggregated_without_erasing_lifecycle():
    diagnostics = DiagnosticCollector("watcher-1", DiagnosticPolicy(limits=DiagnosticLimits(max_records=2)))
    diagnostics.record("ready")
    for _ in range(100):
        diagnostics.record("admission", retained=True)
    snapshot = diagnostics.snapshot()
    assert [record.kind for record in snapshot.records] == ["ready"]
    assert snapshot.counters["received"] == 100


def test_trace_ids_require_explicit_export_opt_in():
    diagnostics = DiagnosticCollector("watcher-1", DiagnosticPolicy(export_trace_ids=True))
    diagnostics.record("ready", trace_id="0af7651916cd43dd8448eb211c80319c")
    assert "0af7651916cd43dd8448eb211c80319c" in diagnostics.export().to_json()


def test_summary_mode_includes_success():
    reports = []

    class Reporter:
        def emit(self, report):
            reports.append(report)

    diagnostics = DiagnosticCollector("watcher-1", DiagnosticPolicy(mode=ReportingMode.SUMMARY))
    diagnostics.record_expectation(outcome="matched", matched=1)
    diagnostics.emit([Reporter()])
    assert len(reports) == 1


@pytest.mark.parametrize("mode", [ReportingMode.SUMMARY, ReportingMode.VERBOSE])
def test_automatic_reporting_waits_for_a_result_and_skips_normal_close(mode):
    diagnostics = DiagnosticCollector("watcher-1", DiagnosticPolicy(mode=mode))
    diagnostics.record("ready")
    assert diagnostics.prepare_report() is None
    assert diagnostics._exported_bytes == 0

    diagnostics.record_expectation(expectation_id="first", outcome="matched", matched=1)
    report = diagnostics.prepare_report()
    assert report is not None
    assert [item["expectation_id"] for item in report.to_dict()["expectations"]] == ["first"]
    assert [record["kind"] for record in report.to_dict()["records"]] == ["ready"]
    spent = diagnostics._exported_bytes

    diagnostics.record("admission", retained=True)
    diagnostics.record("closed", cleanup_state="complete")
    assert diagnostics.prepare_report() is None
    assert diagnostics.prepare_report() is None
    assert diagnostics._exported_bytes == spent


def test_automatic_reports_contain_only_new_results_but_manual_exports_keep_history():
    diagnostics = DiagnosticCollector("watcher-1", DiagnosticPolicy(mode=ReportingMode.SUMMARY))
    diagnostics.record_expectation(expectation_id="first", outcome="matched", matched=1)
    assert diagnostics.prepare_report() is not None
    diagnostics.record_expectation(expectation_id="second", outcome="matched", matched=1)

    manual = diagnostics.export().to_dict()
    assert [item["expectation_id"] for item in manual["expectations"]] == ["first", "second"]
    automatic = diagnostics.prepare_report()
    assert automatic is not None
    assert [item["expectation_id"] for item in automatic.to_dict()["expectations"]] == ["second"]
    assert "Expectation ID: first" not in automatic.attachments["expectation-result.txt"]
    assert diagnostics.prepare_report() is None
    assert [item["expectation_id"] for item in diagnostics.export().to_dict()["expectations"]] == [
        "first",
        "second",
    ]


def test_identical_recorded_results_are_distinct_completed_waits():
    diagnostics = DiagnosticCollector(
        "watcher-1",
        DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            limits=DiagnosticLimits(max_expectation_summaries=1),
        ),
    )
    for _ in range(3):
        # Internal generations must work even when IDs are absent or repeated.
        diagnostics.record_expectation(outcome="matched", matched=1)
        report = diagnostics.prepare_report()
        assert report is not None
        assert report.to_dict()["expectations"] == [{"outcome": "matched", "matched": 1}]
        assert report.to_dict()["omitted_expectations"] == 0
        assert not report.truncated
        assert diagnostics.prepare_report() is None
    assert diagnostics.snapshot().omitted_expectations == 2
    assert diagnostics.export().truncated


def test_automatic_report_retention_omissions_apply_to_the_pending_window():
    diagnostics = DiagnosticCollector(
        "watcher-1",
        DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            limits=DiagnosticLimits(max_records=1, max_expectation_summaries=1),
        ),
    )
    for number in range(3):
        diagnostics.record("ready", count=number)
        diagnostics.record_expectation(expectation_id=f"wait_{number}", outcome="matched")
    report = diagnostics.prepare_report()
    assert report is not None
    assert report.to_dict()["omitted_expectations"] == 2
    assert report.to_dict()["omitted_records"] == 2
    assert report.truncated

    diagnostics.record("closed", cleanup_state="complete")
    diagnostics.record_expectation(expectation_id="next", outcome="matched")
    later = diagnostics.prepare_report()
    assert later is not None
    assert later.to_dict()["omitted_expectations"] == 0
    assert later.to_dict()["omitted_records"] == 0
    assert not later.truncated
    assert diagnostics.snapshot().omitted_expectations == 3
    assert diagnostics.snapshot().omitted_records == 3


def test_errors_only_preparation_preserves_unreported_success_until_a_failure():
    diagnostics = DiagnosticCollector("watcher-1")
    diagnostics.record_expectation(expectation_id="first", outcome="matched")
    assert diagnostics.prepare_report() is None
    assert diagnostics._exported_bytes == 0
    diagnostics.record_expectation(expectation_id="failure", outcome="timeout")
    report = diagnostics.prepare_report()
    assert report is not None
    assert [item["expectation_id"] for item in report.to_dict()["expectations"]] == ["first", "failure"]
    assert diagnostics.prepare_report() is None

    diagnostics.record_expectation(expectation_id="later", outcome="matched")
    assert diagnostics.prepare_report() is None
    diagnostics.record("context_failure", error_type="ValueError")
    later = diagnostics.prepare_report()
    assert later is not None
    assert [item["expectation_id"] for item in later.to_dict()["expectations"]] == ["later"]
    assert diagnostics.prepare_report() is None


@pytest.mark.parametrize(
    "kind", ["startup_failure", "context_failure", "cleanup_failure", "terminal_failure"]
)
def test_new_lifecycle_failure_does_not_repeat_an_already_reported_result(kind):
    diagnostics = DiagnosticCollector("watcher-1", DiagnosticPolicy(mode=ReportingMode.SUMMARY))
    diagnostics.record_expectation(expectation_id="success", outcome="matched")
    assert diagnostics.prepare_report() is not None
    diagnostics.record(kind, error_code="transport_connection", error_type="TransportError")
    report = diagnostics.prepare_report()
    assert report is not None
    assert report.to_dict()["expectations"] == []
    assert [record["kind"] for record in report.to_dict()["records"]] == [kind]
    assert "observer-summary.json" in report.attachments
    assert "transport-state.json" in report.attachments
    assert "expectation-result.json" not in report.attachments
    assert "expectation-result.txt" not in report.attachments
    assert diagnostics.prepare_report() is None


def test_forced_report_can_export_pending_lifecycle_without_repeating_results():
    diagnostics = DiagnosticCollector("watcher-1", DiagnosticPolicy(mode=ReportingMode.SUMMARY))
    diagnostics.record_expectation(expectation_id="success", outcome="matched")
    assert diagnostics.prepare_report() is not None
    diagnostics.record("closed", cleanup_state="complete")
    report = diagnostics.prepare_report(force=True)
    assert report is not None
    assert report.to_dict()["expectations"] == []
    assert [record["kind"] for record in report.to_dict()["records"]] == ["closed"]
    assert diagnostics.prepare_report() is None


def test_failed_automatic_export_leaves_pending_results_available(monkeypatch):
    diagnostics = DiagnosticCollector("watcher-1", DiagnosticPolicy(mode=ReportingMode.SUMMARY))
    diagnostics.record_expectation(expectation_id="pending", outcome="matched")
    export = diagnostics._export

    def fail(_):
        raise DiagnosticExportError("Temporary export failure")

    monkeypatch.setattr(diagnostics, "_export", fail)
    with pytest.raises(DiagnosticExportError):
        diagnostics.prepare_report()
    monkeypatch.setattr(diagnostics, "_export", export)
    report = diagnostics.prepare_report()
    assert report is not None
    assert [item["expectation_id"] for item in report.to_dict()["expectations"]] == ["pending"]
    assert diagnostics.prepare_report() is None


def test_automatic_result_cursors_do_not_depend_on_exported_identifiers():
    diagnostics = DiagnosticCollector(
        "watcher-1",
        DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            limits=DiagnosticLimits(max_attachment_bytes=128, max_export_bytes=4096),
        ),
    )
    diagnostics.record_expectation(expectation_id="omitted_by_budget", outcome="matched")
    report = diagnostics.prepare_report()
    assert report is not None and report.truncated
    assert "omitted_by_budget" not in report.to_json()
    spent = diagnostics._exported_bytes
    diagnostics.record("closed", cleanup_state="complete")
    assert diagnostics.prepare_report() is None
    assert diagnostics._exported_bytes == spent


def test_core_import_and_missing_optional_dependencies_in_clean_process():
    import os
    from pathlib import Path
    import subprocess
    import sys

    program = """
import importlib.abc
import sys
class DenyOptional(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'google', 'allure', 'confluent_kafka', 'pytest', 'opentelemetry'}:
            raise ModuleNotFoundError(fullname)
sys.meta_path.insert(0, DenyOptional())
import event_observer
from event_observer.codecs.protobuf import ProtobufDecoder
from event_observer.integrations.allure import AllureReporter
from event_observer.errors import MissingOptionalDependencyError
assert not any(name.startswith('google.protobuf') for name in sys.modules)
for operation in (lambda: ProtobufDecoder(str), AllureReporter):
    try:
        operation()
    except MissingOptionalDependencyError:
        pass
    else:
        raise AssertionError('missing dependency was not normalized')
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run([sys.executable, "-c", program], env=environment, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_mismatch_sampling_retains_first_and_last_without_values():
    from types import SimpleNamespace

    diagnostics = DiagnosticCollector(
        "watcher-1", DiagnosticPolicy(limits=DiagnosticLimits(max_mismatch_samples=4))
    )
    mismatch = SimpleNamespace(
        path="SECRET_PATH", reason_code="field_not_equal", expected="SECRET_EXPECTED", actual="SECRET_ACTUAL"
    )
    for sequence in range(10):
        diagnostics.record_mismatch("expectation_1", sequence, [mismatch])
    diagnostics.record_expectation(expectation_id="expectation_1", scope_id="scope_1", outcome="timeout")
    summary = diagnostics.snapshot().expectations[0]
    assert summary.scope_id == "scope_1"
    assert [sample["event_sequence"] for sample in summary.samples] == [0, 1, 8, 9]
    assert summary.mismatch_count == 10
    assert summary.mismatch_samples_omitted == 6
    assert "SECRET" not in diagnostics.export().to_json()


def test_allure_emits_completed_steps_only_for_bound_active_test(monkeypatch):
    from contextlib import contextmanager
    import sys
    from types import SimpleNamespace

    steps = []
    attachments = []
    active = ["test-1"]

    @contextmanager
    def step(name):
        steps.append(("start", name))
        yield
        steps.append(("stop", name))

    class Logger:
        def get_test(self, _):
            return SimpleNamespace(uuid=active[0]) if active[0] is not None else None

    manager = SimpleNamespace(get_plugins=lambda: [SimpleNamespace(allure_logger=Logger())])
    monkeypatch.setitem(
        sys.modules,
        "allure",
        SimpleNamespace(
            step=step,
            attach=lambda body, **kwargs: attachments.append((body, kwargs)),
            attachment_type=SimpleNamespace(JSON="json"),
        ),
    )
    monkeypatch.setitem(sys.modules, "allure_commons", SimpleNamespace(plugin_manager=manager))
    from event_observer.integrations.allure import AllureReporter

    reporter = AllureReporter()
    diagnostics = DiagnosticCollector("watcher-1")
    report = diagnostics.export()
    reporter.emit(report)
    assert [kind for kind, _ in steps] == ["start", "stop"]
    assert len(attachments) == 1
    active[0] = "test-2"
    reporter.emit(report)
    assert len(attachments) == 1
    active[0] = None
    reporter.emit(report)
    assert len(attachments) == 1


def test_mismatch_metadata_only_never_inspects_payload():
    from types import SimpleNamespace

    class Uninspectable:
        def __getattribute__(self, name):
            raise AssertionError("payload was inspected")

    diagnostics = DiagnosticCollector("observer_1")
    diagnostics.record_mismatch(
        "expectation_1", 1, [SimpleNamespace(reason_code="predicate_false")], payload=Uninspectable()
    )
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="timeout")
    assert "content" not in diagnostics.snapshot().expectations[0].samples[0]
    assert "message-content.json" not in diagnostics.export().attachments


def test_opted_in_mismatch_capture_redacts_detached_payload():
    original = {"secret": "SECRET", "public": 42}

    def redact(payload):
        payload.pop("secret")
        return payload

    diagnostics = DiagnosticCollector(
        "observer_1", DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD, redactor=redact)
    )
    diagnostics.record_mismatch("expectation_1", 1, (), payload=original)
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="timeout")
    assert original == {"secret": "SECRET", "public": 42}
    summary = diagnostics.snapshot().expectations[0]
    assert summary.samples[0]["content"]["payload"]["public"] == 42
    exported = diagnostics.export()
    assert "message-content.json" in exported.attachments
    assert "SECRET" not in exported.to_json()
    assert "SECRET" not in "".join(exported.attachments.values())


def test_protobuf_disclosure_has_named_attachment_and_redactor_failure_is_closed():
    protobuf = pytest.importorskip("google.protobuf.wrappers_pb2")
    message = protobuf.StringValue(value="VISIBLE_MESSAGE")
    diagnostics = DiagnosticCollector(
        "observer_1", DiagnosticPolicy(disclosure=DisclosurePolicy.FULL_PAYLOAD)
    )
    diagnostics.record_mismatch("expectation_1", 1, (), payload=message)
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="timeout")
    exported = diagnostics.export()
    assert (
        json.loads(exported.attachments["protobuf-message.json"])["messages"][0]["payload"]
        == "VISIBLE_MESSAGE"
    )

    def broken(_):
        raise RuntimeError("SECRET_FAILURE")

    diagnostics = DiagnosticCollector(
        "observer_2", DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD, redactor=broken)
    )
    diagnostics.record_mismatch("expectation_1", 1, (), payload=message)
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="timeout")
    exported = diagnostics.export()
    assert "redaction_failed" in exported.attachments["protobuf-message.json"]
    assert "VISIBLE_MESSAGE" not in exported.to_json()
    assert "SECRET" not in "".join(exported.attachments.values())


def test_attachments_and_content_respect_watcher_budgets():
    policy = DiagnosticPolicy(
        disclosure=DisclosurePolicy.FULL_PAYLOAD,
        limits=DiagnosticLimits(max_attachment_bytes=512, max_export_bytes=1024),
    )
    diagnostics = DiagnosticCollector("observer_1", policy)
    for sequence in range(100):
        diagnostics.record_mismatch("expectation_1", sequence, (), payload={"large": "x" * 200})
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="timeout")
    assert diagnostics.snapshot().truncated
    exported = diagnostics.export()
    assert len(exported.to_json().encode()) <= 512
    assert all(len(value.encode()) <= 512 for value in exported.attachments.values())
    assert (
        len(exported.to_json().encode()) + sum(len(value.encode()) for value in exported.attachments.values())
        <= 1024
    )


def test_export_limit_counts_indentation_when_compact_payload_would_fit():
    diagnostics = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            limits=DiagnosticLimits(max_attachment_bytes=1024, max_export_bytes=8192, max_mismatch_samples=1),
        ),
    )
    payload = {"values": [0] * 80}
    diagnostics.record_mismatch("expectation_1", 1, (), payload=payload)
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="timeout")
    assert len(diagnostics.snapshot().to_json().encode()) < 1024
    assert diagnostics.snapshot().to_dict()["expectations"][0]["samples"][0]["content"]["payload"] == payload

    exported = diagnostics.export()
    assert exported.to_dict()["omitted_expectations"] == 1
    assert exported.to_dict()["expectations"] == []
    assert exported.to_json().startswith('{\n  "schema_version": 1,\n')
    assert exported.truncated
    assert len(exported.to_json().encode()) <= 1024
    assert all(len(body.encode()) <= 1024 for body in exported.attachments.values())


def test_formatted_attachments_count_towards_cumulative_export_budget():
    diagnostics = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            limits=DiagnosticLimits(
                max_attachment_bytes=2048, max_export_bytes=10000, max_mismatch_samples=1
            ),
        ),
    )
    payload = {"title": "all done now 😀", "values": [0] * 10}
    diagnostics.record_mismatch("expectation_1", 1, (), payload=payload)
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="timeout")

    first = diagnostics.export()
    content = first.attachments["message-content.json"]
    assert json.loads(content)["messages"][0]["payload"] == payload
    assert '\n  "messages": [\n    {' in content
    reports = [first]
    for _ in range(16):
        try:
            reports.append(diagnostics.export())
        except DiagnosticExportError:
            break
    else:
        pytest.fail("Repeated exports did not exhaust the byte budget")
    assert len(reports) > 1
    assert any(not report.attachments for report in reports)
    assert all(report.to_json().startswith('{\n  "schema_version": 1,\n') for report in reports)
    assert (
        sum(
            len(report.to_json().encode()) + sum(len(body.encode()) for body in report.attachments.values())
            for report in reports
        )
        <= 10000
    )
    with pytest.raises(DiagnosticExportError):
        diagnostics.export()


def test_truncated_attachment_keeps_indentation_with_a_tight_remaining_budget():
    diagnostics = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            limits=DiagnosticLimits(max_attachment_bytes=2048, max_export_bytes=1500, max_mismatch_samples=1),
        ),
    )
    diagnostics.record_mismatch(
        "expectation_1", 1, (), payload={"title": "all done now 😀", "values": [0] * 10}
    )
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="timeout")

    report = diagnostics.export()
    content = report.attachments["message-content.json"]
    assert json.loads(content) == {"schema_version": 1, "truncated": True, "omitted": True}
    assert content.startswith('{\n  "schema_version": 1,\n')
    assert report.truncated
    assert (
        len(report.to_json().encode()) + sum(len(body.encode()) for body in report.attachments.values())
        <= 1500
    )


def test_ready_boundary_positions_have_explicit_truncation_without_source_labels():
    diagnostics = DiagnosticCollector("observer_1")
    positions = {partition: partition * 10 for partition in range(30)}
    diagnostics.record(
        "ready",
        requested_start_mode="offset",
        requested_offsets=positions,
        resolved_positions=positions,
        ready_monotonic=1.25,
        source="SECRET_TOPIC",
    )
    record = diagnostics.snapshot().records[0]
    assert record.attributes["requested_start_mode"] == "offset"
    assert len(record.attributes["resolved_positions"]) == 20
    assert record.attributes["resolved_positions_omitted"] == 10
    assert record.attributes["requested_offsets_omitted"] == 10
    assert diagnostics.snapshot().truncated
    assert "SECRET" not in diagnostics.export().to_json()


def test_allure_binding_from_reusable_prototype_targets_each_test(monkeypatch):
    from contextlib import nullcontext
    import sys
    from types import SimpleNamespace

    active = ["test-1"]
    attachments = []

    class Logger:
        def get_test(self, _):
            return SimpleNamespace(uuid=active[0])

    monkeypatch.setitem(
        sys.modules,
        "allure",
        SimpleNamespace(
            step=lambda _: nullcontext(),
            attach=lambda body, **kwargs: attachments.append((active[0], kwargs["name"])),
            attachment_type=SimpleNamespace(JSON="json"),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "allure_commons",
        SimpleNamespace(
            plugin_manager=SimpleNamespace(get_plugins=lambda: [SimpleNamespace(allure_logger=Logger())])
        ),
    )
    from event_observer.integrations.allure import AllureReporter

    prototype = AllureReporter()
    first = prototype.bind()
    collector = DiagnosticCollector("observer_1")
    collector.record("ready")
    report = collector.export()
    first.emit(report)
    active[0] = "test-2"
    second = prototype.bind()
    first.emit(report)
    second.emit(report)
    assert attachments == [("test-1", "event-timeline.json"), ("test-2", "event-timeline.json")]


def test_named_failure_attachments_match_conditions_without_sensitive_values():
    from types import SimpleNamespace

    diagnostics = DiagnosticCollector("observer_1")
    diagnostics.record("ready")
    diagnostics.record_mismatch(
        "expectation_1", 1, [SimpleNamespace(reason_code="field_not_equal", expected="SECRET")]
    )
    diagnostics.record_expectation(expectation_id="expectation_1", outcome="error")
    diagnostics.record("terminal_failure", error_code="transport_connection", exception="SECRET")
    exported = diagnostics.export()
    assert set(exported.attachments) == {
        "observer-summary.json",
        "expectation-result.json",
        "expectation-result.txt",
        "mismatches.json",
        "event-timeline.json",
        "transport-state.json",
    }
    for name, body in exported.attachments.items():
        if name.endswith(".json"):
            assert body.startswith('{\n  "schema_version": 1,\n')
            assert json.loads(body)["schema_version"] == 1
        assert "SECRET" not in body


def test_trace_valid_missing_ambiguous_and_zero_ids():
    valid = extract_trace_context((("traceparent", PARENT),))
    assert valid.status is TraceContextStatus.VALID
    assert valid.trace_id == "0af7651916cd43dd8448eb211c80319c"
    assert valid.span_id == "b7ad6b7169203331"
    assert extract_trace_context(()).status is TraceContextStatus.MISSING
    assert (
        extract_trace_context((("traceparent", PARENT), ("TraceParent", PARENT))).status
        is TraceContextStatus.AMBIGUOUS
    )
    zeros = b"00-00000000000000000000000000000000-b7ad6b7169203331-01"
    assert extract_trace_context((("traceparent", zeros),)).status is TraceContextStatus.INVALID


@pytest.mark.parametrize(
    "parent",
    [
        b"ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        PARENT + b"-extra",
        PARENT.upper(),
        PARENT[:-1],
        b"\xff",
        None,
    ],
)
def test_invalid_trace_context_does_not_raise(parent):
    assert extract_trace_context((("traceparent", parent),)).status is TraceContextStatus.INVALID


def test_future_trace_version_can_have_unknown_fields():
    future = b"01" + PARENT[2:] + b"-future-data"
    result = extract_trace_context((("traceparent", future),))
    assert result.status is TraceContextStatus.VALID


def test_invalid_tracestate_keeps_valid_traceparent():
    result = extract_trace_context(
        (("traceparent", PARENT), ("tracestate", b"foo=value,foo=duplicate")),
        include_tracestate=True,
    )
    assert result.status is TraceContextStatus.VALID
    assert result.trace_id is not None
    assert result.tracestate is None
    assert not result.tracestate_valid


def test_tracestate_is_retained_only_by_opt_in():
    headers = (("traceparent", PARENT), ("tracestate", b"vendor=value"))
    assert extract_trace_context(headers).tracestate is None
    assert extract_trace_context(headers, include_tracestate=True).tracestate == "vendor=value"


def test_collector_records_use_the_injected_clock_domain():
    from datetime import datetime, timezone

    class FixedClock:
        def monotonic(self):
            return -12.5

        def utcnow(self):
            return datetime(2026, 11, 2, 3, 4, 5, tzinfo=timezone.utc)

    collector = DiagnosticCollector("watcher", clock=FixedClock())
    collector.record("ready")
    collector.record_reporter_error()
    records = collector.snapshot().records
    assert [record.recorded_monotonic for record in records] == [-12.5, -12.5]
    assert [record.recorded_at for record in records] == ["2026-11-02T03:04:05+00:00"] * 2


@pytest.mark.parametrize("options", [{"clock": object()}, {"clone_payload": 42}])
def test_collector_dependency_hooks_require_callable_interfaces(options):
    with pytest.raises(DiagnosticConfigurationError):
        DiagnosticCollector("watcher", **options)


def test_mismatch_sample_omissions_mark_all_views_truncated_without_poisoning_later_reports():
    from types import SimpleNamespace

    collector = DiagnosticCollector(
        "watcher",
        DiagnosticPolicy(mode=ReportingMode.SUMMARY, limits=DiagnosticLimits(max_mismatch_samples=3)),
    )
    for sequence in range(10):
        collector.record_mismatch("first", sequence, [SimpleNamespace(reason_code="predicate_false")])
    collector.record_expectation(expectation_id="first", outcome="timeout")
    snapshot = collector.snapshot()
    [summary] = snapshot.expectations
    assert [sample["event_sequence"] for sample in summary.samples] == [0, 1, 9]
    assert summary.mismatch_count == 10
    assert summary.mismatch_samples_omitted == 7
    assert snapshot.truncated
    assert collector.export().truncated
    report = collector.prepare_report()
    assert report is not None and report.truncated
    assert "Omitted mismatch samples: 7" in report.attachments["expectation-result.txt"]
    assert "Diagnostic export truncated: yes" in report.attachments["expectation-result.txt"]

    collector.record_expectation(expectation_id="next", outcome="matched")
    next_report = collector.prepare_report()
    assert next_report is not None and not next_report.truncated
    assert [summary["expectation_id"] for summary in next_report.to_dict()["expectations"]] == ["next"]
    assert "Diagnostic export truncated: no" in next_report.attachments["expectation-result.txt"]
    assert collector.snapshot().truncated


def test_single_slot_sample_buffers_capture_lazily_and_use_independent_capacities():
    captured = []

    def clone(payload):
        captured.append(payload)
        return {"sequence": payload}

    collector = DiagnosticCollector(
        "watcher",
        DiagnosticPolicy(
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            limits=DiagnosticLimits(max_mismatch_samples=1, max_matched_samples=3),
        ),
        clone_payload=clone,
    )
    for sequence in range(5):
        collector.record_mismatch("first", sequence, (), payload=sequence)
    assert captured == [0]
    for sequence in range(5, 10):
        collector.record_match("first", sequence, payload=sequence)
    collector.record_expectation(expectation_id="first", outcome="matched", matched=5)
    [summary] = collector.snapshot().expectations
    assert captured == [0, 5, 6, 7, 8, 9]
    assert [sample["event_sequence"] for sample in summary.samples] == [0]
    assert [sample["event_sequence"] for sample in summary.matched_samples] == [5, 6, 9]
    assert summary.mismatch_samples_omitted == 4
    assert summary.matched_samples_omitted == 2


@pytest.mark.parametrize(
    ("record_count", "expectation_count", "keep_records", "keep_expectations"),
    [(6, 2, 2, 2), (2, 6, 2, 2), (4, 4, 2, 3)],
)
def test_export_preserves_balanced_oldest_first_removal_and_exact_budget(
    record_count, expectation_count, keep_records, keep_expectations
):
    from datetime import datetime, timezone

    class FixedClock:
        def monotonic(self):
            return 10.0

        def utcnow(self):
            return datetime(2026, 11, 2, tzinfo=timezone.utc)

    def populate(collector):
        for index in range(record_count):
            collector.record("ready", count=index)
        for index in range(expectation_count):
            collector.record_expectation(expectation_id=f"expectation_{index}", outcome="matched", matched=1)

    baseline = DiagnosticCollector("watcher", clock=FixedClock())
    populate(baseline)
    expected = baseline.snapshot().to_dict()
    expected["records"] = expected["records"][-keep_records:]
    expected["expectations"] = expected["expectations"][-keep_expectations:]
    expected["omitted_records"] = record_count - keep_records
    expected["omitted_expectations"] = expectation_count - keep_expectations
    expected["truncated"] = True
    budget = len(json.dumps(expected, ensure_ascii=True, allow_nan=False, indent=2).encode())
    collector = DiagnosticCollector(
        "watcher", DiagnosticPolicy(limits=DiagnosticLimits(max_attachment_bytes=budget)), clock=FixedClock()
    )
    populate(collector)
    report = collector.export()
    assert report.to_dict() == expected
    assert len(report.to_json().encode()) == budget
    assert all(len(body.encode()) <= budget for body in report.attachments.values())


def test_export_pruning_does_not_serialize_once_per_discarded_entry(monkeypatch):
    import event_observer.diagnostics as diagnostic_module

    collector = DiagnosticCollector(
        "watcher", DiagnosticPolicy(limits=DiagnosticLimits(max_attachment_bytes=2048))
    )
    for index in range(1000):
        collector.record("ready", count=index)
    for index in range(256):
        collector.record_expectation(expectation_id=f"expectation_{index}", outcome="matched", matched=1)
    original = diagnostic_module._attachment_bytes
    serializations = []

    def serialize(value):
        serializations.append(None)
        return original(value)

    monkeypatch.setattr(diagnostic_module, "_attachment_bytes", serialize)
    report = collector.export()
    # This bounds algorithmic work without a timing assertion or depending on
    # a particular search implementation; per-entry pruning takes over 1000 calls.
    assert len(serializations) < 40
    data = report.to_dict()
    assert data["records"][-1]["sequence"] == 1000
    assert data["expectations"][-1]["expectation_id"] == "expectation_255"
    assert data["omitted_records"] + len(data["records"]) == 1000
    assert data["omitted_expectations"] + len(data["expectations"]) == 256
    assert report.truncated
    assert len(report.to_json().encode()) <= 2048
    assert all(len(body.encode()) <= 2048 for body in report.attachments.values())


def capture_sample(collector, category, expectation_id, sequence):
    if category == "matched":
        collector.record_match(expectation_id, sequence, payload={"sequence": sequence})
    else:
        collector.record_mismatch(expectation_id, sequence, (), payload={"sequence": sequence})


def interleaved_collector(*, max_records=4):
    return DiagnosticCollector(
        "watcher",
        DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            limits=DiagnosticLimits(
                max_records=max_records,
                max_expectation_summaries=1,
                max_mismatch_samples=2,
                max_matched_samples=2,
            ),
        ),
    )


@pytest.mark.parametrize("category", ["matched", "mismatch"])
def test_evicted_inflight_sample_buffer_has_explicit_loss_counts(category):
    collector = interleaved_collector()
    capture_sample(collector, category, "first", 0)
    capture_sample(collector, category, "first", 1)
    capture_sample(collector, category, "second", 2)
    collector.record_expectation(
        expectation_id="first", outcome="matched", matched=2 if category == "matched" else 0
    )
    snapshot = collector.snapshot()
    [summary] = snapshot.expectations
    assert getattr(summary, f"{category}_samples_omitted") == 2
    if category == "mismatch":
        assert summary.mismatch_count == 2
    assert snapshot.truncated
    [loss] = snapshot.records
    assert loss.kind == "sample_buffer_evicted"
    assert loss.expectation_id == "first"
    assert loss.attributes["count"] == 2
    assert loss.attributes["reason_code"] == f"{category}_samples_evicted"
    assert loss.attributes[f"{category}_samples_omitted"] == 2
    report = collector.prepare_report()
    assert report is not None and report.truncated
    assert "Diagnostic export truncated: yes" in report.attachments["expectation-result.txt"]


@pytest.mark.parametrize("category", ["matched", "mismatch"])
def test_inflight_loss_survives_earlier_report_ack_and_buffer_recreation_without_poisoning_fresh_results(
    category,
):
    collector = interleaved_collector()
    capture_sample(collector, category, "first", 0)
    capture_sample(collector, category, "first", 1)
    capture_sample(collector, category, "second", 2)
    before = collector.snapshot()
    collector.record_expectation(
        expectation_id="second", outcome="matched", matched=1 if category == "matched" else 0
    )
    earlier_report = collector.prepare_report()
    assert earlier_report is not None and earlier_report.truncated
    assert earlier_report.to_dict()["records"][0]["expectation_id"] == "first"

    # The first expectation is still active after its original loss record was
    # acknowledged with the second expectation's report, and can match again.
    capture_sample(collector, category, "first", 3)
    collector.record_expectation(
        expectation_id="first", outcome="matched", matched=3 if category == "matched" else 0
    )
    [summary] = collector.snapshot().expectations
    assert getattr(summary, f"{category}_samples_omitted") == 2
    samples = summary.matched_samples if category == "matched" else summary.samples
    assert [sample["event_sequence"] for sample in samples] == [3]
    if category == "mismatch":
        assert summary.mismatch_count == 3
    later_report = collector.prepare_report()
    assert later_report is not None and later_report.truncated
    assert later_report.to_dict()["records"] == []
    assert before.records[0].attributes["state"] == "pending"
    assert collector.snapshot().records[0].attributes["state"] == "accounted"

    # A fresh wait and an explicitly reused collector ID must not inherit a
    # previous wait's accounted losses. Full snapshots still retain that history.
    for expectation_id, sequence in (("fresh", 4), ("first", 5)):
        capture_sample(collector, category, expectation_id, sequence)
        collector.record_expectation(
            expectation_id=expectation_id, outcome="matched", matched=1 if category == "matched" else 0
        )
        report = collector.prepare_report()
        assert report is not None and not report.truncated
        [result] = report.to_dict()["expectations"]
        assert result[f"{category}_samples_omitted"] == 0
    assert collector.snapshot().truncated


@pytest.mark.parametrize("category", ["matched", "mismatch"])
def test_sample_loss_ledger_is_bounded_and_its_own_eviction_remains_explicit(category):
    collector = interleaved_collector(max_records=1)
    for sequence in range(10):
        capture_sample(collector, category, f"wait_{sequence}", sequence)
    snapshot = collector.snapshot()
    assert len(snapshot.records) == 1
    assert snapshot.omitted_records == 8
    assert snapshot.truncated
    collector.record_expectation(
        expectation_id="wait_0", outcome="matched", matched=1 if category == "matched" else 0
    )
    report = collector.prepare_report()
    assert report is not None and report.truncated
    assert report.to_dict()["omitted_records"] == 8
    assert report.to_dict()["records"][0]["expectation_id"] == "wait_8"

    collector.record_expectation(
        expectation_id="wait_9", outcome="matched", matched=1 if category == "matched" else 0
    )
    clean = collector.prepare_report()
    assert clean is not None and not clean.truncated
    assert clean.to_dict()["omitted_records"] == 0


@pytest.mark.parametrize("category", ["matched", "mismatch"])
def test_authoritative_totals_preserve_loss_after_ledger_eviction_and_ack(category):
    collector = interleaved_collector(max_records=1)
    for sequence, expectation_id in enumerate(("first", "second", "third")):
        capture_sample(collector, category, expectation_id, sequence)
    collector.record_expectation(
        expectation_id="third",
        outcome="matched" if category == "matched" else "timeout",
        matched=1 if category == "matched" else 0,
        mismatch_count=1 if category == "mismatch" else 0,
    )
    earlier = collector.prepare_report()
    assert earlier is not None and earlier.truncated
    assert earlier.to_dict()["omitted_records"] == 1
    assert earlier.to_dict()["records"][0]["expectation_id"] == "second"

    # All metadata about the first buffer was both evicted and acknowledged.
    # Only the core's completed evaluation counters can now recover its loss.
    capture_sample(collector, category, "first", 3)
    collector.record_expectation(
        expectation_id="first",
        outcome="matched" if category == "matched" else "timeout",
        matched=2 if category == "matched" else 0,
        mismatch_count=2 if category == "mismatch" else 0,
    )
    [summary] = collector.snapshot().expectations
    assert getattr(summary, f"{category}_samples_omitted") == 1
    if category == "mismatch":
        assert summary.mismatch_count == 2
    report = collector.prepare_report()
    assert report is not None and report.truncated
    assert report.to_dict()["records"] == []
    assert report.to_dict()["omitted_records"] == 0
    samples = summary.matched_samples if category == "matched" else summary.samples
    assert [sample["event_sequence"] for sample in samples] == [3]

    capture_sample(collector, category, "fresh", 4)
    collector.record_expectation(
        expectation_id="fresh",
        outcome="matched" if category == "matched" else "timeout",
        matched=1 if category == "matched" else 0,
        mismatch_count=1 if category == "mismatch" else 0,
    )
    clean = collector.prepare_report()
    assert clean is not None and not clean.truncated
