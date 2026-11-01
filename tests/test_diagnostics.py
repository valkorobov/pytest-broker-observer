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
        DiagnosticPolicy(limits=DiagnosticLimits(max_attachment_bytes=256, max_export_bytes=256)),
    )
    diagnostics.export()
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
        "mismatches.json",
        "event-timeline.json",
        "transport-state.json",
    }
    for body in exported.attachments.values():
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
