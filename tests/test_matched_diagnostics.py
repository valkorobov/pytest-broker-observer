"""Opted-in successful event content stays bounded, isolated, and attributable."""

import json
from types import SimpleNamespace

import pytest

from event_observer.diagnostics import (
    DiagnosticCollector,
    DiagnosticLimits,
    DiagnosticPolicy,
    DisclosurePolicy,
    ReportingMode,
)
from event_observer.errors import DiagnosticConfigurationError, DiagnosticExportError


def finish(collector, expectation_id, *, matched=1, outcome="matched"):
    collector.record_expectation(
        expectation_id=expectation_id,
        outcome=outcome,
        matched=matched,
        requested_count=matched,
    )


def content_messages(report, attachment="message-content.json"):
    return json.loads(report.attachments[attachment])["messages"]


def test_matched_sampling_default_preserves_positional_limit_arguments():
    limits = DiagnosticLimits(10, 11, 12, 13, 2048, 8192)
    assert limits.max_matched_samples == 20
    assert limits.max_attachment_bytes == 2048
    assert limits.max_export_bytes == 8192


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_matched_sample_limit_requires_a_positive_integer(limit):
    with pytest.raises(DiagnosticConfigurationError):
        DiagnosticLimits(max_matched_samples=limit)


def test_metadata_only_match_capture_never_inspects_or_retains_payload():
    class Uninspectable:
        def __getattribute__(self, name):
            raise AssertionError("The payload was inspected")

    collector = DiagnosticCollector("observer_1")
    collector.record_match("expectation_1", 1, payload=Uninspectable())
    finish(collector, "expectation_1")
    summary = collector.snapshot().expectations[0]
    assert summary.matched == 1
    assert not getattr(summary, "matched_samples", ())
    exported = collector.export()
    assert "message-content.json" not in exported.attachments
    assert "protobuf-message.json" not in exported.attachments


def test_full_matched_json_is_detached_at_capture_and_has_event_identity():
    payload = {"operation_id": "operation-42", "values": [1]}
    collector = DiagnosticCollector("observer_1", DiagnosticPolicy(disclosure=DisclosurePolicy.FULL_PAYLOAD))
    collector.record_match("expectation_1", 42, payload=payload)
    payload["values"].append(2)
    finish(collector, "expectation_1")
    summary = collector.snapshot().expectations[0]
    assert summary.matched_samples[0]["event_sequence"] == 42
    assert summary.matched_samples[0]["content"]["payload"]["values"] == (1,)
    [message] = content_messages(collector.export())
    assert message["match_status"] == "matched"
    assert message["expectation_id"] == "expectation_1"
    assert message["event_sequence"] == 42
    assert message["payload"] == {"operation_id": "operation-42", "values": [1]}


@pytest.mark.parametrize(
    "payload,expected,encoding",
    [
        (b"completed", "completed", "utf-8"),
        ("Ready 😀".encode(), "Ready 😀", "utf-8"),
        (b"\xff\x00", "/wA=", "base64"),
        (bytearray(b"completed"), "completed", "utf-8"),
        (b"", "", "utf-8"),
        (None, None, None),
    ],
)
def test_full_matched_raw_content_preserves_bytes_encoding_and_tombstones(payload, expected, encoding):
    collector = DiagnosticCollector("observer_1", DiagnosticPolicy(disclosure=DisclosurePolicy.FULL_PAYLOAD))
    collector.record_match("expectation_1", 0, payload=payload)
    finish(collector, "expectation_1")
    [message] = content_messages(collector.export())
    assert message["match_status"] == "matched"
    assert message["payload"] == expected
    assert message.get("payload_encoding") == encoding
    assert not message.get("payload_omitted", False)


def test_redacted_matched_json_never_mutates_canonical_payload():
    payload = {"secret": "SECRET_VALUE", "operation_id": "operation-42"}

    def redact(value):
        value.pop("secret")
        value["redacted"] = True
        return value

    collector = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD, redactor=redact),
    )
    collector.record_match("expectation_1", 1, payload=payload)
    finish(collector, "expectation_1")
    report = collector.export()
    [message] = content_messages(report)
    assert message["payload"] == {"operation_id": "operation-42", "redacted": True}
    assert payload == {"secret": "SECRET_VALUE", "operation_id": "operation-42"}
    assert "SECRET" not in report.to_json() + "".join(report.attachments.values())


def test_redactor_returning_bytes_stays_fail_closed_after_receiving_the_original_bytes():
    calls = []

    def redact(value):
        calls.append(value)
        return b"sanitized"

    collector = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD, redactor=redact),
    )
    collector.record_match("expectation_1", 1, payload=b"SECRET_RAW")
    finish(collector, "expectation_1")
    report = collector.export()
    [message] = content_messages(report)
    assert calls == [b"SECRET_RAW"]
    assert message["payload_omitted"]
    assert message["redaction_error"] == "redaction_failed"
    assert "payload" not in message
    assert "SECRET_RAW" not in report.to_json() + "".join(report.attachments.values())


@pytest.mark.parametrize("sanitized", [{"value": "sanitized"}, "sanitized"])
def test_raw_byte_redactor_can_return_sanitized_json_without_prior_encoding(sanitized):
    original = b"SECRET_RAW"
    calls = []

    def redact(value):
        calls.append(value)
        assert isinstance(value, bytes)
        return sanitized

    collector = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(disclosure=DisclosurePolicy.REDACTED_PAYLOAD, redactor=redact),
    )
    collector.record_match("expectation_1", 1, payload=original)
    finish(collector, "expectation_1")
    report = collector.export()
    [message] = content_messages(report)
    assert calls == [original]
    assert message["payload"] == sanitized
    assert "payload_encoding" not in message
    assert not message.get("payload_omitted", False)
    assert "SECRET_RAW" not in report.to_json() + "".join(report.attachments.values())


@pytest.mark.parametrize("redacted", [False, True])
def test_matched_protobuf_uses_named_attachment_and_detached_redaction(redacted):
    wrappers = pytest.importorskip("google.protobuf.wrappers_pb2")
    payload = wrappers.StringValue(value="PROTO_SECRET")

    def redact(value):
        value.value = "sanitized"
        return {"value": value.value}

    policy = DiagnosticPolicy(
        disclosure=DisclosurePolicy.REDACTED_PAYLOAD if redacted else DisclosurePolicy.FULL_PAYLOAD,
        redactor=redact if redacted else None,
    )
    collector = DiagnosticCollector("observer_1", policy)
    collector.record_match("expectation_1", 7, payload=payload)
    finish(collector, "expectation_1")
    report = collector.export()
    [message] = content_messages(report, "protobuf-message.json")
    assert message["match_status"] == "matched"
    assert message["expectation_id"] == "expectation_1"
    assert message["event_sequence"] == 7
    assert message["payload"] == ({"value": "sanitized"} if redacted else "PROTO_SECRET")
    assert payload.value == "PROTO_SECRET"
    assert "message-content.json" not in report.attachments
    if redacted:
        assert "PROTO_SECRET" not in report.to_json() + "".join(report.attachments.values())


def test_matching_and_mismatching_content_keep_separate_samples_and_statuses():
    collector = DiagnosticCollector("observer_1", DiagnosticPolicy(disclosure=DisclosurePolicy.FULL_PAYLOAD))
    collector.record_mismatch(
        "expectation_1", 10, [SimpleNamespace(reason_code="predicate_false")], payload={"status": "pending"}
    )
    collector.record_match("expectation_1", 11, payload={"status": "completed"})
    finish(collector, "expectation_1")
    summary = collector.snapshot().expectations[0]
    assert summary.samples[0]["event_sequence"] == 10
    assert summary.matched_samples[0]["event_sequence"] == 11
    assert summary.mismatch_count == 1
    report = collector.export()
    messages = content_messages(report)
    assert {(message["match_status"], message["event_sequence"]) for message in messages} == {
        ("mismatch", 10),
        ("matched", 11),
    }
    assert {message["expectation_id"] for message in messages} == {"expectation_1"}
    mismatch_document = json.loads(report.attachments["mismatches.json"])
    assert [sample["event_sequence"] for sample in mismatch_document["expectations"][0]["samples"]] == [10]
    outcome_document = json.loads(report.attachments["expectation-result.json"])
    assert "matched_samples" not in outcome_document["expectations"][0]
    assert "samples" not in outcome_document["expectations"][0]


def test_matched_content_retains_bounded_first_and_last_samples_with_omission_count():
    collector = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            limits=DiagnosticLimits(max_matched_samples=3),
        ),
    )
    for sequence in range(10):
        collector.record_match("expectation_1", sequence, payload={"sequence": sequence})
    finish(collector, "expectation_1", matched=10)
    snapshot = collector.snapshot()
    summary = snapshot.expectations[0]
    assert summary.matched == 10
    assert [sample["event_sequence"] for sample in summary.matched_samples] == [0, 1, 9]
    assert summary.matched_samples_omitted == 7
    assert snapshot.truncated

    exported = collector.export()
    assert [message["event_sequence"] for message in content_messages(exported)] == [0, 1, 9]
    assert exported.truncated
    assert "Diagnostic export truncated: yes" in exported.attachments["expectation-result.txt"]

    automatic = collector.prepare_report()
    assert automatic is not None
    assert automatic.truncated
    text = automatic.attachments["expectation-result.txt"]
    assert "Diagnostic export truncated: yes" in text
    assert "Omitted matched message samples: 7" in text

    collector.record_match("expectation_2", 10, payload={"sequence": 10})
    finish(collector, "expectation_2")
    next_report = collector.prepare_report()
    assert next_report is not None
    assert not next_report.truncated
    [next_summary] = next_report.to_dict()["expectations"]
    assert next_summary["expectation_id"] == "expectation_2"
    assert next_summary["matched_samples_omitted"] == 0
    assert "Diagnostic export truncated: no" in next_report.attachments["expectation-result.txt"]
    # A full retained snapshot still includes the earlier, explicitly omitted samples.
    assert collector.snapshot().truncated


def test_content_capture_budget_is_shared_across_matches_mismatches_and_expectations():
    collector = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            limits=DiagnosticLimits(
                max_attachment_bytes=4096,
                max_export_bytes=1024,
                max_matched_samples=1,
                max_mismatch_samples=1,
            ),
        ),
    )
    payload = {"data": "x" * 420}
    collector.record_match("expectation_1", 1, payload=payload)
    finish(collector, "expectation_1")
    collector.record_mismatch("expectation_2", 2, (), payload=payload)
    finish(collector, "expectation_2", matched=0, outcome="timeout")
    collector.record_match("expectation_3", 3, payload=payload)
    finish(collector, "expectation_3")
    first, second, third = collector.snapshot().expectations
    assert first.matched_samples[0]["content"]["payload"]["data"] == payload["data"]
    assert second.samples[0]["content"]["payload"]["data"] == payload["data"]
    assert third.matched_samples[0]["content"]["payload_omitted"]
    assert third.matched_samples[0]["content"]["reason_code"] == "content_budget_exhausted"
    assert collector.snapshot().truncated


def test_export_budget_does_not_reset_for_later_successful_expectations():
    limit = 8192
    collector = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(
            mode=ReportingMode.SUMMARY,
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            limits=DiagnosticLimits(max_attachment_bytes=2048, max_export_bytes=limit, max_matched_samples=1),
        ),
    )
    reports = []
    for sequence in range(32):
        expectation_id = f"expectation_{sequence}"
        collector.record_match(expectation_id, sequence, payload={"value": "completed"})
        finish(collector, expectation_id)
        try:
            report = collector.prepare_report()
        except DiagnosticExportError:
            break
        assert report is not None
        reports.append(report)
    else:
        pytest.fail("Successful match reports escaped the cumulative export budget")
    assert len(reports) > 1
    assert "message-content.json" in reports[0].attachments
    assert (
        sum(
            len(report.to_json().encode()) + sum(len(body.encode()) for body in report.attachments.values())
            for report in reports
        )
        <= limit
    )
    with pytest.raises(DiagnosticExportError):
        collector.prepare_report()


def test_nested_non_json_content_is_omitted_without_repr_or_raw_fallback():
    class NotJson:
        def __repr__(self):
            raise AssertionError("Content fallback must not call repr")

    collector = DiagnosticCollector("observer_1", DiagnosticPolicy(disclosure=DisclosurePolicy.FULL_PAYLOAD))
    collector.record_match("expectation_1", 1, payload={"secret": "SECRET_VALUE", "invalid": NotJson()})
    finish(collector, "expectation_1")
    report = collector.export()
    [message] = content_messages(report)
    assert message["payload_omitted"]
    assert "payload" not in message
    assert "SECRET_VALUE" not in report.to_json() + "".join(report.attachments.values())


def test_large_matched_content_is_omitted_before_the_expectation_outcome_is_dropped():
    limits = DiagnosticLimits(max_attachment_bytes=1024, max_export_bytes=8192, max_matched_samples=1)
    collector = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(disclosure=DisclosurePolicy.FULL_PAYLOAD, limits=limits),
    )
    payload = {"values": [0] * 80}
    collector.record_match("expectation_1", 1, payload=payload)
    finish(collector, "expectation_1")
    assert (
        collector.snapshot().expectations[0].matched_samples[0]["content"]["payload"]["values"] == (0,) * 80
    )
    report = collector.export()
    data = report.to_dict()
    assert data["omitted_expectations"] == 0
    [summary] = data["expectations"]
    assert summary["outcome"] == "matched"
    assert summary["matched"] == 1
    assert summary["matched_samples"][0]["content"]["payload_omitted"]
    assert report.truncated
    outcome_document = json.loads(report.attachments["expectation-result.json"])
    assert outcome_document["expectations"][0]["outcome"] == "matched"
    [message] = content_messages(report)
    assert message["match_status"] == "matched"
    assert message["payload_omitted"]
    assert len(report.to_json().encode()) <= limits.max_attachment_bytes
    assert all(len(body.encode()) <= limits.max_attachment_bytes for body in report.attachments.values())
    assert (
        len(report.to_json().encode()) + sum(len(body.encode()) for body in report.attachments.values())
        <= limits.max_export_bytes
    )
