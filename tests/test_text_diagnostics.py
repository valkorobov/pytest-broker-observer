"""Readable waiting results preserve the diagnostic contract and byte budgets."""

import json
from types import SimpleNamespace

import pytest

from event_observer.diagnostics import (
    DiagnosticCollector,
    DiagnosticLimits,
    DiagnosticPolicy,
    DisclosurePolicy,
)
from event_observer.diagnostics._text import truncate_text
from event_observer.errors import DiagnosticExportError


def record_wait(diagnostics, **changes):
    summary = dict(
        expectation_id="expectation_1",
        scope_id="scope_1",
        outcome="matched",
        requested_count=1,
        matched=1,
        evaluated=1,
        elapsed=0.25,
        timeout=1,
        deadline=12345.75,
        deadline_overrun=0,
        scope_excluded=0,
        not_applicable=0,
        complete=True,
        require_complete=True,
    )
    summary.update(changes)
    diagnostics.record_expectation(**summary)


def test_successful_wait_has_plain_explanation_and_configured_timeout():
    diagnostics = DiagnosticCollector("observer_1")
    record_wait(diagnostics)
    report = diagnostics.export()
    body = report.attachments["expectation-result.txt"]
    assert "Outcome: MATCHED" in body
    assert "The requested matching event count was reached." in body
    assert "Matching events: 1 / 1 requested" in body
    assert "Elapsed: 0.25 s" in body
    assert "Configured timeout: 1 s" in body
    assert "12345.75" not in body  # Absolute monotonic time is not a timeout duration.
    assert "Observation complete: yes; required: yes" in body
    assert "Diagnostic export truncated: no" in body
    assert list(report.attachments)[-1] == "expectation-result.txt"
    assert json.loads(report.attachments["expectation-result.json"])["expectations"][0]["timeout"] == 1


def test_latest_result_precedes_previous_wait_with_bounded_mismatch_details():
    diagnostics = DiagnosticCollector(
        "observer_1", DiagnosticPolicy(limits=DiagnosticLimits(max_mismatch_samples=1))
    )
    record_wait(diagnostics, expectation_id="earlier")
    for sequence in (41, 42, 43):
        diagnostics.record_mismatch(
            "latest", sequence, [SimpleNamespace(reason_code="field_not_equal", expected="SECRET")]
        )
    record_wait(
        diagnostics,
        expectation_id="latest",
        outcome="timeout",
        requested_count=2,
        matched=1,
        evaluated=4,
        error_code="event_timeout",
        error_type="EventTimeoutError",
    )
    body = diagnostics.export().attachments["expectation-result.txt"]
    assert body.index("Outcome: TIMEOUT") < body.index("Outcome: MATCHED")
    assert body.index("Expectation ID: latest") < body.index("Expectation ID: earlier")
    assert "Only part of the requested event count matched." in body
    assert "Matching events: 1 / 2 requested" in body
    assert "Mismatched events: 3" in body
    assert "Mismatch sample, event 41: field_not_equal" in body
    assert "Omitted mismatch samples: 2" in body
    assert "Error: event_timeout (EventTimeoutError)" in body
    assert "SECRET" not in body


@pytest.mark.parametrize(
    ("fields", "explanation"),
    [
        ({"evaluated": 0}, "No events were evaluated by this expectation."),
        ({"evaluated": 0, "scope_excluded": 2}, "Events were excluded by the scope"),
        ({"evaluated": 3, "not_applicable": 3}, "All evaluated events were not applicable"),
        ({"evaluated": 3}, "Events were evaluated, but none matched"),
        ({"evaluated": None}, "The requested matching event count was not reached."),
    ],
)
def test_timeout_explanation_distinguishes_missing_and_nonmatching_events(fields, explanation):
    diagnostics = DiagnosticCollector("observer_1")
    record_wait(diagnostics, outcome="timeout", matched=0, **fields)
    body = diagnostics.export().attachments["expectation-result.txt"]
    assert "Outcome: TIMEOUT" in body
    assert explanation in body
    assert "No events arrived" not in body


@pytest.mark.parametrize(
    ("outcome", "explanation"),
    [("error", "stopped because an error occurred"), ("cancelled", "cancelled before completion")],
)
def test_error_and_cancelled_waits_do_not_claim_success(outcome, explanation):
    diagnostics = DiagnosticCollector("observer_1")
    record_wait(
        diagnostics,
        outcome=outcome,
        matched=0,
        complete=False,
        require_complete=False,
        error_type="IncompleteObservationError" if outcome == "error" else None,
    )
    body = diagnostics.export().attachments["expectation-result.txt"]
    assert f"Outcome: {outcome.upper()}" in body
    assert explanation in body
    assert "Observation complete: no; required: no" in body
    assert "The requested matching event count was reached." not in body


def test_text_omits_content_and_arbitrary_labels_even_when_json_disclosure_is_enabled():
    diagnostics = DiagnosticCollector(
        "observer_1",
        DiagnosticPolicy(
            disclosure=DisclosurePolicy.FULL_PAYLOAD,
            allowed_fields=("description", "source"),
        ),
    )
    diagnostics.record_mismatch(
        "expectation_1",
        7,
        [SimpleNamespace(reason_code="field_not_equal", expected="SECRET_EXPECTED", actual="SECRET_ACTUAL")],
        payload={"token": "SECRET_PAYLOAD"},
    )
    record_wait(
        diagnostics,
        outcome="timeout",
        matched=0,
        description="SECRET_DESCRIPTION",
        source="SECRET_TOPIC",
        key="SECRET_KEY",
        headers={"token": "SECRET_HEADER"},
        cause="SECRET_CAUSE",
        credentials="SECRET_CREDENTIALS",
    )
    report = diagnostics.export()
    assert "SECRET_PAYLOAD" in report.attachments["message-content.json"]
    assert "SECRET_DESCRIPTION" in report.to_json()
    text = report.attachments["expectation-result.txt"]
    assert "field_not_equal" in text
    assert "SECRET" not in text


def test_no_text_is_created_before_any_expectation_completes():
    diagnostics = DiagnosticCollector("observer_1")
    diagnostics.record("ready")
    assert "expectation-result.txt" not in diagnostics.export().attachments


def test_text_is_truncated_with_a_notice_and_all_files_respect_attachment_limits():
    limits = DiagnosticLimits(max_attachment_bytes=480, max_export_bytes=4096)
    diagnostics = DiagnosticCollector("observer_1", DiagnosticPolicy(limits=limits))
    # A small machine summary fits; the full explanation needs more than 480 bytes.
    diagnostics.record_expectation(outcome="timeout", evaluated=0, matched=0, requested_count=1)
    report = diagnostics.export()
    text = report.attachments["expectation-result.txt"]
    assert "Outcome: TIMEOUT" in text
    assert text.endswith("[Text truncated: diagnostic byte limit reached.]\n")
    assert "Diagnostic export truncated: no" not in text
    assert report.truncated
    assert all(
        len(body.encode("utf-8")) <= limits.max_attachment_bytes for body in report.attachments.values()
    )
    assert (
        len(report.to_json().encode()) + sum(len(body.encode()) for body in report.attachments.values())
        <= 4096
    )


@pytest.mark.parametrize("budget", [128, 129, 256, 257])
def test_text_truncation_preserves_multibyte_characters_and_complete_lines(budget):
    body = "Результат ожидания 😀\n" * 100
    text = truncate_text(body, budget)
    assert len(text.encode("utf-8")) <= budget
    assert text.startswith("Результат ожидания 😀\n")
    assert text.endswith("[Text truncated: diagnostic byte limit reached.]\n")
    assert "\ufffd" not in text


def test_repeated_exports_charge_text_alongside_json_and_stop_at_cumulative_limit():
    limits = DiagnosticLimits(max_attachment_bytes=2048, max_export_bytes=8192)
    diagnostics = DiagnosticCollector("observer_1", DiagnosticPolicy(limits=limits))
    record_wait(diagnostics)
    reports = []
    for _ in range(32):
        try:
            reports.append(diagnostics.export())
        except DiagnosticExportError:
            break
    else:
        pytest.fail("Reports escaped the cumulative diagnostic byte limit")
    assert len(reports) > 1
    assert "expectation-result.txt" in reports[0].attachments
    assert "expectation-result.txt" in reports[1].attachments
    assert (
        sum(
            len(report.to_json().encode()) + sum(len(body.encode()) for body in report.attachments.values())
            for report in reports
        )
        <= limits.max_export_bytes
    )
    assert all(
        len(body.encode()) <= limits.max_attachment_bytes
        for report in reports
        for body in report.attachments.values()
    )
