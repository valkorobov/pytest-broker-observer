"""Human-readable expectation results from sanitized diagnostic data only."""

from collections.abc import Mapping
import math
import re
from typing import Any


_CODE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:-]{0,127}\Z")
_TRUNCATION_NOTICE = "\n[Text truncated: diagnostic byte limit reached.]\n"


def _code(value: Any) -> str:
    return value if isinstance(value, str) and _CODE.fullmatch(value) else "not recorded"


def _count_value(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _count(value: Any) -> str:
    count = _count_value(value)
    return str(count) if count is not None else "not recorded"


def _duration(value: Any) -> str:
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    ):
        return f"{value:.6g} s"
    return "not recorded"


def _flag(value: Any) -> str:
    return "yes" if value is True else "no" if value is False else "not recorded"


def _explanation(summary: Mapping[str, Any]) -> str:
    outcome = summary.get("outcome")
    if outcome in {"matched", "success"}:
        return "The requested matching event count was reached."
    if outcome == "cancelled":
        return "The expectation was cancelled before completion."
    if outcome == "error":
        return "The expectation stopped because an error occurred."
    if outcome != "timeout":
        return "No explanation is available for the recorded outcome."

    evaluated = _count_value(summary.get("evaluated"))
    matched = _count_value(summary.get("matched"))
    requested = _count_value(summary.get("requested_count"))
    if matched is not None and requested is not None and 0 < matched < requested:
        detail = "Only part of the requested event count matched."
    elif evaluated == 0:
        detail = (
            "Events were excluded by the scope; no events were evaluated by this expectation."
            if (_count_value(summary.get("scope_excluded")) or 0) > 0
            else "No events were evaluated by this expectation."
        )
    elif evaluated is not None and evaluated > 0 and _count_value(summary.get("not_applicable")) == evaluated:
        detail = "All evaluated events were not applicable to the matcher."
    elif evaluated is not None and evaluated > 0 and matched == 0:
        detail = "Events were evaluated, but none matched the expectation."
    else:
        detail = "The requested matching event count was not reached."
    return f"The observation deadline expired. {detail}"


def expectation_text(data: Mapping[str, Any]) -> str:
    """Render only known metrics and identifiers, excluding disclosed content."""
    lines = ["Expectation results (latest first)"]
    expectations = data.get("expectations", [])
    for index, summary in reversed(list(enumerate(expectations, start=1))):
        lines.extend(
            [
                "",
                f"{index}. Outcome: {_code(summary.get('outcome')).upper()}",
                _explanation(summary),
                f"Expectation ID: {_code(summary.get('expectation_id'))}",
                f"Scope ID: {_code(summary.get('scope_id'))}",
                f"Matching events: {_count(summary.get('matched'))} / {_count(summary.get('requested_count'))} requested",
                f"Evaluated events: {_count(summary.get('evaluated'))}",
                f"Elapsed: {_duration(summary.get('elapsed'))}",
                f"Configured timeout: {_duration(summary.get('timeout'))}",
                f"Deadline overrun: {_duration(summary.get('deadline_overrun'))}",
                f"Excluded by scope: {_count(summary.get('scope_excluded'))}",
                f"Not applicable: {_count(summary.get('not_applicable'))}",
                f"Observation complete: {_flag(summary.get('complete'))}; required: {_flag(summary.get('require_complete'))}",
            ]
        )
        if summary.get("error_code") is not None or summary.get("error_type") is not None:
            lines.append(f"Error: {_code(summary.get('error_code'))} ({_code(summary.get('error_type'))})")
        if "mismatch_count" in summary:
            lines.append(f"Mismatched events: {_count(summary.get('mismatch_count'))}")
        for sample in summary.get("samples", []):
            reasons = [_code(mismatch.get("reason_code")) for mismatch in sample.get("mismatches", [])]
            lines.append(
                f"Mismatch sample, event {_count(sample.get('event_sequence'))}: "
                + (", ".join(reasons) if reasons else "no detailed reason recorded")
            )
        if "mismatch_samples_omitted" in summary:
            lines.append(f"Omitted mismatch samples: {_count(summary.get('mismatch_samples_omitted'))}")
        if "matched_samples_omitted" in summary:
            lines.append(f"Omitted matched message samples: {_count(summary.get('matched_samples_omitted'))}")
    lines.extend(
        [
            "",
            f"Omitted expectations: {_count(data.get('omitted_expectations'))}",
            f"Diagnostic export truncated: {_flag(data.get('truncated'))}",
        ]
    )
    return "\n".join(lines)


def truncate_text(body: str, budget: int) -> str:
    """Keep complete UTF-8 text lines and an explicit byte-limit notice."""
    encoded = body.encode("utf-8")
    notice = _TRUNCATION_NOTICE.encode("utf-8")
    if len(encoded) <= budget:
        return body
    if budget < len(notice):
        return notice[: max(0, budget)].decode("utf-8", errors="ignore")
    prefix = encoded[: max(0, budget - len(notice))]
    if b"\n" in prefix:
        prefix = prefix.rsplit(b"\n", 1)[0]
    return prefix.decode("utf-8", errors="ignore") + _TRUNCATION_NOTICE
