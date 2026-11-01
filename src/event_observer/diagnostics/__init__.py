"""Bounded diagnostic records and fail-closed, policy-based JSON exports."""

from __future__ import annotations

import json
import math
import re
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from itertools import islice
from types import MappingProxyType
from typing import Any, Protocol

from ..errors import DiagnosticConfigurationError, DiagnosticExportError


class DisclosurePolicy(str, Enum):
    METADATA_ONLY = "METADATA_ONLY"
    REDACTED_PAYLOAD = "REDACTED_PAYLOAD"
    FULL_PAYLOAD = "FULL_PAYLOAD"


class ReportingMode(str, Enum):
    ERRORS_ONLY = "ERRORS_ONLY"
    SUMMARY = "SUMMARY"
    VERBOSE = "VERBOSE"


@dataclass(frozen=True, slots=True)
class DiagnosticLimits:
    max_records: int = 1000
    max_expectation_summaries: int = 256
    max_mismatch_samples: int = 20
    max_mismatches_per_sample: int = 10
    max_attachment_bytes: int = 256 * 1024
    max_export_bytes: int = 2 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise DiagnosticConfigurationError(f"{name} must be a positive integer")
        if self.max_attachment_bytes < 128 or self.max_export_bytes < 128:
            raise DiagnosticConfigurationError("JSON budgets must allow at least 128 bytes")


@dataclass(frozen=True, slots=True)
class DiagnosticPolicy:
    disclosure: DisclosurePolicy = DisclosurePolicy.METADATA_ONLY
    mode: ReportingMode = ReportingMode.ERRORS_ONLY
    limits: DiagnosticLimits = field(default_factory=DiagnosticLimits)
    redactor: Callable[[Any], Any] | None = field(default=None, repr=False)
    allowed_fields: tuple[str, ...] = ()
    aliases: Mapping[str, str] = field(default_factory=dict, repr=False)
    export_trace_ids: bool = False
    export_tracestate: bool = False

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "disclosure", DisclosurePolicy(self.disclosure))
            object.__setattr__(self, "mode", ReportingMode(self.mode))
        except ValueError as error:
            raise DiagnosticConfigurationError("Invalid diagnostic mode/disclosure") from error
        if not isinstance(self.limits, DiagnosticLimits):
            raise DiagnosticConfigurationError("limits must be DiagnosticLimits")
        if self.disclosure is DisclosurePolicy.REDACTED_PAYLOAD and not callable(self.redactor):
            raise DiagnosticConfigurationError("REDACTED_PAYLOAD requires a redactor")
        if (
            isinstance(self.allowed_fields, str)
            or not isinstance(self.allowed_fields, (tuple, list))
            or not all(isinstance(value, str) for value in self.allowed_fields)
        ):
            raise DiagnosticConfigurationError("allowed_fields must contain strings")
        if not isinstance(self.aliases, Mapping) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in self.aliases.items()
        ):
            raise DiagnosticConfigurationError("aliases must map strings to strings")
        if not isinstance(self.export_trace_ids, bool) or not isinstance(self.export_tracestate, bool):
            raise DiagnosticConfigurationError("Trace export flags must be bool")
        object.__setattr__(self, "allowed_fields", tuple(self.allowed_fields))
        object.__setattr__(self, "aliases", MappingProxyType(dict(self.aliases)))


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _attachment_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, indent=2).encode("utf-8")


@dataclass(frozen=True, slots=True)
class DiagnosticRecord:
    schema_version: int
    sequence: int
    kind: str
    observer_id: str
    recorded_at: str
    recorded_monotonic: float
    attributes: Mapping[str, Any] = field(default_factory=dict)
    scope_id: str | None = None
    expectation_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "attributes", _freeze(dict(self.attributes)))

    def to_dict(self) -> dict[str, Any]:
        return {name: _thaw(getattr(self, name)) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class ExpectationDiagnostics:
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "attributes", _freeze(dict(self.attributes)))

    def to_dict(self) -> dict[str, Any]:
        return _thaw(self.attributes)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.attributes[name]
        except KeyError:
            raise AttributeError(name) from None


@dataclass(frozen=True, slots=True)
class ScopeDiagnostics:
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "attributes", _freeze(dict(self.attributes)))


@dataclass(frozen=True, slots=True)
class DiagnosticSnapshot:
    observer_id: str
    records: tuple[DiagnosticRecord, ...]
    expectations: tuple[ExpectationDiagnostics, ...]
    omitted_records: int = 0
    omitted_expectations: int = 0
    truncated: bool = False
    schema_version: int = 1
    counters: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "counters", _freeze(dict(self.counters)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "observer_id": self.observer_id,
            "records": [record.to_dict() for record in self.records],
            "expectations": [summary.to_dict() for summary in self.expectations],
            "omitted_records": self.omitted_records,
            "omitted_expectations": self.omitted_expectations,
            "truncated": self.truncated,
            "counters": dict(self.counters),
        }

    def to_json(self) -> str:
        return _json_bytes(self.to_dict()).decode("utf-8")


@dataclass(frozen=True, slots=True)
class DiagnosticExport:
    data: Mapping[str, Any]
    truncated: bool = False
    attachments: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", _freeze(dict(self.data)))
        object.__setattr__(self, "attachments", MappingProxyType(dict(self.attachments)))

    def to_dict(self) -> dict[str, Any]:
        return _thaw(self.data)

    def to_json(self) -> str:
        return _attachment_bytes(self.to_dict()).decode("utf-8")


class DiagnosticReporter(Protocol):
    def emit(self, report: DiagnosticExport) -> None: ...


# String-valued arbitrary exception messages, labels and matcher data are not
# technical metadata. Only known identifiers/codes and numeric measures enter
# the default export. Explicit allowed_fields can opt in to other labels.
_SAFE_FIELDS = frozenset(
    {
        "status",
        "state",
        "outcome",
        "error_type",
        "error_code",
        "reason",
        "sequence",
        "event_sequence",
        "lower_bound",
        "upper_bound",
        "deadline",
        "elapsed",
        "duration",
        "evaluated",
        "matched",
        "filtered",
        "received",
        "scope_excluded",
        "mismatched",
        "not_applicable",
        "requested_count",
        "count",
        "retained_events",
        "retained_bytes",
        "dropped",
        "complete",
        "require_complete",
        "truncated",
        "omitted",
        "startup_deadline",
        "cleanup_deadline",
        "cancelled",
        "callback_overrun",
        "reporter_errors",
        "result_bytes",
        "scope_lower_bound",
        "start_mode",
        "boundary",
        "requested_start",
        "resolved_start",
        "cleanup_outcome",
        "terminal_error",
        "trace_status",
        "mismatch_count",
        "mismatches",
        "code",
        "scope_id",
        "expectation_id",
        "reason_code",
        "cleanup_state",
        "deadline_overrun",
        "samples",
        "mismatch_samples_omitted",
        "requested_start_mode",
        "requested_offsets",
        "resolved_positions",
        "ready_monotonic",
        "positions_omitted",
        "requested_offsets_omitted",
        "resolved_positions_omitted",
    }
)
_CODE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:-]{0,127}\Z")
_NO_PAYLOAD = object()
_DENIED_FIELDS = frozenset(
    {
        "password",
        "sasl_password",
        "credentials",
        "client_options",
        "transport_config",
        "ssl_key_password",
        "ssl_key_location",
        "bootstrap_servers",
        "exception",
        "cause",
        "payload",
        "raw",
        "key",
        "headers",
        "expected",
        "actual",
        "native_client",
    }
)


class DiagnosticCollector:
    def __init__(self, observer_id: str, policy: DiagnosticPolicy | None = None) -> None:
        self.observer_id = observer_id
        self.policy = policy or DiagnosticPolicy()
        self._records: deque[DiagnosticRecord] = deque()
        self._expectations: deque[ExpectationDiagnostics] = deque()
        self._sequence = 0
        self._omitted_records = 0
        self._omitted_expectations = 0
        self._exported_bytes = 0
        self._reporter_errors = 0
        self._failure_generation = 0
        self._reported_failure_generation = 0
        self._counters = {"received": 0, "retained": 0, "dropped": 0, "filtered": 0}
        self._content_capture_bytes = 0
        self._truncated_attributes = False
        self._mismatch_samples: OrderedDict[str, tuple[list[dict[str, Any]], deque[dict[str, Any]], int]] = (
            OrderedDict()
        )

    def _value(self, value: Any, *, strings: bool = False, depth: int = 0) -> Any:
        if depth > 6:
            return None
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value if value.bit_length() <= 64 else None
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, Enum):
            value = value.value
        if isinstance(value, str):
            value = self.policy.aliases.get(value, value)
            return value[:512] if strings else (value if _CODE.fullmatch(value) else None)
        if isinstance(value, (tuple, list)):
            return [self._value(item, strings=strings, depth=depth + 1) for item in value[:20]]
        if isinstance(value, Mapping):
            return {
                str(key): self._value(
                    item, strings=strings or key in self.policy.allowed_fields, depth=depth + 1
                )
                for key, item in islice(value.items(), 20)
                if (isinstance(key, str) or (isinstance(key, int) and not isinstance(key, bool)))
                and key not in _DENIED_FIELDS
                and (key in _SAFE_FIELDS or key in self.policy.allowed_fields or str(key).isdecimal())
            }
        return None

    def _sanitize(self, attributes: Mapping[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in attributes.items():
            if key in _DENIED_FIELDS:
                continue
            if key == "trace_id" or key == "span_id":
                if self.policy.export_trace_ids:
                    size = 32 if key == "trace_id" else 16
                    result[key] = (
                        value
                        if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % size, value)
                        else None
                    )
            elif key == "tracestate":
                if self.policy.export_tracestate:
                    result[key] = self._value(value, strings=True)
            elif key in _SAFE_FIELDS or key in self.policy.allowed_fields:
                result[key] = self._value(value, strings=key in self.policy.allowed_fields)
                if (
                    key in {"requested_offsets", "resolved_positions"}
                    and isinstance(value, Mapping)
                    and len(value) > 20
                ):
                    result[key + "_omitted"] = len(value) - 20
                    result["truncated"] = True
                    self._truncated_attributes = True
        return result

    def record(self, kind: str, **attributes: Any) -> None:
        if kind == "admission":
            self._counters["received"] += 1
            self._counters["retained" if attributes.get("retained") else "dropped"] += 1
            return
        if kind == "filtered":
            self._counters["received"] += 1
            self._counters["filtered"] += 1
            return
        if kind in {"terminal_failure", "cleanup_failure", "startup_cleanup_failure", "context_failure"}:
            self._failure_generation += 1
        self._sequence += 1
        kind = kind if isinstance(kind, str) and _CODE.fullmatch(kind) else "diagnostic"
        safe = self._sanitize(attributes)
        record = DiagnosticRecord(
            schema_version=1,
            sequence=self._sequence,
            kind=kind,
            observer_id=self.observer_id,
            recorded_at=datetime.now(timezone.utc).isoformat(),
            recorded_monotonic=time.monotonic(),
            attributes=safe,
            scope_id=safe.get("scope_id"),
            expectation_id=safe.get("expectation_id"),
        )
        # Receive-path counters should be aggregated by the core, not emitted
        # once per record. Reserve lifecycle/error capacity against summaries.
        if len(self._records) >= self.policy.limits.max_records:
            self._records.popleft()
            self._omitted_records += 1
        self._records.append(record)

    def record_mismatch(
        self, expectation_id: str, event_sequence: int, mismatches: Any, *, payload: Any = _NO_PAYLOAD
    ) -> None:
        """Retain first/last reason samples without preserving expected/actual."""
        limits = self.policy.limits
        if expectation_id not in self._mismatch_samples:
            if len(self._mismatch_samples) >= limits.max_expectation_summaries:
                self._mismatch_samples.popitem(last=False)
            self._mismatch_samples[expectation_id] = ([], deque(maxlen=limits.max_mismatch_samples // 2), 0)
        first, last, count = self._mismatch_samples[expectation_id]
        reasons = []
        for mismatch in islice(mismatches, limits.max_mismatches_per_sample):
            item = {"reason_code": getattr(mismatch, "reason_code", "mismatch")}
            if "path" in self.policy.allowed_fields:
                item["path"] = getattr(mismatch, "path", "")
            reasons.append(self._sanitize(item))
        sample: dict[str, Any] = {"event_sequence": event_sequence, "mismatches": reasons}
        retains_sample = len(first) < (limits.max_mismatch_samples + 1) // 2 or bool(last.maxlen)
        if (
            retains_sample
            and payload is not _NO_PAYLOAD
            and self.policy.disclosure is not DisclosurePolicy.METADATA_ONLY
        ):
            content = self._payload_document(payload)
            try:
                content["format"] = "protobuf" if hasattr(payload, "DESCRIPTOR") else "json"
            except Exception:
                content["format"] = "json"
            encoded = _json_bytes(content)
            sample_budget = max(128, limits.max_attachment_bytes // limits.max_mismatch_samples)
            if (
                len(encoded) > sample_budget
                or self._content_capture_bytes + len(encoded) > limits.max_export_bytes
            ):
                content = {
                    "payload_omitted": True,
                    "truncated": True,
                    "reason_code": "content_budget_exhausted",
                    "format": content["format"],
                }
            else:
                self._content_capture_bytes += len(encoded)
            if content.get("payload_omitted"):
                self._counters["content_omitted"] = self._counters.get("content_omitted", 0) + 1
            sample["content"] = content
        if len(first) < (limits.max_mismatch_samples + 1) // 2:
            first.append(sample)
        else:
            last.append(sample)
        self._mismatch_samples[expectation_id] = (first, last, count + 1)

    def record_expectation(self, **summary: Any) -> None:
        expectation_id = summary.get("expectation_id")
        samples = (
            self._mismatch_samples.pop(expectation_id, None) if isinstance(expectation_id, str) else None
        )
        if samples is not None:
            first, last, count = samples
            summary = dict(summary)
            summary["samples"] = [*first, *last]
            summary["mismatch_count"] = count
            summary["mismatch_samples_omitted"] = max(0, count - len(first) - len(last))
        if summary.get("outcome") not in {"matched", "success"}:
            self._failure_generation += 1
        if len(self._expectations) >= self.policy.limits.max_expectation_summaries:
            self._expectations.popleft()
            self._omitted_expectations += 1
        safe = self._sanitize(summary)
        if samples is not None:
            # These samples were already sanitized/copy-isolated at capture.
            # General-purpose attribute allowlists never permit raw payloads.
            safe["samples"] = [*samples[0], *samples[1]]
        self._expectations.append(ExpectationDiagnostics(safe))

    def snapshot(self) -> DiagnosticSnapshot:
        return DiagnosticSnapshot(
            self.observer_id,
            tuple(self._records),
            tuple(self._expectations),
            self._omitted_records,
            self._omitted_expectations,
            bool(
                self._omitted_records
                or self._omitted_expectations
                or self._counters.get("content_omitted")
                or self._truncated_attributes
            ),
            counters=self._counters,
        )

    def _payload_document(self, payload: Any) -> dict[str, Any]:
        if self.policy.disclosure is DisclosurePolicy.METADATA_ONLY:
            return {}
        try:
            from ..decoders import DefaultPayloadAccess

            clone = DefaultPayloadAccess().clone_for_user(payload)
            if self.policy.disclosure is DisclosurePolicy.REDACTED_PAYLOAD:
                redactor = self.policy.redactor
                if redactor is None:
                    raise DiagnosticConfigurationError("Redacted content requires a redactor")
                clone = redactor(clone)
            elif hasattr(clone, "DESCRIPTOR"):
                from google.protobuf.json_format import MessageToDict

                clone = MessageToDict(clone, preserving_proto_field_name=True)
            # JSON's own encoder rejects native objects and bounds are applied
            # before this content is retained in diagnostics.
            result = {"payload": clone}
            encoded = _json_bytes(result)
            if len(encoded) > self.policy.limits.max_attachment_bytes:
                result = {"payload_omitted": True, "truncated": True}
        except Exception:
            result = {"payload_omitted": True, "redaction_error": "redaction_failed"}
        return json.loads(_json_bytes(result))

    def disclose_payload(self, payload: Any) -> dict[str, Any]:
        """Explicit content export. Never invoked by the receive path."""
        result = self._payload_document(payload)
        if not result:
            return result
        encoded = _json_bytes(result)
        if self._exported_bytes + len(encoded) > self.policy.limits.max_export_bytes:
            raise DiagnosticExportError("Diagnostic export budget exhausted")
        self._exported_bytes += len(encoded)
        return json.loads(encoded)

    @staticmethod
    def _attachment_documents(data: dict[str, Any]) -> dict[str, Any]:
        documents: dict[str, Any] = {}
        records = data.get("records", [])
        expectations = data.get("expectations", [])
        failure_kinds = {"terminal_failure", "cleanup_failure", "startup_cleanup_failure", "context_failure"}
        failures = [record for record in records if record["kind"] in failure_kinds]
        if failures:
            documents["observer-summary.json"] = {
                "schema_version": 1,
                "counters": data.get("counters", {}),
                "failures": failures,
            }
        if expectations:
            outcomes = []
            mismatches = []
            protobuf_content = []
            other_content = []
            for expectation in expectations:
                outcome = {key: value for key, value in expectation.items() if key != "samples"}
                outcomes.append(outcome)
                samples = expectation.get("samples", [])
                if samples:
                    mismatches.append(
                        {
                            "expectation_id": expectation.get("expectation_id"),
                            "samples": [
                                {key: value for key, value in sample.items() if key != "content"}
                                for sample in samples
                            ],
                        }
                    )
                for sample in samples:
                    if "content" not in sample:
                        continue
                    content = dict(sample["content"])
                    document = {"event_sequence": sample["event_sequence"], **content}
                    (protobuf_content if content.get("format") == "protobuf" else other_content).append(
                        document
                    )
            documents["expectation-result.json"] = {"schema_version": 1, "expectations": outcomes}
            if mismatches:
                documents["mismatches.json"] = {"schema_version": 1, "expectations": mismatches}
            if protobuf_content:
                documents["protobuf-message.json"] = {"schema_version": 1, "messages": protobuf_content}
            if other_content:
                documents["message-content.json"] = {"schema_version": 1, "messages": other_content}
        if records:
            documents["event-timeline.json"] = {"schema_version": 1, "records": records}
        transport = [
            record
            for record in failures
            if (
                str(record.get("attributes", {}).get("error_code", "")).startswith("transport_")
                or record.get("attributes", {}).get("error_code")
                in {"topology_changed", "unexpected_stream_end", "start_position_unavailable"}
                or record.get("attributes", {}).get("error_type")
                in {"TopologyChangedError", "UnexpectedStreamEndError", "StartPositionUnavailableError"}
            )
        ]
        if transport:
            documents["transport-state.json"] = {"schema_version": 1, "failures": transport}
        return documents

    def export(self) -> DiagnosticExport:
        data = self.snapshot().to_dict()
        budget = min(
            self.policy.limits.max_attachment_bytes,
            self.policy.limits.max_export_bytes - self._exported_bytes,
        )
        if budget < 128:
            raise DiagnosticExportError("Diagnostic export budget exhausted")
        while len(_attachment_bytes(data)) > budget and (data["records"] or data["expectations"]):
            if len(data["records"]) >= len(data["expectations"]):
                data["records"].pop(0)
                data["omitted_records"] += 1
            else:
                data["expectations"].pop(0)
                data["omitted_expectations"] += 1
            data["truncated"] = True
        if len(_attachment_bytes(data)) > budget:
            data = {"schema_version": 1, "truncated": True, "omitted": self._sequence}
        encoded = _attachment_bytes(data)
        attachments = {}
        spent = len(encoded)
        for name, document in self._attachment_documents(data).items():
            attachment = _attachment_bytes(document)
            available = min(
                self.policy.limits.max_attachment_bytes,
                self.policy.limits.max_export_bytes - self._exported_bytes - spent,
            )
            if len(attachment) > available:
                data["truncated"] = True
                if available < 128:
                    continue
                attachment = _attachment_bytes({"schema_version": 1, "truncated": True, "omitted": True})
            attachments[name] = attachment.decode("utf-8")
            spent += len(attachment)
        self._exported_bytes += len(_attachment_bytes(data)) + sum(
            len(value.encode("utf-8")) for value in attachments.values()
        )
        return DiagnosticExport(data, bool(data.get("truncated")), attachments)

    def emit(
        self, reporters: tuple[DiagnosticReporter, ...] | list[DiagnosticReporter], *, force: bool = False
    ) -> None:
        """Best effort automatic reporting; reporter faults never change results."""
        if not reporters:
            return
        if (
            not force
            and self.policy.mode is ReportingMode.ERRORS_ONLY
            and self._failure_generation <= self._reported_failure_generation
        ):
            return
        try:
            report = self.export()
        except Exception:
            self._reporter_errors += 1
            return
        self._reported_failure_generation = self._failure_generation
        for reporter in reporters:
            try:
                reporter.emit(report)
            except Exception:
                self._reporter_errors += 1
                self.record(
                    "reporter_error", error_code="reporter_failed", reporter_errors=self._reporter_errors
                )


__all__ = [
    "DiagnosticCollector",
    "DiagnosticExport",
    "DiagnosticLimits",
    "DiagnosticPolicy",
    "DiagnosticRecord",
    "DiagnosticReporter",
    "DiagnosticSnapshot",
    "DisclosurePolicy",
    "ExpectationDiagnostics",
    "ReportingMode",
    "ScopeDiagnostics",
]
