"""Bounded diagnostic records and fail-closed, policy-based JSON exports."""

from __future__ import annotations

import base64
import json
import math
import re
from collections import OrderedDict, deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from enum import Enum
from itertools import islice
from types import MappingProxyType
from typing import Any, Protocol

from .._runtime.clock import Clock, SystemClock
from ..errors import DiagnosticConfigurationError, DiagnosticExportError
from ._text import expectation_text, truncate_text


class DisclosurePolicy(str, Enum):
    """Select whether diagnostic content may contain message bodies.

    :ivar METADATA_ONLY: Export technical metadata without inspecting payloads.
    :ivar REDACTED_PAYLOAD: Export only JSON-compatible output from a redactor.
    :ivar FULL_PAYLOAD: Export detached bodies, including explicit byte encodings.
    """

    METADATA_ONLY = "METADATA_ONLY"
    REDACTED_PAYLOAD = "REDACTED_PAYLOAD"
    FULL_PAYLOAD = "FULL_PAYLOAD"


class ReportingMode(str, Enum):
    """Choose when completed expectations trigger automatic reporting.

    :ivar ERRORS_ONLY: Report failures while retaining successful summaries locally.
    :ivar SUMMARY: Report each newly completed expectation or failure once.
    :ivar VERBOSE: Enable summary reporting without weakening disclosure limits.
    """

    ERRORS_ONLY = "ERRORS_ONLY"
    SUMMARY = "SUMMARY"
    VERBOSE = "VERBOSE"


@dataclass(frozen=True, slots=True)
class DiagnosticLimits:
    """Bound retained diagnostics and cumulative encoded output per watcher.

    :ivar max_records: Maximum retained lifecycle and error records.
    :ivar max_expectation_summaries: Maximum retained completed expectation summaries.
    :ivar max_mismatch_samples: First/last mismatching event samples per expectation.
    :ivar max_mismatches_per_sample: Maximum sanitized reasons in one mismatch sample.
    :ivar max_attachment_bytes: UTF-8 byte ceiling for a snapshot or single attachment.
    :ivar max_export_bytes: Shared lifetime byte ceiling for snapshots and attachments.
    :ivar max_matched_samples: First/last matching content samples per expectation.
    """

    max_records: int = 1000
    max_expectation_summaries: int = 256
    max_mismatch_samples: int = 20
    max_mismatches_per_sample: int = 10
    max_attachment_bytes: int = 256 * 1024
    max_export_bytes: int = 2 * 1024 * 1024
    max_matched_samples: int = 20

    def __post_init__(self) -> None:
        """Reject limits that cannot provide bounded, valid diagnostic output.

        :returns: ``None`` after validating positive integers and minimum JSON budgets.
        :raises DiagnosticConfigurationError: If a limit is invalid or smaller than 128 bytes.
        """
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise DiagnosticConfigurationError(f"{name} must be a positive integer")
        if self.max_attachment_bytes < 128 or self.max_export_bytes < 128:
            raise DiagnosticConfigurationError("JSON budgets must allow at least 128 bytes")


@dataclass(frozen=True, slots=True)
class DiagnosticPolicy:
    """Configure disclosure, reporting, metadata allowlists, and aliases.

    :ivar disclosure: Payload disclosure level; metadata-only by default.
    :ivar mode: Automatic reporting trigger policy.
    :ivar limits: Independent retention limits and shared export byte ceilings.
    :ivar redactor: Synchronous detached-payload sanitizer required for redacted output.
    :ivar allowed_fields: Additional metadata labels permitted in diagnostic exports.
    :ivar aliases: Immutable replacements for explicitly exposed metadata labels.
    :ivar export_trace_ids: Whether validated trace/span identifiers may be exported.
    :ivar export_tracestate: Whether the trace-state field may be exported.
    """

    disclosure: DisclosurePolicy = DisclosurePolicy.METADATA_ONLY
    mode: ReportingMode = ReportingMode.ERRORS_ONLY
    limits: DiagnosticLimits = field(default_factory=DiagnosticLimits)
    redactor: Callable[[Any], Any] | None = field(default=None, repr=False)
    allowed_fields: tuple[str, ...] = ()
    aliases: Mapping[str, str] = field(default_factory=dict, repr=False)
    export_trace_ids: bool = False
    export_tracestate: bool = False

    def __post_init__(self) -> None:
        """Normalize enums and detach policy collections before collection starts.

        :returns: ``None`` after replacing aliases and allowed fields with immutable copies.
        :raises DiagnosticConfigurationError: If a mode, limit, redactor, or metadata option is invalid.
        """
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
    """Detach JSON-shaped containers into immutable snapshot values.

    :param value: A scalar, dictionary, or nested sequence to freeze.
    :returns: Read-only mappings and tuples, with scalar values preserved.
    """
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    """Create mutable JSON-shaped containers from an immutable snapshot.

    :param value: A frozen mapping, tuple, or scalar.
    :returns: Detached dictionaries and lists suitable for JSON serialization.
    """
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _json_bytes(value: Any) -> bytes:
    """Encode compact, standards-compliant JSON for capture accounting.

    :param value: JSON-compatible diagnostic data.
    :returns: ASCII-escaped UTF-8 JSON bytes without insignificant whitespace.
    :raises TypeError: If a value is not JSON-compatible.
    :raises ValueError: If a value contains non-finite numbers or a circular reference.
    """
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _attachment_bytes(value: Any) -> bytes:
    """Encode the exact indented JSON format used in report attachments.

    :param value: JSON-compatible diagnostic data.
    :returns: UTF-8 bytes whose length is charged to export budgets.
    :raises TypeError: If a value cannot be encoded as JSON.
    :raises ValueError: If JSON would contain non-finite numbers or a circular reference.
    """
    return json.dumps(value, ensure_ascii=True, allow_nan=False, indent=2).encode("utf-8")


@dataclass(frozen=True, slots=True)
class DiagnosticRecord:
    """Represent one immutable lifecycle or failure record.

    :ivar schema_version: Version of the exported diagnostic schema.
    :ivar sequence: Watcher-local increasing diagnostic record number.
    :ivar kind: Sanitized technical event kind.
    :ivar observer_id: Identity of the watcher that owns this record.
    :ivar recorded_at: UTC timestamp serialized in ISO 8601 form.
    :ivar recorded_monotonic: Timestamp in the watcher's monotonic clock domain.
    :ivar attributes: Detached, sanitized technical metadata.
    :ivar scope_id: Associated scope identity, when recorded.
    :ivar expectation_id: Associated expectation identity, when recorded.
    """

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
        """Freeze nested record attributes independently of caller-owned mappings.

        :returns: ``None`` after replacing attributes with an immutable detached view.
        """
        object.__setattr__(self, "attributes", _freeze(dict(self.attributes)))

    def to_dict(self) -> dict[str, Any]:
        """Copy every schema field into a JSON-compatible mutable record.

        :returns: A detached dictionary; editing it does not change the record.
        """
        return {name: _thaw(getattr(self, name)) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class ExpectationDiagnostics:
    """Expose an immutable completed expectation summary.

    :ivar attributes: Sanitized outcome metrics and separately captured bounded samples.
    """

    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Detach and freeze summary attributes, including captured sample containers.

        :returns: ``None`` after installing immutable attributes.
        """
        object.__setattr__(self, "attributes", _freeze(dict(self.attributes)))

    def to_dict(self) -> dict[str, Any]:
        """Return a mutable copy of the sanitized expectation summary.

        :returns: A detached JSON-compatible dictionary.
        """
        return _thaw(self.attributes)

    def __getattr__(self, name: str) -> Any:
        """Provide attribute access to retained technical summary fields.

        :param name: Summary field to retrieve, such as ``outcome`` or ``lower_bound``.
        :returns: The immutable field value.
        :raises AttributeError: If the summary did not retain that field.
        """
        try:
            return self.attributes[name]
        except KeyError:
            raise AttributeError(name) from None


@dataclass(frozen=True, slots=True)
class ScopeDiagnostics:
    """Store immutable technical metadata for an observation scope.

    :ivar attributes: Detached scope diagnostic attributes.
    """

    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Detach and freeze the supplied scope metadata.

        :returns: ``None`` after installing immutable attributes.
        """
        object.__setattr__(self, "attributes", _freeze(dict(self.attributes)))


@dataclass(frozen=True, slots=True)
class DiagnosticSnapshot:
    """Capture retained records, completed expectations, and omission counters.

    :ivar observer_id: Watcher identity shared by records and expectations.
    :ivar records: Immutable lifecycle/error records in admission order.
    :ivar expectations: Immutable completed summaries in completion order.
    :ivar omitted_records: Number of records evicted by retention limits.
    :ivar omitted_expectations: Number of completed summaries evicted by retention.
    :ivar truncated: Whether retained diagnostics have known omissions or truncation.
    :ivar schema_version: Exported schema version.
    :ivar counters: Immutable receive, retention, and diagnostic-content counters.
    """

    observer_id: str
    records: tuple[DiagnosticRecord, ...]
    expectations: tuple[ExpectationDiagnostics, ...]
    omitted_records: int = 0
    omitted_expectations: int = 0
    truncated: bool = False
    schema_version: int = 1
    counters: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Detach aggregate counters so a captured snapshot cannot change later.

        :returns: ``None`` after freezing counters.
        """
        object.__setattr__(self, "counters", _freeze(dict(self.counters)))

    def to_dict(self) -> dict[str, Any]:
        """Build a detached JSON-compatible snapshot with explicit omissions.

        :returns: The snapshot schema, records, summaries, counters, and truncation flag.
        """
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
        """Serialize the retained snapshot without spending an export budget.

        :returns: Compact JSON text representing this immutable snapshot.
        """
        return _json_bytes(self.to_dict()).decode("utf-8")


@dataclass(frozen=True, slots=True)
class DiagnosticExport:
    """Hold one budgeted report and its already prepared attachments.

    :ivar data: Immutable metadata snapshot selected for this export.
    :ivar truncated: Whether any selected data or attachment was omitted or shortened.
    :ivar attachments: Immutable mapping of attachment names to UTF-8 text bodies.
    """

    data: Mapping[str, Any]
    truncated: bool = False
    attachments: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Detach report data and attachment mappings before reporter callbacks run.

        :returns: ``None`` after installing immutable report containers.
        """
        object.__setattr__(self, "data", _freeze(dict(self.data)))
        object.__setattr__(self, "attachments", MappingProxyType(dict(self.attachments)))

    def to_dict(self) -> dict[str, Any]:
        """Return an independent mutable copy of the exported metadata.

        :returns: A JSON-compatible dictionary detached from reporter-visible state.
        """
        return _thaw(self.data)

    def to_json(self) -> str:
        """Render metadata in the same indented JSON format used by attachments.

        :returns: UTF-8-compatible JSON text whose bytes were included in export accounting.
        """
        return _attachment_bytes(self.to_dict()).decode("utf-8")


class DiagnosticReporter(Protocol):
    """Define the minimal synchronous sink for a prepared diagnostic report.

    Implementations receive immutable data and must not own observer lifecycle state.
    """

    def emit(self, report: DiagnosticExport) -> None:
        """Consume one completed report in the caller's reporting context.

        :param report: Immutable, already budgeted metadata and attachment bodies.
        :returns: ``None`` when the sink has processed the report.
        :raises Exception: Implementations may fail; automatic reporting isolates sink failures.
        """
        ...


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
        "timeout",
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
        "matched_samples_omitted",
        "requested_start_mode",
        "requested_offsets",
        "resolved_positions",
        "ready_monotonic",
        "positions_omitted",
        "requested_offsets_omitted",
        "resolved_positions_omitted",
        "requested_stream_id",
        "resolved_stream_id",
    }
)
_CODE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:-]{0,127}\Z")
_NO_PAYLOAD = object()
_FAILURE_KINDS = frozenset(
    {"startup_failure", "terminal_failure", "cleanup_failure", "startup_cleanup_failure", "context_failure"}
)
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


@dataclass(slots=True)
class _SampleBuffer:
    """Retain the first and last samples while counting every eligible event.

    :ivar limit: Total first/last retention capacity, with the odd slot assigned first.
    :ivar first: Earliest retained sample dictionaries.
    :ivar last: Bounded deque containing the most recent trailing samples.
    :ivar count: Total eligible events, including samples omitted by retention.
    :ivar omitted: Eligible events whose sample bodies are absent from retained storage.
    """

    limit: int
    first: list[dict[str, Any]] = field(default_factory=list, init=False)
    last: deque[dict[str, Any]] = field(init=False)
    count: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        """Allocate the trailing half of the first/last sample capacity.

        :returns: ``None`` after creating the bounded trailing deque.
        """
        self.last = deque(maxlen=self.limit // 2)

    def append(self, capture: Callable[[], dict[str, Any]]) -> None:
        """Count an event and capture its sample only when a retention slot exists.

        :param capture: Lazy factory for one detached, sanitized sample.
        :returns: ``None`` after retaining or omitting the event's sample.
        :raises Exception: If a required sample factory fails; the event is then not counted.
        """
        if len(self.first) < (self.limit + 1) // 2:
            self.first.append(capture())
        elif self.last.maxlen:
            self.last.append(capture())
        self.count += 1

    def retained(self) -> list[dict[str, Any]]:
        """List retained samples in their original event order.

        :returns: A new list containing the earliest samples followed by the trailing samples.
        """
        return [*self.first, *self.last]

    @property
    def omitted(self) -> int:
        """Count eligible events whose sample bodies were not retained.

        :returns: Total eligible count minus the first and last retained sample counts.
        """
        return self.count - len(self.first) - len(self.last)


def _has_omitted_samples(attributes: Iterable[Mapping[str, Any]]) -> bool:
    """Recognize omission from either independent first/last sample budget.

    :param attributes: Selected sanitized summaries or diagnostic-record attributes.
    :returns: Whether any selected item omitted matched or mismatched samples.
    """
    return any(
        item.get("matched_samples_omitted") or item.get("mismatch_samples_omitted") for item in attributes
    )


def _sample_total(value: Any, fallback: int) -> int:
    """Prefer authoritative expectation counters over recoverable sample-buffer counts.

    :param value: Completed expectation's total, when supplied by the core.
    :param fallback: Count recoverable from current buffers and retained eviction records.
    :returns: A valid explicit nonnegative integer total, otherwise the fallback count.
    """
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else fallback


def _retained_suffix(data: dict[str, Any], removed: int) -> dict[str, Any]:
    """Select suffixes matching count-balanced removal from the oldest end.

    :param data: Detached snapshot with record and expectation lists.
    :param removed: Total oldest entries to remove, at most the two list lengths combined.
    :returns: A new top-level snapshot with suffix lists and updated omission counters.
    """
    records = data["records"]
    expectations = data["expectations"]
    record_drops = min(removed, max(0, len(records) - len(expectations)))
    expectation_drops = min(removed, max(0, len(expectations) - len(records)))
    balanced_drops = removed - record_drops - expectation_drops
    record_drops += (balanced_drops + 1) // 2
    expectation_drops += balanced_drops // 2
    return {
        **data,
        "records": records[record_drops:],
        "expectations": expectations[expectation_drops:],
        "omitted_records": data["omitted_records"] + record_drops,
        "omitted_expectations": data["omitted_expectations"] + expectation_drops,
        "truncated": True,
    }


def _bounded_snapshot(data: dict[str, Any], budget: int, record_sequence: int) -> dict[str, Any]:
    """Fit a detached snapshot without serializing once per discarded entry.

    Successful payload bodies are omitted before outcomes. Remaining removals retain
    exactly the same record/expectation suffixes as count-balanced oldest-first pruning.

    :param data: Detached snapshot; successful sample content may be replaced in place.
    :param budget: Maximum encoded bytes available for the metadata snapshot.
    :param record_sequence: Lifetime record count used by the last-resort omission document.
    :returns: A fitting snapshot or a minimal explicit omission document.
    """
    if len(_attachment_bytes(data)) <= budget:
        return data
    # Optional successful content must not displace its outcome.
    for expectation in data["expectations"]:
        for sample in expectation.get("matched_samples", []):
            content = sample["content"]
            if "payload" in content:
                sample["content"] = {
                    "payload_omitted": True,
                    "truncated": True,
                    "reason_code": "content_budget_exhausted",
                    "format": content["format"],
                }
                data["truncated"] = True
    if len(_attachment_bytes(data)) <= budget:
        return data
    # Each candidate is a suffix produced by the original count-balanced drop
    # order. Search the number of removals instead of serializing after each one.
    low = 1
    high = len(data["records"]) + len(data["expectations"])
    if high:
        while low < high:
            middle = (low + high) // 2
            if len(_attachment_bytes(_retained_suffix(data, middle))) <= budget:
                high = middle
            else:
                low = middle + 1
        data = _retained_suffix(data, low)
    if len(_attachment_bytes(data)) > budget:
        return {"schema_version": 1, "truncated": True, "omitted": record_sequence}
    return data


class DiagnosticCollector:
    """Collect bounded watcher diagnostics and prepare deduplicated reports.

    All mutable state belongs to the watcher's runtime. Reporter calls receive detached
    exports, and payload capture uses the same injected clock/copying dependencies as
    observation. This class does not open transports or run report callbacks in workers.

    :ivar observer_id: Identity attached to every diagnostic record and export.
    :ivar policy: Immutable disclosure, retention, and reporting policy.
    :ivar _clock: Watcher clock used for diagnostic UTC and monotonic timestamps.
    :ivar _clone_payload: Detached-payload copier invoked only for opted-in content.
    :ivar _records: Bounded lifecycle/error records and the recoverable sample-loss ledger.
    :ivar _expectations: Retained immutable completed expectation summaries.
    :ivar _sequence: Lifetime diagnostic record sequence counter.
    :ivar _omitted_records: Number of lifecycle/error records evicted by retention.
    :ivar _omitted_expectations: Number of completed summaries evicted by retention.
    :ivar _exported_bytes: Cumulative encoded metadata and attachment bytes already exported.
    :ivar _reporter_errors: Number of automatic export or reporter failures observed.
    :ivar _failure_generation: Lifetime count of reporting-triggering failures.
    :ivar _reported_failure_generation: Failure generation acknowledged by prepared reports.
    :ivar _expectation_generation: Lifetime number of completed expectation summaries.
    :ivar _reported_expectation_generation: Completion generation acknowledged by prepared reports.
    :ivar _reported_record_sequence: Last diagnostic record acknowledged by reporting.
    :ivar _counters: Aggregated receive, retention, and omitted-content counters.
    :ivar _content_capture_bytes: Cumulative content bytes admitted to diagnostic sample storage.
    :ivar _truncated_attributes: Whether technical metadata was shortened during sanitization.
    :ivar _mismatch_samples: Bounded per-expectation buffers of mismatch samples.
    :ivar _matched_samples: Independent bounded buffers of opted-in matching content samples.
    """

    def __init__(
        self,
        observer_id: str,
        policy: DiagnosticPolicy | None = None,
        *,
        clock: Clock | None = None,
        clone_payload: Callable[[Any], Any] | None = None,
    ) -> None:
        """Create an empty collector with explicit clock and payload isolation hooks.

        :param observer_id: Watcher identity to include in exported records.
        :param policy: Diagnostic policy, or conservative metadata-only defaults.
        :param clock: Runtime clock; defaults to the system UTC and monotonic clock.
        :param clone_payload: PayloadAccess-compatible copying callable, or the default safe copier.
        :returns: ``None`` after initializing independent retention and lifetime byte accounting.
        :raises DiagnosticConfigurationError: If the clock interface or copying callable is invalid.
        """
        self.observer_id = observer_id
        self.policy = policy or DiagnosticPolicy()
        self._clock = clock if clock is not None else SystemClock()
        if not callable(getattr(self._clock, "monotonic", None)) or not callable(
            getattr(self._clock, "utcnow", None)
        ):
            raise DiagnosticConfigurationError("clock must provide monotonic() and utcnow()")
        if clone_payload is None:
            from ..decoders import DefaultPayloadAccess

            clone_payload = DefaultPayloadAccess().clone_for_user
        if not callable(clone_payload):
            raise DiagnosticConfigurationError("clone_payload must be callable")
        self._clone_payload = clone_payload
        self._records: deque[DiagnosticRecord] = deque()
        self._expectations: deque[ExpectationDiagnostics] = deque()
        self._sequence = 0
        self._omitted_records = 0
        self._omitted_expectations = 0
        self._exported_bytes = 0
        self._reporter_errors = 0
        self._failure_generation = 0
        self._reported_failure_generation = 0
        self._expectation_generation = 0
        self._reported_expectation_generation = 0
        self._reported_record_sequence = 0
        self._counters = {"received": 0, "retained": 0, "dropped": 0, "filtered": 0}
        self._content_capture_bytes = 0
        self._truncated_attributes = False
        self._mismatch_samples: OrderedDict[str, _SampleBuffer] = OrderedDict()
        self._matched_samples: OrderedDict[str, _SampleBuffer] = OrderedDict()

    def _value(self, value: Any, *, strings: bool = False, depth: int = 0) -> Any:
        """Reduce metadata to bounded primitives without invoking arbitrary repr.

        :param value: Metadata value to sanitize recursively.
        :param strings: Whether explicitly allowed text may bypass technical-code validation.
        :param depth: Current nested-container depth, bounded to six levels.
        :returns: A safe bounded scalar/container, or ``None`` for unsupported content.
        """
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
        """Apply denylist, allowlist, alias, and trace-disclosure rules to metadata.

        :param attributes: Candidate technical attributes; raw message fields remain denied.
        :returns: A detached safe dictionary with explicit position-map truncation markers.
        """
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
        """Record a lifecycle/error event or aggregate a receive-path counter.

        :param kind: Technical record kind; admission and filtered events update counters only.
        :param attributes: Candidate metadata sanitized before retention.
        :returns: ``None`` after updating counters or appending a bounded timestamped record.
        """
        if kind == "admission":
            self._counters["received"] += 1
            self._counters["retained" if attributes.get("retained") else "dropped"] += 1
            return
        if kind == "filtered":
            self._counters["received"] += 1
            self._counters["filtered"] += 1
            return
        if kind in _FAILURE_KINDS:
            self._failure_generation += 1
        self._sequence += 1
        kind = kind if isinstance(kind, str) and _CODE.fullmatch(kind) else "diagnostic"
        safe = self._sanitize(attributes)
        record = DiagnosticRecord(
            schema_version=1,
            sequence=self._sequence,
            kind=kind,
            observer_id=self.observer_id,
            recorded_at=self._clock.utcnow().isoformat(),
            recorded_monotonic=self._clock.monotonic(),
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

    def _sample_buffer(
        self,
        buffers: OrderedDict[str, _SampleBuffer],
        expectation_id: str,
        limit: int,
        sample_kind: str,
    ) -> _SampleBuffer:
        """Find an expectation buffer and bound the number of in-progress buffers.

        :param buffers: Matched or mismatch buffer map, maintained independently.
        :param expectation_id: Identity of the active expectation.
        :param limit: First/last sample capacity for this buffer category.
        :param sample_kind: Matched or mismatch category used for explicit eviction metadata.
        :returns: Existing buffer or a fresh buffer after evicting the oldest map entry if needed.
        """
        if expectation_id not in buffers:
            if len(buffers) >= self.policy.limits.max_expectation_summaries:
                evicted_id, evicted = buffers.popitem(last=False)
                if evicted.count:
                    self.record(
                        "sample_buffer_evicted",
                        expectation_id=evicted_id,
                        reason_code=f"{sample_kind}_samples_evicted",
                        state="pending",
                        count=evicted.count,
                        truncated=True,
                        **{f"{sample_kind}_samples_omitted": evicted.count},
                    )
            buffers[expectation_id] = _SampleBuffer(limit)
        return buffers[expectation_id]

    def _account_sample_loss(self, expectation_id: str | None) -> dict[str, int]:
        """Transfer retained eviction counts into one completed expectation summary.

        Eviction records are the bounded loss ledger. Replacing an accounted record
        preserves existing immutable snapshots while preventing reused IDs from
        counting the same loss twice. Evicted ledger records remain visible through
        ordinary record-retention omission counters.

        :param expectation_id: Completed expectation identity, when supplied.
        :returns: Lost matched and mismatch counts recoverable from retained records.
        """
        losses = {"matched": 0, "mismatch": 0}
        if expectation_id is None:
            return losses
        records = deque()
        for record in self._records:
            if (
                record.kind == "sample_buffer_evicted"
                and record.expectation_id == expectation_id
                and record.attributes.get("state") == "pending"
            ):
                for category in losses:
                    count = record.attributes.get(f"{category}_samples_omitted", 0)
                    if isinstance(count, int) and not isinstance(count, bool) and count > 0:
                        losses[category] += count
                record = replace(record, attributes={**record.attributes, "state": "accounted"})
            records.append(record)
        self._records = records
        return losses

    def record_mismatch(
        self, expectation_id: str, event_sequence: int, mismatches: Any, *, payload: Any = _NO_PAYLOAD
    ) -> None:
        """Count a mismatch and lazily retain first/last sanitized reason samples.

        :param expectation_id: Expectation receiving the mismatching event.
        :param event_sequence: Journal sequence identifying the evaluated event.
        :param mismatches: Iterable of matcher reasons; expected and actual values are excluded.
        :param payload: Optional canonical payload, inspected only under an opted-in policy.
        :returns: ``None`` after updating the expectation's independent mismatch sample buffer.
        """
        limits = self.policy.limits
        buffer = self._sample_buffer(
            self._mismatch_samples, expectation_id, limits.max_mismatch_samples, "mismatch"
        )

        def capture_sample() -> dict[str, Any]:
            """Build one retained mismatch sample under the current disclosure policy.

            :returns: Sanitized bounded reasons and, only when opted in, detached content.
            """
            reasons = []
            for mismatch in islice(mismatches, limits.max_mismatches_per_sample):
                item = {"reason_code": getattr(mismatch, "reason_code", "mismatch")}
                if "path" in self.policy.allowed_fields:
                    item["path"] = getattr(mismatch, "path", "")
                reasons.append(self._sanitize(item))
            sample: dict[str, Any] = {"event_sequence": event_sequence, "mismatches": reasons}
            if payload is not _NO_PAYLOAD and self.policy.disclosure is not DisclosurePolicy.METADATA_ONLY:
                sample["content"] = self._capture_content(payload, limits.max_mismatch_samples)
            return sample

        buffer.append(capture_sample)

    def _capture_content(self, payload: Any, sample_limit: int) -> dict[str, Any]:
        """Capture one detached payload document under shared storage byte budgets.

        :param payload: Canonical payload to copy before redaction or serialization.
        :param sample_limit: Capacity used to divide the per-attachment content allowance.
        :returns: A format-tagged content document or an explicit safe omission marker.
        """
        content = self._payload_document(payload)
        try:
            content["format"] = "protobuf" if hasattr(payload, "DESCRIPTOR") else "json"
        except Exception:
            content["format"] = "json"
        encoded = _json_bytes(content)
        sample_budget = max(128, self.policy.limits.max_attachment_bytes // sample_limit)
        if (
            len(encoded) > sample_budget
            or self._content_capture_bytes + len(encoded) > self.policy.limits.max_export_bytes
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
        return content

    def record_match(self, expectation_id: str, event_sequence: int, *, payload: Any = _NO_PAYLOAD) -> None:
        """Count accepted matches and lazily retain opted-in first/last message bodies.

        :param expectation_id: Expectation whose result set accepted this event.
        :param event_sequence: Journal sequence of the accepted event.
        :param payload: Canonical payload, or a sentinel when no content is supplied.
        :returns: ``None``; metadata-only disclosure does not inspect or sample the payload.
        """
        if self.policy.disclosure is DisclosurePolicy.METADATA_ONLY or payload is _NO_PAYLOAD:
            return
        limits = self.policy.limits
        buffer = self._sample_buffer(
            self._matched_samples, expectation_id, limits.max_matched_samples, "matched"
        )

        def capture_sample() -> dict[str, Any]:
            """Build the detached content sample for a retained accepted match.

            :returns: The event sequence and a policy-filtered content document.
            """
            return {
                "event_sequence": event_sequence,
                "content": self._capture_content(payload, limits.max_matched_samples),
            }

        buffer.append(capture_sample)

    def record_expectation(self, **summary: Any) -> None:
        """Finalize a result summary and transfer its independent sample buffers.

        :param summary: Outcome metrics, identifiers, and error fields. Core-supplied ``matched``
            and ``mismatch_count`` totals preserve omission counts after loss-record eviction.
        :returns: ``None`` after sanitizing and retaining one immutable completed summary.
        """
        expectation_id = summary.get("expectation_id")
        losses = self._account_sample_loss(expectation_id if isinstance(expectation_id, str) else None)
        samples = (
            self._mismatch_samples.pop(expectation_id, None) if isinstance(expectation_id, str) else None
        )
        mismatch_total = _sample_total(
            summary.get("mismatch_count"), (samples.count if samples is not None else 0) + losses["mismatch"]
        )
        if samples is not None or mismatch_total:
            summary["mismatch_count"] = mismatch_total
            summary["mismatch_samples_omitted"] = max(
                0, mismatch_total - (len(samples.retained()) if samples is not None else 0)
            )
        matched_samples = (
            self._matched_samples.pop(expectation_id, None) if isinstance(expectation_id, str) else None
        )
        if self.policy.disclosure is not DisclosurePolicy.METADATA_ONLY:
            matched_total = _sample_total(
                summary.get("matched"),
                (matched_samples.count if matched_samples is not None else 0) + losses["matched"],
            )
            if matched_samples is not None or matched_total:
                summary["matched_samples_omitted"] = max(
                    0, matched_total - (len(matched_samples.retained()) if matched_samples is not None else 0)
                )
        if summary.get("outcome") not in {"matched", "success"}:
            self._failure_generation += 1
        if len(self._expectations) >= self.policy.limits.max_expectation_summaries:
            self._expectations.popleft()
            self._omitted_expectations += 1
        safe = self._sanitize(summary)
        if samples is not None:
            # These samples were already sanitized/copy-isolated at capture.
            # General-purpose attribute allowlists never permit raw payloads.
            safe["samples"] = samples.retained()
        if matched_samples is not None:
            safe["matched_samples"] = matched_samples.retained()
        self._expectations.append(ExpectationDiagnostics(safe))
        self._expectation_generation += 1

    def snapshot(self) -> DiagnosticSnapshot:
        """Capture retained state without spending bytes or acknowledging reports.

        :returns: An immutable snapshot with both matched and mismatch omission flags.
        """
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
                or _has_omitted_samples(summary.attributes for summary in self._expectations)
                or _has_omitted_samples(record.attributes for record in self._records)
            ),
            counters=self._counters,
        )

    def _payload_document(self, payload: Any) -> dict[str, Any]:
        """Copy and disclose a payload while failing closed on every content failure.

        :param payload: Canonical payload; metadata-only mode returns before inspecting it.
        :returns: Detached JSON-compatible content, an omission marker, or an empty mapping.
        """
        if self.policy.disclosure is DisclosurePolicy.METADATA_ONLY:
            return {}
        try:
            clone = self._clone_payload(payload)
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
            if self.policy.disclosure is DisclosurePolicy.FULL_PAYLOAD and isinstance(
                clone, (bytes, bytearray)
            ):
                try:
                    result = {"payload": clone.decode("utf-8"), "payload_encoding": "utf-8"}
                except UnicodeDecodeError:
                    result = {
                        "payload": base64.b64encode(clone).decode("ascii"),
                        "payload_encoding": "base64",
                    }
            encoded = _json_bytes(result)
            if len(encoded) > self.policy.limits.max_attachment_bytes:
                result = {"payload_omitted": True, "truncated": True}
        except Exception:
            result = {"payload_omitted": True, "redaction_error": "redaction_failed"}
        return json.loads(_json_bytes(result))

    def disclose_payload(self, payload: Any) -> dict[str, Any]:
        """Explicitly export one payload under disclosure and cumulative byte limits.

        :param payload: Payload to detach, optionally redact, and serialize.
        :returns: An independent content document, or an empty mapping in metadata-only mode.
        :raises DiagnosticExportError: If cumulative export bytes would exceed the configured limit.
        """
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
        """Project sanitized export data into the established attachment categories.

        :param data: Detached selected snapshot, including already captured sample content.
        :returns: Ordered JSON-compatible documents for outcomes, samples, timeline, and failures.
        """
        documents: dict[str, Any] = {}
        records = data.get("records", [])
        expectations = data.get("expectations", [])
        failures = [record for record in records if record["kind"] in _FAILURE_KINDS]
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
                outcome = {
                    key: value
                    for key, value in expectation.items()
                    if key not in {"samples", "matched_samples"}
                }
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
                for match_status, content_samples in (
                    ("mismatch", samples),
                    ("matched", expectation.get("matched_samples", [])),
                ):
                    for sample in content_samples:
                        if "content" not in sample:
                            continue
                        content = dict(sample["content"])
                        document = {
                            "expectation_id": expectation.get("expectation_id"),
                            "event_sequence": sample["event_sequence"],
                            "match_status": match_status,
                            **content,
                        }
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
        """Export the retained snapshot without acknowledging automatic report generations.

        :returns: A detached report whose encoded bytes consume the shared export allowance.
        :raises DiagnosticExportError: If insufficient lifetime budget remains for valid JSON.
        """
        return self._export(self.snapshot().to_dict())

    def prepare_report(self, *, force: bool = False) -> DiagnosticExport | None:
        """Prepare new outcomes/errors once and acknowledge only a successful export.

        :param force: Prepare retained pending data even without a normal reporting trigger.
        :returns: A budgeted report, or ``None`` when no selected mode trigger is pending.
        :raises DiagnosticExportError: If byte exhaustion prevents export; pending generations remain unacknowledged.
        """
        pending = self._expectation_generation - self._reported_expectation_generation
        failed = self._failure_generation > self._reported_failure_generation
        if not force:
            if self.policy.mode is ReportingMode.ERRORS_ONLY and not failed:
                return None
            if not pending and not failed:
                return None

        data = self.snapshot().to_dict()
        data["expectations"] = data["expectations"][-pending:] if pending else []
        data["records"] = [
            record for record in data["records"] if record["sequence"] > self._reported_record_sequence
        ]
        data["omitted_expectations"] = max(0, pending - len(data["expectations"]))
        data["omitted_records"] = max(
            0, self._sequence - self._reported_record_sequence - len(data["records"])
        )
        data["truncated"] = bool(
            data["omitted_expectations"]
            or data["omitted_records"]
            or self._counters.get("content_omitted")
            or self._truncated_attributes
            or _has_omitted_samples(data["expectations"])
            or _has_omitted_samples(record["attributes"] for record in data["records"])
        )
        report = self._export(data)
        # Internal generations remain useful when retention or export limits
        # omit IDs. A failed export leaves every pending item unacknowledged.
        self._reported_expectation_generation = self._expectation_generation
        self._reported_failure_generation = self._failure_generation
        self._reported_record_sequence = self._sequence
        return report

    def _export(self, data: dict[str, Any]) -> DiagnosticExport:
        """Bound a detached snapshot and encode attachments in their established priority order.

        :param data: Mutable selected snapshot, independent of the collector's retained records.
        :returns: An immutable report with exact cumulative UTF-8 byte accounting.
        :raises DiagnosticExportError: If fewer than 128 lifetime export bytes remain.
        """
        budget = min(
            self.policy.limits.max_attachment_bytes,
            self.policy.limits.max_export_bytes - self._exported_bytes,
        )
        if budget < 128:
            raise DiagnosticExportError("Diagnostic export budget exhausted")
        data = _bounded_snapshot(data, budget, self._sequence)
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
        if data.get("expectations"):
            available = min(
                self.policy.limits.max_attachment_bytes,
                self.policy.limits.max_export_bytes - self._exported_bytes - spent,
            )
            text = expectation_text(data)
            if len(text.encode("utf-8")) > available:
                data["truncated"] = True
                if available >= 128:
                    text = truncate_text(expectation_text(data), available)
                else:
                    text = ""
            if text:
                attachments["expectation-result.txt"] = text
        self._exported_bytes += len(_attachment_bytes(data)) + sum(
            len(value.encode("utf-8")) for value in attachments.values()
        )
        return DiagnosticExport(data, bool(data.get("truncated")), attachments)

    def record_reporter_error(self) -> None:
        """Record one automatic-export or sink failure with a shared increasing count.

        :returns: ``None`` after appending a safe reporter-error record without exception text.
        """
        self._reporter_errors += 1
        self.record("reporter_error", error_code="reporter_failed", reporter_errors=self._reporter_errors)

    def emit(
        self, reporters: tuple[DiagnosticReporter, ...] | list[DiagnosticReporter], *, force: bool = False
    ) -> None:
        """Prepare a report and invoke sinks without changing observation results.

        :param reporters: Synchronous sinks called in the current reporting context.
        :param force: Forward an explicit reporting trigger to report preparation.
        :returns: ``None``; export/sink failures update safe error bookkeeping instead of propagating.
        """
        if not reporters:
            return
        try:
            report = self.prepare_report(force=force)
        except Exception:
            self.record_reporter_error()
            return
        if report is None:
            return
        for reporter in reporters:
            try:
                reporter.emit(report)
            except Exception:
                self.record_reporter_error()


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
