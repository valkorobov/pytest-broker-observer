# Diagnostics and safe reports

A watcher provides `watcher.diagnostics.snapshot()`. The snapshot contains
immutable lifecycle records, completed expectations, and aggregated receive
counters. `scope.snapshot()` shows the saved expectation results for that scope;
the user predicate does not need to be called again for reporting.

An expectation summary distinguishes `evaluated`, `matched`, `scope_excluded`,
and `not_applicable`. For mismatches, the first and last samples, event sequence
numbers, and reason codes are retained. `EventTimeoutError.diagnostics` helps
distinguish an empty stream from an unsuitable correlation or insufficient count.

```python
from event_observer.diagnostics import DiagnosticLimits, DiagnosticPolicy, ReportingMode

policy = DiagnosticPolicy(
    mode=ReportingMode.SUMMARY,
    limits=DiagnosticLimits(
        max_records=500,
        max_expectation_summaries=100,
        max_mismatch_samples=10,
        max_matched_samples=10,
        max_attachment_bytes=128 * 1024,
    ),
)
```

By default, `ERRORS_ONLY` sends a report to reporters on failure. `SUMMARY`
also includes successful operations. `VERBOSE` retains the same memory limits.
Incoming events are aggregated in `snapshot.counters` so frequent receives do
not evict lifecycle records. The default total watcher export budget is 2 MiB;
a single JSON attachment is limited to 256 KiB. Truncation preserves valid JSON
and explicit omission counters.

`watcher.diagnostics.export()` returns `DiagnosticExport`; its `.to_dict()`
and `.to_json()` create a detached representation. `.to_json()` and all report
JSON attachments are formatted with `indent=2`. Limits account for the UTF-8
document size, including indentation and line breaks. The snapshot has already
been sanitized according to the policy; it contains no protobuf objects,
exceptions, or native clients.

## Content disclosure

In JSON exports, `METADATA_ONLY` excludes payload, key, headers, expected/actual
values, user descriptions, source labels, and trace IDs. `allowed_fields`
permits selected additional labels; `aliases` replaces their values.
Credentials and transport configuration are excluded from diagnostic exports.

With disclosure enabled, matching messages and mismatch samples automatically
receive bounded content copies. `matched_samples` retains the first and last
matching messages per expectation (20 by default, configurable through
`max_matched_samples`); `matched_samples_omitted` counts the rest. Mismatch
sampling has its own `max_mismatch_samples` limit. Explicit `disclose_payload()`
is also available for individual artifacts:

```python
from event_observer.diagnostics import DiagnosticPolicy, DisclosurePolicy


def redact(payload):
    payload.pop("access_token", None)
    return payload


policy = DiagnosticPolicy(
    disclosure=DisclosurePolicy.REDACTED_PAYLOAD,
    redactor=redact,
)
```

The redactor receives an independent copy. An exception or invalid JSON from
it produces `payload_omitted` and a safe error code, without falling back to
the original content. `FULL_PAYLOAD` is a separate explicit permission to
disclose a message. Content processing runs during expectation evaluation,
outside receive and without Journal locks. By default, the payload is not
inspected even for reporting. Total retained sample content is limited by
a shared capture budget equal to `max_export_bytes` across matched/mismatched
samples and expectations; when exhausted, metadata and a truncation indicator
are retained. Capture or redaction failures omit content without changing a
successful wait result. Export limits include all formatted JSON and text and
apply cumulatively across reports. If successful sample content makes an export
too large, it is replaced with omission markers before expectation summaries
are removed.

For `FULL_PAYLOAD`, top-level `bytes` and `bytearray` are represented as UTF-8
text with `payload_encoding="utf-8"`, or lossless base64 with
`payload_encoding="base64"` when UTF-8 decoding fails. Empty bytes remain an
empty string; a tombstone remains JSON `null`. Redactors still receive the
original detached payload type and must return JSON-compatible data; their
invalid output is never replaced with the raw message.

Original exceptions may remain in Python chaining. The policy protects the
library's own exports; it does not control pytest tracebacks or external user
loggers.

## Allure

Install the dependency from the local project:

```shell
python -m pip install -e '.[allure]'
```

```python
from event_observer import SyncEventObserver
from event_observer.diagnostics import DiagnosticPolicy, ReportingMode
from event_observer.integrations.allure import AllureReporter

events = SyncEventObserver(
    transport=adapter,
    reporters=[AllureReporter()],
    diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
)
```

Run pytest with `--alluredir` so the plugin records results and the reporter
receives an active test context. `SUMMARY` in the example includes attachments
on success; without this setting, `ERRORS_ONLY` applies. The complete runnable
test suite, HTML generation, and report viewing, including an intentional
`FAILED`, are described in [examples/allure](../examples/allure/README.md).

For a real broker, see the [Kafka examples with AllureReporter](../examples/allure/README.md#kafka-and-allurereporter):
Compose, correlation, an expected timeout, and a separate report with JSON attachments.

The reporter creates completed summary steps, does not hold a step open across
`await`, and does not create a step for each message. It checks the active test
context and does not attach an observation report to the next test. The actual
expectation duration is stored in JSON. A reporter failure does not change
the expectation outcome. Each watcher receives a separate Allure binding in
the current test's context, so a factory with a reporter can be kept in a
session fixture. A previous watcher's binding does not write attachments to
the next test.

Automatic reports include only newly completed expectations and new diagnostic
records. Entering READY and normally exiting `watch()` do not create additional
summary steps. In `SUMMARY` mode, one expectation produces one summary with
its result in Allure.
A new context, transport, or cleanup error creates a separate report without
attaching previously sent matching results again.
Manual `snapshot()` and `export()` retain the watcher's full retained history.

The summary title includes the transport adapter class and the watcher's topic/stream:
`Event observation summary [KafkaAdapter: orders.events]`.
These names appear in the title regardless of JSON disclosure; `aliases` also
applies to them. For example, `aliases={"KafkaAdapter": "Kafka", "internal.orders": "orders"}`
produces the title `Event observation summary [Kafka: orders]`. Each label is
limited to 512 characters, with line breaks replaced by spaces. Each watcher
has its own title, including when sharing a factory and running concurrent
expectations.

`DiagnosticExport.attachments` contains ready-to-use JSON artifacts:
`expectation-result.json`, `mismatches.json`, `event-timeline.json`, and, for
the corresponding errors, `observer-summary.json` and `transport-state.json`.
Disclosed Protobuf content goes into `protobuf-message.json`; other
JSON-compatible payloads go into `message-content.json`. Allure attaches only
these already sanitized documents. Limits account for encoded JSON and the
combined size of all attachments.

Each message entry includes `expectation_id`, `event_sequence`, and
`match_status` (`matched` or `mismatch`). The status describes the event's
evaluation: a partially satisfied count expectation may retain matched bodies
even when its final outcome is timeout, cancellation, or error. Outcome and
mismatch attachments do not duplicate the message bodies; the text explanation
also remains free of message content.

Completed expectations also receive `expectation-result.txt` with a text
explanation. It shows the outcome and its explanation, the number of events
found and requested, elapsed time and the configured timeout, scope exclusions,
not-applicable types, mismatch reasons, and observation completeness. When one
export contains several expectations, the most recently completed one comes
first; the expectation ID links the text to JSON. Automatic reporting does
not repeat previous expectations.

The text is built from retained sanitized results, without calling the matcher
again. It excludes payload, key, headers, and exception text; disclosed message
content remains in separate JSON attachments. Allure attaches the explanation
as `text/plain`. The same limits on actual UTF-8 bytes and the total export
budget apply to text. When the budget is insufficient, JSON artifacts take
priority; text may be truncated with a notice or omitted.

## Trace context

`ObservedEvent.trace_context` contains `MISSING`, `VALID`, `INVALID`, or
`AMBIGUOUS`. An invalid `traceparent` does not prevent message decoding.
Invalid `tracestate` is discarded separately from a valid trace ID.
The library respects W3C versioning, does not create spans, and does not
install the extracted context as the current OpenTelemetry context.

Trace IDs are exported only with `export_trace_ids=True`, and tracestate
requires a separate `export_tracestate=True`. Baggage is not carried automatically.

## Payload isolation and estimation

The watcher's configured `PayloadAccess` returns detached copies to user
callbacks, results, and the redactor. The default is `DefaultPayloadAccess`;
a custom copying strategy also applies to diagnostic content. Protobuf uses `CopyFrom`; its accounted estimate is
`128 + 2 * ByteSize()`. For other objects, the estimate sums `sys.getsizeof`
for unique objects, built-in container contents, instance dictionaries,
and slots, including inherited private slots.

Traversal is limited to 64 levels and 100,000 visited references. A large
container is traversed incrementally, without an additional list of all its
elements. A shared object is not counted twice, but visits to repeated
references are also bounded. Exceeding the budget raises `PayloadIsolationError`.

Opaque native allocations cannot be measured by this traversal. A decoder
returning such objects must provide its own `PayloadAccess` with
`clone_for_user(value)` and `estimate_bytes(value)`. The accounted-bytes model
is not a measurement of the entire process's RSS.

Diagnostic timestamps use the watcher's injected `Clock`, including when it is
used to control admission time in deterministic tests. A standalone
`DiagnosticCollector` defaults to system time and default payload copying, or
accepts explicit `clock` and `clone_payload` dependencies.
