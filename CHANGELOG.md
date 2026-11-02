# Changelog

## 0.1.0 — candidate, unpublished

The first MVP based on [SPEC.md](SPEC.md), with distribution name
`pytest-broker-observer` and import namespace `event_observer`.

Added:

- An enforced 80% unit coverage threshold for the complete library with branch
  measurement, checked on Python 3.11–3.14 with HTML/XML/JSON CI artifacts.
  Coverage starts before pytest loads the passive plugin; examples and real
  brokers run separately. Broker-free Kafka harness and public API regressions
  cover confirmed publication, owned-resource cleanup, and lazy imports.
- A shared asynchronous core and synchronous facade with a continuously running runtime loop.
- Lazy factories, readiness, lifecycle, controlled cleanup, and typed errors.
- Independent Kafka sessions through `confluent-kafka`, explicit start boundaries,
  timestamp replay, and topology-change detection.
- A bounded Journal with three overflow policies and a conservative loss ledger.
- `wait_for`, `wait_for_event`, `wait_for_count`, scoped correlation, and nesting.
- Raw/Protobuf decoders, explicit type resolvers, payload isolation, and matchers
  with three-valued logic.
- Bounded safe diagnostics, opt-in Allure, and passive W3C trace context.
- FakeTransport, AdapterTestHarness, and reusable transport contract suites.
- Typing with `py.typed`, optional extras, a passive pytest plugin, wheel/sdist
  packaging, documentation, and a CI workflow.
- Pre-commit checks for Ruff lint, formatting, and Pyrefly using versions from
  `uv.lock`; CI runs the same checks.
- Runnable pytest examples with fixtures, parametrization, synchronous/asynchronous
  APIs, Protobuf, and a separate opt-in Kafka scenario; CI includes the examples.
- Docker Compose with local Kafka in KRaft mode, a readiness health check,
  a configurable port, and a persistent volume; local tests and CI share the profile.
- Pytest examples with AllureReporter: synchronous/asynchronous success, timeout,
  redaction, and an opt-in intentional failure; standalone HTML viewing and a CI artifact.
- Kafka integration examples with AllureReporter: successful event correlation,
  expected-timeout diagnostics, local Compose setup, and a separate HTML report
  uploaded by the Kafka CI job.
- Report JSON attachments with `indent=2`, with formatting included in byte limits;
  Allure summary titles include the transport and topic/stream and support aliases.
- An `expectation-result.txt` attachment explaining wait outcomes, timing, counters,
  completeness, and mismatch reasons; UTF-8 budgets and export policy also apply
  to the text summary.
- Automatic reports without repeated results: each wait is sent once, ordinary
  close creates no summary, and new errors retain separate diagnostics. Manual
  export remains complete.
- English project documentation, specification, examples, comments, and Allure
  labels; local and CI report generation defaults to English.
- Opted-in matched message bodies alongside mismatch samples, with independent
  first/last sample limits, shared byte budgets, detached redaction, and explicit
  expectation/status labels. Raw bytes use UTF-8 or base64 under `FULL_PAYLOAD`.
  Kafka Allure examples show redacted JSON bodies for success and expected timeout.
- RabbitMQ exchange observation through optional, lazily imported `aio-pika`:
  a dedicated exclusive auto-delete queue per watcher, confirmed readiness,
  bounded prefetch/handoff, and acknowledgement after committed Journal
  admission, filtering, or an explicit DROP outcome. Existing application queues
  remain independent; latest-only sessions fail closed without reconnect.
- RabbitMQ publisher/admin harness with broker confirmations and owned exchange
  cleanup, reusable contracts with explicit null/duplicate-header preconditions,
  sync/async pytest examples, and redacted JSON Allure success/timeout scenarios.
- An optional RabbitMQ 4.4.0 management Compose profile pinned by image digest,
  local guides, and a CI job with a separate Allure report artifact.
- Specification revision 1.2 documenting the RabbitMQ extension while preserving
  the original Core/Kafka requirements and acceptance identifiers.
- Redis Streams through optional, lazily imported redis-py: independent `XREAD`
  cursors with latest, earliest, and exclusive `StartPosition.after_stream_id(...)`
  starts. Native IDs remain separate from Kafka integer offsets and timestamps.
- Read-only atomic metadata/read batches and conservative failure on new
  deletion/trim evidence or metadata rewind, without consumer groups,
  acknowledgements, application-stream writes, or retries after known errors.
  Metadata-equivalent delete/recreate between checks is not guaranteed detectable.
- A Redis Streams publisher/admin harness with owned typed-stream cleanup,
  explicit null-header capability preconditions, sync/async pytest examples,
  and redacted JSON Allure success/timeout scenarios.
- An authenticated Redis 8.2.11 Compose profile pinned by digest, with append-only
  storage, local guides, and a `redis-streams` CI job with a separate Allure artifact.
- Specification revision 1.3 documenting the Redis Streams extension while
  preserving existing Core, Kafka, RabbitMQ, and acceptance contracts.
- Reusable `ObservationMark` boundaries: capture `watcher.mark()` or
  `scope.mark()` before an action and pass `since=mark` to any sync/async wait.
  Marks preserve parent filters and intersect their lower boundary without
  changing scopes, broker cursors, retention guarantees, or wait deadlines.
  Their timestamps are informational; matching uses Journal admission order.
- Marked pytest/Allure examples for Kafka, RabbitMQ, and Redis Streams, an
  effective Journal lower bound in text diagnostics, and specification revision 1.4.
- Sphinx docstrings for all named source definitions and `:ivar:` attribute
  documentation, with a dependency-free source audit in CI.
- Runtime hardening after the SOLID/DRY/KISS review: actual cancellation
  acknowledgement before accepting another sync command, preserved primary
  shutdown failures, and retained Kafka native exception causes.
- Shared diagnostic copying/clock dependencies, sample-retention and reporter
  error policies, and bounded report trimming without per-item full reserialization.
- Separate blocking thread and async runtimes sharing scheduler-independent
  expectation, admission, retention, and reporting rules. The sync path creates
  no asyncio loop; built-in transports expose explicit blocking providers.
- Blocking Kafka, Redis Streams, and Pika RabbitMQ sessions, blocking transport
  contract suites, explicit `event_observer.aio` imports, and migration guidance
  for async-only custom adapters. Specification revision 1.5 records the change.

Temporal assertions, further brokers, Schema Registry, and checkpoint reconnect
are deferred. Verified results and remaining release gates are in
[docs/status.md](docs/status.md); verified dependency combinations are in
[docs/compatibility.md](docs/compatibility.md).
