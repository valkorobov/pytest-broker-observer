# Changelog

## 0.1.0 — candidate, unpublished

The first MVP based on [SPEC.md](SPEC.md), with distribution name
`pytest-broker-observer` and import namespace `event_observer`.

Added:

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

Temporal assertions, additional brokers, Schema Registry, and checkpoint reconnect
are deferred. Verified results and remaining release gates are in
[docs/status.md](docs/status.md); verified dependency combinations are in
[docs/compatibility.md](docs/compatibility.md).
