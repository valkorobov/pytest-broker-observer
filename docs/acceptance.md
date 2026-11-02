# SPEC.md verification

This table links implemented areas to reproducible checks. A row does not
mean that every acceptance criterion in its range has automatically been met:
full release traceability for AC-01–AC-88 and RG-01 remains mandatory before
publication. The current local results are listed in [status.md](status.md).

| AC / area | Repository evidence | Verification boundary |
| --- | --- | --- |
| AC-01–06: lazy factory, optional imports, sync/async, READY | `tests/test_api.py`, `tests/test_observer.py`, `tests/test_contracts.py` | Basic scenarios and import isolation verified locally. |
| AC-07–19: Kafka positions, transactions, topology, isolation | `tests/test_kafka.py`, `tests/integration/` | Native doubles are separate from real-broker nodes; a full Kafka fault profile is still required. |
| AC-20–21: raw null/empty, duplicate/null headers | `tests/test_api.py`, transport contracts | Accepted fields remain detached and immutable. Null payload, duplicate-header, and null-header-value contract inputs have explicit capability preconditions; Kafka/Core preservation remains required. |
| AC-22–27: Protobuf, required fields, mixed types | `tests/test_decoders.py`, `tests/test_protobuf_runtime.py` | Generated wrapper types and synthetic descriptors; a full runtime/gencode version matrix is still required. |
| AC-28–29: mutation isolation | `tests/test_matchers.py`, `tests/test_observer.py`, `tests/test_protobuf_runtime.py` | Predicates, results, snapshots, and branches do not change the canonical payload. |
| AC-30–35: retention, hard limit, loss ledger, budgets | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_settlement.py` | Conservative completeness and a separate discard outcome; RSS benchmarks are still required. |
| AC-36–44: history/count, callbacks, tri-state, presence | `tests/test_observer.py`, `tests/test_matchers.py` | Truth tables, invalid callback results, configuration errors. |
| AC-45–49: immutable scopes, nesting, from-now, snapshots | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_reporting.py` | Scope boundaries do not expand the parent; snapshots do not invoke predicates again. |
| AC-50–53: deadline/commit priority | `tests/test_reliability.py` | Injected clock: before, at, and after the deadline; error-before-commit and callback overrun. |
| AC-54–59: background failure, cancellation, close | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_kafka.py` | Startup rollback, concurrent close, primary error priority, full handoff. Uninterruptible native hangs require a subprocess fault profile. |
| AC-60–68: sync facade | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_kafka.py` | Blocking action collection, value returns, affinity, reentrancy, 100 lifecycle cycles; a full Ctrl-C subprocess profile is still required. |
| AC-69–75: diagnostics/redaction/Allure/trace | `tests/test_diagnostics.py`, `tests/test_matched_diagnostics.py`, `tests/test_reporting.py` | Safe defaults, bounded matched/mismatched content, raw encodings, failed redactor, partial outcomes, reporter thread/context, W3C extraction. A full Allure runner matrix is still required. |
| AC-76–78: adapter contracts/fault applicability | `testing/contracts`, `testing/harness.py`, `testing/kafka.py`, `testing/rabbitmq.py`, `testing/redis_streams.py`, `tests/integration/test_kafka_contracts.py`, `tests/integration/test_rabbitmq_contracts.py`, `tests/integration/test_redis_streams_contracts.py` | Shared nodes use declared preconditions, including null payload, duplicate-header, and null-header-value support. A claimed capability cannot be hidden by `NOT_APPLICABLE`. Toxiproxy routing is still required. |
| AC-79–83: packaging, passive pytest, xdist | `.github/workflows/ci.yml`, `tests/test_api.py`, clean installations of candidate artifacts | Wheel/sdist, `py.typed`, stdlib-only import, and pure-sync pytest verified separately. |
| AC-84: bounded runtime/resources | Journal, bridge, diagnostic/export budgets; reliability suites | Retained library-owned structures are bounded; a full load/RSS benchmark is still required. |
| AC-85–88: artifact integrity, publishing, docs | `pyproject.toml`, `uv.lock`, CI, docs/status.md | Build and metadata checks completed. Name/license/Trusted Publisher/attestations require the owner; no publication has taken place. |

Fake/native-double test results are not presented as evidence of broker
guarantees. An explicit skip when infrastructure is unavailable keeps this
gap visible. The measured execution speed of these tests is not a performance
baseline for the production library.

The RabbitMQ extension in [SPEC.md section 28](../SPEC.md#s28) adds the following
evidence without renumbering the original acceptance criteria:

| Extension area | Repository evidence | Verification boundary |
| --- | --- | --- |
| Lazy optional client, configuration, readiness, bounded transfer, settlement, failure, cleanup | `tests/test_rabbitmq.py`, `src/event_observer/transports/rabbitmq.py` | Controlled native clients exercise failure paths; real broker checks remain separate. |
| Reusable transport contracts and declared capability limits | `tests/integration/test_rabbitmq_contracts.py` | Dedicated subscriptions, READY, and cleanup are required; earliest replay, null bodies, duplicate headers, and keys are explicitly not applicable. |
| Sync/async Protobuf, routing bindings, unique headers, application-queue independence | `tests/integration/test_rabbitmq_observer.py`, `tests/test_rabbitmq_harness.py` | Real nodes require an explicitly configured RabbitMQ broker; harness doubles cover independent publication and owned exchange cleanup. |
| Runnable examples, redacted matched/mismatched bodies, local profile, CI | `examples/pytest/test_rabbitmq.py`, `examples/allure/test_rabbitmq.py`, `compose.yaml`, `.github/workflows/ci.yml` | Commands and CI configuration define reproducible runs; confirmed results are recorded separately in [status.md](status.md). |

The Redis Streams extension in [SPEC.md section 29](../SPEC.md#s29) adds separate
evidence while preserving the existing Core/Kafka/RabbitMQ acceptance entries:

| Extension area | Repository evidence | Verification boundary |
| --- | --- | --- |
| Lazy optional client, configuration, typed native IDs, independent reads, bounded handoff, failure, cleanup | `tests/test_redis_streams.py`, `src/event_observer/transports/redis_streams.py`, `src/event_observer/models.py` | Controlled native clients and model checks cover failure paths and connection/server identity changes before handoff; native IDs are exclusive and do not replace Kafka offsets. |
| Reusable contracts and null-header preconditions | `tests/integration/test_redis_streams_contracts.py`, `testing/contracts` | Earliest is applicable; null bodies, duplicate/null headers, and keys are explicitly not applicable. Existing transport preservation remains mandatory. |
| Real stream semantics and independent publisher/admin ownership | `tests/integration/test_redis_streams_observer.py`, `tests/test_redis_streams_harness.py` | Protobuf, headers, group/pending-entry independence, native starts, and active deletion/trim checks require real Redis where applicable. Exact metadata-equivalent recreation detection is not claimed. |
| Redacted Allure content, local authenticated profile, CI | `examples/pytest/test_redis_streams.py`, `examples/allure/test_redis_streams.py`, `compose.yaml`, `.github/workflows/ci.yml` | Commands and CI define reproducible runs; actual versions and verified results are recorded separately. |

The runtime separation in [SPEC.md section 31](../SPEC.md#s31) adds evidence
for the blocking backend without replacing the async checks above:

| Runtime area | Repository evidence | Verification boundary |
| --- | --- | --- |
| Shared admission, expectation state, marks, reservations and deadline ranges | `tests/test_expectation_engine.py`, existing parity/reliability tests | Both schedulers use the same policy implementation. |
| Worker ownership, continuous collection, bounded commands and actual cancellation acknowledgement | `tests/test_sync_runtime.py`, `tests/test_sync_api.py` | Deterministic handoff/interruption regressions; arbitrary uninterruptible callbacks remain subject to the documented abandoned-worker limit. |
| Blocking native sessions, lazy providers and absolute startup budgets | `tests/test_sync_kafka.py`, `tests/test_sync_rabbitmq.py`, `tests/test_sync_redis_streams.py`, `tests/test_transport_startup_deadlines.py` | Controlled clients cover rare failures and Core checks late custom startup/decoding; these do not establish real broker guarantees. |
| Separate blocking transport contracts and native actor regressions | `tests/test_sync_contracts.py`, `tests/integration/test_sync_contracts.py` | Real nodes require configured brokers; RabbitMQ idle connection interruption additionally requires the management endpoint. |
| No asyncio runtime in sync observation, stdlib-only installation | `tests/test_sync_runtime.py`, clean wheel check in CI | Loop creation is forbidden during blocking startup, waits, marks and teardown; async-only third-party adapters require a blocking provider. |
