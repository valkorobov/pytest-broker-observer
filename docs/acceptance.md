# SPEC.md verification

This table links implemented areas to reproducible checks. A row does not
mean that every acceptance criterion in its range has automatically been met:
full release traceability for AC-01–AC-88 and RG-01 remains mandatory before
publication. The current local results are listed in [status.md](status.md).

| AC / area | Repository evidence | Verification boundary |
| --- | --- | --- |
| AC-01–06: lazy factory, optional imports, sync/async, READY | `tests/test_api.py`, `tests/test_observer.py`, `tests/test_contracts.py` | Basic scenarios and import isolation verified locally. |
| AC-07–19: Kafka positions, transactions, topology, isolation | `tests/test_kafka.py`, `tests/integration/` | Native doubles are separate from real-broker nodes; a full Kafka fault profile is still required. |
| AC-20–21: raw null/empty, duplicate/null headers | `tests/test_api.py`, transport contracts | All fields remain detached and immutable. |
| AC-22–27: Protobuf, required fields, mixed types | `tests/test_decoders.py`, `tests/test_protobuf_runtime.py` | Generated wrapper types and synthetic descriptors; a full runtime/gencode version matrix is still required. |
| AC-28–29: mutation isolation | `tests/test_matchers.py`, `tests/test_observer.py`, `tests/test_protobuf_runtime.py` | Predicates, results, snapshots, and branches do not change the canonical payload. |
| AC-30–35: retention, hard limit, loss ledger, budgets | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_settlement.py` | Conservative completeness and a separate discard outcome; RSS benchmarks are still required. |
| AC-36–44: history/count, callbacks, tri-state, presence | `tests/test_observer.py`, `tests/test_matchers.py` | Truth tables, invalid callback results, configuration errors. |
| AC-45–49: immutable scopes, nesting, from-now, snapshots | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_reporting.py` | Scope boundaries do not expand the parent; snapshots do not invoke predicates again. |
| AC-50–53: deadline/commit priority | `tests/test_reliability.py` | Injected clock: before, at, and after the deadline; error-before-commit and callback overrun. |
| AC-54–59: background failure, cancellation, close | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_kafka.py` | Startup rollback, concurrent close, primary error priority, full handoff. Uninterruptible native hangs require a subprocess fault profile. |
| AC-60–68: sync facade | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_kafka.py` | Blocking action collection, value returns, affinity, reentrancy, 100 lifecycle cycles; a full Ctrl-C subprocess profile is still required. |
| AC-69–75: diagnostics/redaction/Allure/trace | `tests/test_diagnostics.py`, `tests/test_matched_diagnostics.py`, `tests/test_reporting.py` | Safe defaults, bounded matched/mismatched content, raw encodings, failed redactor, partial outcomes, reporter thread/context, W3C extraction. A full Allure runner matrix is still required. |
| AC-76–78: adapter contracts/fault applicability | `testing/contracts`, `testing/harness.py`, `testing/kafka.py`, `tests/integration/test_kafka_contracts.py` | Shared nodes apply without Core changes. Toxiproxy routing is still required. |
| AC-79–83: packaging, passive pytest, xdist | `.github/workflows/ci.yml`, `tests/test_api.py`, clean installations of candidate artifacts | Wheel/sdist, `py.typed`, stdlib-only import, and pure-sync pytest verified separately. |
| AC-84: bounded runtime/resources | Journal, bridge, diagnostic/export budgets; reliability suites | Retained library-owned structures are bounded; a full load/RSS benchmark is still required. |
| AC-85–88: artifact integrity, publishing, docs | `pyproject.toml`, `uv.lock`, CI, docs/status.md | Build and metadata checks completed. Name/license/Trusted Publisher/attestations require the owner; no publication has taken place. |

Fake/native-double test results are not presented as evidence of broker
guarantees. An explicit skip when infrastructure is unavailable keeps this
gap visible. The measured execution speed of these tests is not a performance
baseline for the production library.
