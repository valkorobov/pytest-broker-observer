# MVP status and publication requirements

This is a working MVP 0.1.0 candidate based on SPEC.md, rather than an
announcement of a published release or one that has passed all release gates.
The normative contract is unchanged; the specification has been translated
into English.

Implemented features include a shared async core and sync facade, a bounded
Journal and loss ledger, independent expectations/scopes, raw and Protobuf
decoders/resolvers, three-valued matchers, a Kafka adapter, safe diagnostics
and an explicit Allure reporter, a passive pytest plugin, reusable transport
contracts, packaging, and CI.

The distribution is named `pytest-broker-observer` as requested by the owner;
the `event_observer` import namespace follows the specification.

## Reproducible checks

Local run on November 1, 2026: CPython 3.14.7, macOS arm64.
Exact dependency versions: [compatibility.json](compatibility.json).

| Check | Result |
| --- | --- |
| Unit/Core/parity/contracts/reliability/reporting | 337 passed |
| pytest-xdist, two processes | 337 passed |
| Ruff lint/format and Pyright | No errors or warnings |
| Wheel + sdist, Twine metadata check | Built, metadata check PASSED |
| Clean wheel installation without extras | Stdlib-only import, `py.typed`, and sync observation passed |
| Clean sdist installation without extras | Import and `py.typed` passed |
| Plain pytest without pytest-asyncio/Kafka/Protobuf/Allure | 1 passed |
| Runnable pytest examples | 9 passed without a broker; separate Kafka example — 1 passed; in a pytest-only environment, 4 passed, 3 skipped |
| Real Kafka 3.9.2 / KRaft / arm64 | 22 passed: shared contracts, replay, topology, transactions, sync/async + Protobuf |
| Local Kafka through Compose | 23 passed including the pytest example; a custom host port and data persistence after `down`/`up` verified |
| Pytest examples with real AllureReporter | 4 passed, 1 skipped; opt-in demonstration — 4 passed, 1 intentional FAILED; JSON attachments and redaction verified |
| HTML Allure 3.22.1 / Node.js 24.19.0 | Regular and demonstration reports built as standalone HTML |
| Kafka examples with AllureReporter / Compose | 2 passed on a real broker; without a Kafka address — 2 skipped |
| Allure report readability | JSON with `indent=2` and limits; titles with transport and topic/stream, aliases, and watcher isolation verified |
| Expectation text explanations | `expectation-result.txt` with outcome reason, timing, and counters; safe export, UTF-8 limits, and text/plain verified |
| Automatic reports without duplicates | New expectations are sent once; errors after success and full manual export are preserved, async/sync and concurrent waiters verified |
| Core with pytest and Allure examples without a broker | 350 passed, 1 skipped, 25 deselected |

The English-language migration was checked on November 2, 2026: examples,
documentation, and report labels use English; Unicode byte-limit coverage is
preserved. Allure generation in local commands and CI selects English explicitly.

The first real-broker run revealed a difference in `get_watermark_offsets`
under `read_committed`: the client returned LSO instead of the physical high
watermark. The fix uses worker-owned AdminClient ListOffsets with
READ_UNCOMMITTED for the boundary while retaining READ_COMMITTED for the
reader. A separate native regression and a repeated full integration run
confirm the fix; the initial failure was not hidden by automatic retry.

These results apply to the local candidate. They do not replace the full CI
matrix, dependency floor verification, or the remaining release gates below.

```sh
uv sync --all-groups --all-extras
uv run pytest -m 'not integration'
uv run pytest -n 2 -m 'not integration'
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv build --no-sources
uv run twine check dist/*
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest tests/integration -v
```

Running the Allure examples, the separate demonstration failure, and viewing HTML:
[examples/allure](../examples/allure/README.md).

Without `KAFKA_BOOTSTRAP_SERVERS`, real Kafka tests have an explicit skip.
A specified but unreachable broker causes a failure. These skips do not prove
Kafka compatibility. Native-client unit tests use a controlled implementation
of its interface and are identified separately from real-broker tests.

## Remaining work before publication

- Owner's choice of license and verification of the PyPI name.
- Verified dependency floors and a full Python/OS/client/broker matrix.
- Full fault suites: ACL/auth in a real environment, advertised endpoints
  through Toxiproxy, broker restart, retention, topic recreation, and compaction.
- A full Testcontainers harness with a pinned digest and mandatory mapping of
  each declared capability to a separate contract node on a real broker.
- RSS/throughput/latency benchmarks with a fixed environment and budgets.
- Remaining Allure reporting contexts: examples verified locally with the real
  pytest plugin and HTML, a separate CI job prepared; the full runner matrix
  still requires confirmation.
- Protected release environment, confirmed OIDC Trusted Publisher,
  attestations, tag/version/hash verification, and publication of verified artifacts.

CI is prepared, but local checks do not replace a successful CI run.
The LICENSE file is intentionally absent pending the owner's decision; public
publication was not performed as part of this task.

Mapping SPEC areas to tests: [acceptance.md](acceptance.md). Extending and running
transport contracts: [testing.md](testing.md).
