# MVP status and publication requirements

This is a working MVP 0.1.0 candidate based on SPEC.md, rather than an
announcement of a published release or one that has passed all release gates.
The specification is maintained in English. Revision 1.5 separates async and
blocking execution, building on reusable observation marks, Kafka, RabbitMQ,
Redis Streams, and policy-controlled message diagnostics.

Implemented features include separate async and blocking thread runtimes sharing
expectation/admission policies, a bounded
Journal and loss ledger, independent expectations/scopes, raw and Protobuf
decoders/resolvers, three-valued matchers, Kafka, RabbitMQ, and Redis Streams adapters, safe diagnostics
and an explicit Allure reporter, a passive pytest plugin, reusable transport
contracts, packaging, and CI.
`ObservationMark` and `since` add per-wait Journal boundaries without changing
scope definitions or broker positions.

The distribution is named `pytest-broker-observer` as requested by the owner;
the `event_observer` import namespace follows the specification.

## Separate execution backends (SPEC 1.5)

The [six-stage migration plan](sync-async-migration-plan.md) is complete.
`event_observer.aio` exposes the async classes; `event_observer.sync` uses a
blocking observation worker with no created or driven asyncio runtime. Existing
top-level imports remain available. Both runtimes share expectation/admission
rules, and all three built-in adapters provide native blocking sessions.
Custom async-only adapters require a blocking implementation or an explicit
`sync_adapter()` provider; see [the migration guide](sync-async.md).

Final local verification on November 2, 2026 used CPython 3.14.7, macOS arm64,
the pinned Kafka 3.9.2, RabbitMQ 4.4.0, and Redis 8.2.11 Compose services,
and Pika 1.4.4 for blocking RabbitMQ:

| Check | Result |
| --- | --- |
| Complete non-integration suite plus pytest/Allure examples | 778 passed, 1 opt-in demonstration skipped, 125 integration nodes deselected |
| Core suite with two xdist workers | 763 passed |
| Async/blocking native contracts, integration tests, pytest and Allure examples | 111 passed, 14 explicit capability skips, 16 non-integration nodes deselected |
| Allure broker artifacts | 6 passed scenarios, 9 distinct wait summaries, 27 JSON attachments with `indent=2`; English text and redacted received bodies verified |
| Pre-commit: Ruff, format, Pyrefly; separate Pyright | Passed; Pyright reported 0 errors and 0 warnings |
| Sphinx source audit | 704 named definitions and 679 explicit attributes documented |
| Wheel/sdist builds and Twine metadata | Passed |
| Clean wheel/sdist without extras | Imports, `py.typed`, and blocking observation passed; wheel observation also forbids asyncio runtime creation |
| Plain pytest without pytest-asyncio or native clients | 1 passed |

Independent review and confirmed fixes are recorded in the
[migration review](sync-async-migration-review.md). Findings included startup
deadline overruns, invalid-session rollback, shutdown publication ordering,
settlement coroutine rejection, cancellation acknowledgement, and primary-error
priority. The final review has no unresolved findings within the implemented
scope. Initial RabbitMQ fault-test failures and their corrections remain in
that record; the final full-broker run passed without automatic retry.

These local checks do not verify the full CI matrix, minimum dependency
versions, production ACL/TLS, all network faults, or load/RSS release gates.

## Observation marks

Call `watcher.mark()` before an application action and pass the result as
`since=mark` to any wait method, including scoped waits. Capture is an ordinary
method in both facades; an async wait is still awaited. The frozen marker
records watcher identity, next Journal sequence, and informational UTC and
monotonic times. Timeout begins with the wait call, and a scoped wait uses the
later of its existing lower boundary and the mark, preserving its filters.

The marker is reusable within one watcher and does not create a scope,
consumer, seek, replay, or report. Capture consumes no scope/waiter/result
quota; subsequent expectations retain normal waiter/result limits without
registering another scope. Snapshots keep
their existing definitions; expectation diagnostics report the effective
lower bound, including `Journal lower bound` in the text explanation.
Markers do not pin history, and strict retention checks remain in force.
Foreign-watcher markers and creation after closure are rejected.

The runnable sync/async examples establish old-event admission without sleeps,
capture a mark, perform the action, and reuse that mark after a new matching
event is already retained. Run the focused usage checks with:

```shell
uv run pytest -c pyproject.toml examples/pytest/test_sync.py examples/pytest/test_async.py -k mark -v
```

The regression scope also includes ownership, lifecycle, parent-boundary
intersection, all wait variants, snapshots, quotas, and retention loss. A mark
selects Journal admission order, rather than broker publication time; delayed
earlier publications can still be admitted afterward. Keep correlation in
place. See [the guide](scopes.md#reusable-observation-marks).

Local marker validation on November 2, 2026 returned 575 passed, 1 skipped,
and 78 deselected without brokers; the core suite passed 560 tests with two
xdist workers. All 11 pytest and Allure examples passed against the running
Compose Kafka, RabbitMQ, and Redis Streams services. Each marked success report
contained one summary, the effective Journal lower bound, formatted JSON, and
a redacted received body. Pre-commit, Pyright, wheel/sdist builds, and Twine
metadata checks passed.

## Reproducible checks

The SOLID/DRY/KISS hardening and Sphinx source documentation were checked on
November 2, 2026: 618 non-integration tests passed, 1 was skipped, and 78 were
deselected; core xdist passed 603. The final broker run passed 71 cases with 7
capability skips. The source audit covers 564 definitions and 539 attributes.
See the [implementation plan](improvement-plan.md) and [follow-up review](solid-dry-kiss-review.md)
for changes, checks, and the first Kafka partition-leader timeout retained in
the verification record.

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

Matched-content diagnostics were checked on November 2, 2026: 384 tests passed
with 1 skipped and 25 integration scenarios deselected; the core suite passed
371 tests with two xdist workers. Both updated Kafka Allure examples passed on
the local Compose broker. Their HTML report contains redacted received bodies,
three distinct expectation summaries, formatted JSON, and English labels.
Pre-commit, Pyright, package builds, and Twine checks passed. Regression coverage
includes metadata-only defaults, failed redactors, Protobuf/raw content,
partial timeout/cancellation, first/last samples, and shared byte budgets.

A separate full Kafka run on November 2 returned 22 passed and 1 failed:
`test_isolation_sessions_both_observe_one_publication` failed during reader
startup with `TransportConnectionError` (`code=None`). Its isolated recheck
passed, but the original failure's cause remains unconfirmed. The two updated
Allure examples do not replace validation of that broader integration suite.

RabbitMQ support was checked on November 2, 2026 with RabbitMQ 4.4.0,
aio-pika 9.6.2, aiormq 6.9.4, and pamqp 3.3.0. The pinned Compose profile
passed its health check. Contracts, observer integration tests, and sync/async
pytest examples returned 17 passed and 4 explicit `NOT_APPLICABLE` skips
(earliest replay, null bodies, duplicate headers, and message keys). Both
RabbitMQ Allure examples passed with redacted bodies and unique expectation
summaries. Broker inspection after teardown found no remaining test queues.

The first integration run exposed pamqp's binary-header requirement; the
harness now encodes bytes as native bytearrays, and the adapter detaches them
back to bytes. A real codec roundtrip covers binary, UTF-8, empty, and null
header values. Flow control was checked with 50 confirmed publications,
prefetch=1, and a one-event handoff. The full non-integration run returned
449 passed, 1 skipped, and 48 deselected; the core xdist run passed 436 tests.
Controlled native adapter and harness checks cover startup failure/cancellation,
acknowledgement order, decode/overflow failures, consumer/connection loss,
independent monitors, deadlines, and owned-resource cleanup. Production ACL/TLS,
network fault, restart, and multi-version broker coverage remain release gates.

Redis Streams support was checked on November 2, 2026 with Redis 8.2.11 and
redis-py 8.1.0. The digest-pinned authenticated Compose profile passed its
health check. Shared contracts, observer integration checks, and pytest
examples returned 25 passed and 3 explicit `NOT_APPLICABLE` skips (null bodies,
duplicate/null header values, and separate message keys). Both Allure examples
passed with redacted bodies, distinct expectation summaries, and English HTML.
Broker inspection after teardown found no remaining test keys.

Real checks covered independent cursors, available-history replay, exclusive
native IDs, a deleted stream tail, binary headers, Protobuf, preservation of an
application consumer group and its pending entries, conservative XDEL/XTRIM
failure, and targeted observer connection loss without a business message.
Controlled-native tests additionally cover atomic read-only snapshots, server
and connection identity changes, cursor availability/future-ID policies,
bounded buffers, validation, cancellation, and cleanup. The final non-broker
run passed 548 tests with 1 skipped and 78 deselected; core xdist passed 535.
Pre-commit, Pyright, wheel/sdist builds, and clean imports remain part of the
candidate checks. Dependency floors, ACL/TLS, cluster/Sentinel, fault/load,
multi-version testing, and metadata-equivalent stream recreation between polls
are not covered by these local results.

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
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest tests/integration/test_kafka*.py -v
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest tests/integration/test_rabbitmq*.py -v
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest tests/integration/test_redis_streams*.py -v
```

Running the Allure examples, the separate demonstration failure, and viewing HTML:
[examples/allure](../examples/allure/README.md).

Without `KAFKA_BOOTSTRAP_SERVERS`, `RABBITMQ_URL`, or `REDIS_URL`, the corresponding real
broker tests explicitly skip. A specified but unreachable broker causes a failure.
These skips do not prove broker compatibility. Native-client unit tests use a controlled implementation
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
