# Adapter verification

## Unit coverage

The minimum unit coverage is **80%** of the entire `src/event_observer` package,
including native adapters and public testing helpers. The configuration in
`pyproject.toml` enables branch measurement and applies the threshold to
Coverage.py's combined executable-statement and branch percentage. Reports
also expose line and branch coverage separately. No project-specific source
omissions or coverage exclusions are configured.

Run the broker-free suite and enforce the same threshold as CI:

```shell
uv run --locked coverage run -m pytest tests -m 'not integration'
uv run --locked coverage report
```

Start measurement with `coverage run`, before pytest loads entrypoints, to
include plugin and fixture registration as well as early Core imports through
the package's public namespace. Starting instrumentation afterward with
`pytest --cov` can miss startup execution and emit `module-not-measured`.
The constructor fixtures defer broker adapter imports until requested; native
clients remain lazy until observation starts. Coverage flags are deliberately
absent from global pytest options, preserving plain pytest-only installations.

The measurement is fresh on each run. It includes unit/native-double contracts
in `tests`, while integration-marked nodes and runnable examples execute
separately. No broker or example coverage is combined into the unit result.
The thread-based sync runtime is included in the measurement.

Generate detailed local reports:

```shell
uv run --locked coverage html
uv run --locked coverage xml
uv run --locked coverage json
```

Open `artifacts/coverage/html/index.html` to inspect uncovered lines and branches.
XML and JSON are written alongside the HTML directory. Generated reports are
ignored by Git. Each Python 3.11–3.14 Core CI job enforces the threshold and
uploads `unit-coverage-python-<version>` for seven days. CI exports diagnostics
even when tests or the coverage check fail; the independent threshold check
still fails the job below 80%.

## Transport contracts

Core does not depend on pytest. Public testing helpers are enabled explicitly:

```python
from event_observer.testing.contracts import (
    IsolationContract,
    LifecycleContract,
    ReadinessContract,
)


class TestMyAdapter(LifecycleContract, ReadinessContract, IsolationContract):
    pass
```

The adapter project provides an async `adapter_harness` fixture that follows the
`event_observer.testing.harness.AdapterTestHarness` Protocol. Each contract
method is a separate pytest node. The fixture creates an independent publisher
and cleans up only its own sources in `finally`. No connections are created
during collection. Each session provides one receive iterator for its entire
lifetime.

Blocking adapters have separate `SyncLifecycleContract`,
`SyncReadinessContract`, and `SyncIsolationContract` mixins in
`event_observer.testing.contracts.sync`. Supply a synchronous
`sync_adapter_harness` fixture with blocking `create_source`, `publish`, and
`cleanup` operations plus an `adapter` implementing `SyncTransportAdapter`.
The contracts call blocking sessions directly; they do not create an asyncio
runtime for observation. The repository's native fixture uses short test-side
loops only for independent publisher/admin clients.

Run the same blocking contract suite against all configured brokers:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ \
RABBITMQ_MANAGEMENT_URL=http://127.0.0.1:15672 \
REDIS_URL=redis://:observer@127.0.0.1:6379/0 \
uv run pytest tests/integration/test_sync_contracts.py -v
```

`RABBITMQ_MANAGEMENT_URL` enables one additional fault-injection test. It
identifies the reader's connection through its owned queue and unique consumer
tag, closes only that connection through the management API, and checks that
idle connection failure is detected without a business message.
AMQP credentials are reused unless management URL credentials are
provided. The test explicitly skips without this endpoint; ordinary transport
contracts need no management API. A supplied but failing endpoint fails the test.

`FakeTransport` supports a READY barrier, immediate delivery, controlled
failure/end, bounded handoff, and stalled close. Publishing from a sync test
thread is supported: async readers receive at most one pending wakeup per
session, while blocking readers use a condition variable. The fake's own history
is used to replay test scenarios; it is not a
production broker or a guarantee of bounded memory for the test publisher.

Use `KafkaAdapterHarness` to verify an existing Kafka broker:

```sh
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest tests/integration/test_kafka*.py -v
```

The base contracts verify READY, the absence of hidden commits/delivery
competition within the declared capabilities, an empty source, history replay,
null/empty payload, headers/key, independent readers, and lifecycle. Separate
Kafka integration tests verify explicit offsets, timestamp fallback,
partition expansion, and an open transaction across a latest boundary.

`ContractPrecondition` records applicability for optional semantics. In
particular, `supports_null_payload`, `supports_duplicate_headers`, and
`supports_null_header_values` distinguish
transport limitations from data loss in accepted events. Replay and message
keys are also checked only when claimed. A missing capability records
`NOT_APPLICABLE`; a claimed capability must pass its contract. Readiness,
independent observation, and owned-resource cleanup remain mandatory.

The harness creates unique topics and deletes only names it has registered.
A producer receipt means delivery acknowledgement, rather than placement in
the producer's local queue. Failure to connect to an explicitly specified
broker is a failure. Without the environment variable, real integration nodes
have an explicit skip.

For RabbitMQ, use `RabbitMQAdapterHarness` with an explicit `RABBITMQ_URL`:

```shell
docker compose --profile rabbitmq up --detach --wait --wait-timeout 120 rabbitmq
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest tests/integration/test_rabbitmq_contracts.py tests/integration/test_rabbitmq_observer.py -v
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest -c pyproject.toml examples/pytest/test_rabbitmq.py -m integration -v
docker compose stop rabbitmq
```

The harness creates unique topic exchanges and deletes only its own exchanges.
Every operation uses an independent native connection, allowing separate
`asyncio.run(...)` calls in sync tests and direct awaits in async tests.
Publication waits for a broker publisher confirmation. Each watcher binds its
own exclusive auto-delete queue, confirms readiness before publication, and
acknowledges committed Journal outcomes. It never consumes an application's
queue or creates the application's source exchange.

RabbitMQ explicitly does not claim earliest replay, null payloads, duplicate
header names, or message keys. The corresponding shared contract nodes record
those preconditions as `NOT_APPLICABLE`. Unique byte/null headers and empty
bytes remain supported. Separate observer tests check sync/async Protobuf,
custom routing bindings, and application-queue independence. Native doubles in
`tests/test_rabbitmq.py` and harness tests cover settlement, buffers, failures,
and cleanup without claiming to replace real broker checks. Missing
`RABBITMQ_URL` skips real nodes; a provided but unreachable broker fails.
See [rabbitmq.md](rabbitmq.md), [local-rabbitmq.md](local-rabbitmq.md), and the
[Allure scenarios](../examples/allure/README.md#rabbitmq-and-allurereporter).

For Redis Streams, use `RedisStreamsAdapterHarness` with explicit `REDIS_URL`:

```shell
docker compose --profile redis up --detach --wait --wait-timeout 120 redis
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest tests/integration/test_redis_streams_contracts.py tests/integration/test_redis_streams_observer.py -v
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest -c pyproject.toml examples/pytest/test_redis_streams.py -m integration -v
docker compose stop redis
```

The harness creates unique typed empty streams through an owned `XADD`/`XDEL`
seed, publishes through an independent native client, and deletes only owned
keys. A receipt includes the server-generated `redis.stream_id`. Each call
closes its client, supporting separate `asyncio.run(...)` calls in sync fixtures
and direct awaits in async tests. The observer never performs those writes.

Earliest replay remains applicable. Null payload, duplicate/null headers, and
key contract nodes record their unsupported capability preconditions. The
combined duplicate/null header node checks `supports_headers`,
`supports_duplicate_headers`, and `supports_null_header_values` separately;
existing Kafka and RabbitMQ behavior is unchanged. Dedicated observer tests
cover sync/async Protobuf, header correlation, existing group/pending-entry
independence, latest after deletion of the last entry, exclusive stream-ID
starts, and conservative active deletion/trim failure.

Future native start IDs are allowed; availability tests distinguish them from
starts below known baseline deletion evidence, which must raise
`StartPositionUnavailableError`. New source deletion/trim or metadata rewind
is terminal even when affected entries were read already. Tests must not claim
that exact metadata-equivalent recreation between checks is detectable.
Native identity regressions must cover replacement connections and server
restart with a reused client ID: `CLIENT ID` plus `INFO server` must fail before
candidate handoff when the connection ID or server `run_id` changes.
Native doubles in `tests/test_redis_streams.py` and
`tests/test_redis_streams_harness.py` remain separate from real-broker evidence.
Missing `REDIS_URL` skips real nodes; a provided but unreachable Redis fails.
See [the adapter guide](redis-streams.md), [local profile](local-redis.md), and
[Allure scenarios](../examples/allure/README.md#redis-streams-and-allurereporter).

Native-client doubles verify rare interleavings and errors without a broker.
They do not replace Fetch/ACL/transaction tests on real Kafka. Separate fault
extension Protocols do not contain silently succeeding no-op implementations.

Core timing tests use an injected clock, journal versions, and barriers;
the deadline boundary is checked before, exactly at, and after the admission
timestamp. Separate checks cover lost wakeup, error-before-commit, 100 waiters
with cancellations, conservative loss compaction, process/thread/loop affinity,
and repeated sync lifecycle. Callbacks run in the runtime, rather than the
sync test thread.

Full broker faults, Toxiproxy, and benchmarks are listed as remaining gates in
[status.md](status.md); CI configuration is not evidence that they have run.
