# Adapter verification

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

`FakeTransport` supports a READY barrier, immediate delivery, controlled
failure/end, bounded handoff, and stalled close. Publishing from a sync test
thread is supported: at most one pending wakeup per session is sent to the
runtime. The fake's own history is used to replay test scenarios; it is not a
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
particular, `supports_null_payload` and `supports_duplicate_headers` distinguish
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
