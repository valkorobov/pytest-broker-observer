# Pytest usage examples

Run all commands below from the repository root. Prepare the development
environment for the complete set of examples:

```shell
uv sync --all-groups --all-extras
uv run pytest -c pyproject.toml examples/pytest -m 'not integration' -v
```

Eleven scenarios use `FakeTransport` and run without a broker. It simulates events
from the service under test; your service or a separate producer performs real
publication. These examples demonstrate the observation API; separate integration
examples use real Kafka, RabbitMQ, and Redis Streams.

See [../allure](../allure/README.md) for examples with `AllureReporter`, diagnostic
JSON attachments, and commands to generate and view HTML reports.

| File | Demonstrates |
| --- | --- |
| [conftest.py](conftest.py) | Project observer fixtures composed from the plugin's factories, a yield fixture for an active watcher, and a simulated service action. |
| [test_sync.py](test_sync.py) | Ordinary `def test_*` functions, header correlation, reusable marks, an envelope with key/headers, and `pytest.mark.parametrize`. |
| [test_async.py](test_async.py) | `pytest.mark.asyncio`, `async with`, ordinary `mark()` calls, parallel waiters, and reading history with `wait_for_count`. |
| [test_protobuf.py](test_protobuf.py) | `message_type`, `field_equals`, an explicit resolver for mixed types, and the `on_type` guard. |
| [test_kafka.py](test_kafka.py) | Kafka transport/observer fixtures, sync/async observation, a unique topic, an independent producer, and cleanup through a yield fixture. |
| [test_rabbitmq.py](test_rabbitmq.py) | RabbitMQ transport/observer fixtures, sync/async dedicated exchange subscriptions, header correlation, and publisher confirmation. |
| [test_redis_streams.py](test_redis_streams.py) | Redis Streams transport/observer fixtures, sync latest observation, async native-ID boundaries, and independent Redis reads. |

## Factory fixtures and project configuration

The installed `event_observer` pytest plugin automatically provides five
session-scoped constructor fixtures:

| Fixture | Constructor | Configuration |
| --- | --- | --- |
| `kafka_transport_factory` | `KafkaAdapter` | `KafkaConfig` |
| `rabbitmq_transport_factory` | `RabbitMQAdapter` | `RabbitMQConfig` |
| `redis_streams_transport_factory` | `RedisStreamsAdapter` | `RedisStreamsConfig` |
| `sync_observer_factory` | `SyncEventObserver` | `transport=` and the existing policy/reporter keywords |
| `async_observer_factory` | `EventObserver` | `transport=` and the existing policy/reporter keywords |

They return existing classes, not preconfigured objects or shared clients.
Factories have no test-bound state, so session-scoped project fixtures can
depend on them. Each call creates a new object. Adapter and observer creation
perform no broker I/O; resources open only in `with` / `async with watch()`.
The plugin does not provide an `events` fixture or manage an active watcher.

For example, define these fixtures in your project's `conftest.py`:

```python
import os

import pytest

from event_observer import LifecyclePolicy
from event_observer.transports.kafka import KafkaConfig


@pytest.fixture(scope="session")
def kafka_config():
    # This environment variable is a project convention, not plugin discovery.
    return KafkaConfig(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"])


@pytest.fixture(scope="session")
def kafka_transport(kafka_transport_factory, kafka_config):
    return kafka_transport_factory(kafka_config)


@pytest.fixture
def events(sync_observer_factory, kafka_transport):
    return sync_observer_factory(
        transport=kafka_transport,
        lifecycle=LifecyclePolicy(startup_timeout=10),
    )


@pytest.fixture
def async_events(async_observer_factory, kafka_transport):
    return async_observer_factory(transport=kafka_transport)
```

Tests use these objects with `events.watch(...)` or `async_events.watch(...)`,
as the runnable broker examples do. Configuration and credential resolution
remain in the project; no broker settings are inferred from environment names.
The same pattern works with `RabbitMQConfig` and `RedisStreamsConfig` and their
corresponding transport factories. Stateless adapters may be session-scoped,
but active watchers should be function-scoped or owned by a test context.
An active async watcher must use a compatible pytest-asyncio loop scope.

For broker-free tests, `conftest.py` creates a function-scoped `FakeTransport`
directly and passes it to both observer factories. Custom transports need no
plugin registration: pass any supported adapter to the observer factory.
Each parametrized case gets a fresh fake transport and independent history.

For multiple brokers, define separately named transport and observer fixtures.
Policies and reporters are explicit constructor arguments. To reuse project-wide
defaults, a project can override a factory using pytest's parent-fixture lookup:

```python
from functools import partial


@pytest.fixture(scope="session")
def sync_observer_factory(sync_observer_factory):
    return partial(sync_observer_factory, lifecycle=LifecyclePolicy(startup_timeout=10))
```

Call arguments can override those defaults. Factories do not capture a test's
nodeid or enable Allure automatically. Pass `ObservationIdentity` explicitly
to `watch(identity=...)` when needed, and reporters to the observer constructor.

With plugin autoload disabled, run `pytest -p event_observer ...`; to disable
these fixtures, use `pytest -p no:event_observer ...`. Do not also register the
same entry-point module through `pytest_plugins` when autoload is enabled.
All fixtures register without broker extras. Using the real transport requires
its extra; constructor validation and missing-dependency errors are not silently
converted into skips.

## Ordinary synchronous tests

Only pytest and the base library are needed to run the synchronous raw examples;
`pytest-asyncio`, Kafka, RabbitMQ, Redis, and Protobuf are optional:

```shell
python -m pip install -e '.[pytest]'
python -m pytest examples/pytest/test_sync.py -v
```

Outside the plugin, import the blocking factory from `event_observer.sync` or
the asynchronous factory from `event_observer.aio`. Existing top-level imports
remain supported. The blocking observer runs a dedicated actor without
creating an asyncio event loop; it keeps admitting events while the test
thread waits on the application action. Both runtimes share matching,
completeness, deadline, and quota semantics.

The `sync_events` fixture in `conftest.py` calls `sync_observer_factory` to
create a lazy observer. The test opens observation with
`with sync_events.watch(...)`. Entering the context waits for
READY, so the service action belongs inside `with`, after creating the scope.

The `watched_events` fixture shows another approach: it opens `watch()` before
the test, passes the watcher through `yield`, and closes the context during
teardown, including after a failed assertion. The parametrized test gets an
independent transport, watcher, and history for each parameter value.

`perform_operation` is an ordinary callback fixture called by the test. Replace
it with your service client in your own project. Library matchers and decoders
run on the owning actor or async loop and should remain short callbacks
without I/O. Blocking callbacks must not reenter their own watcher.

The built-in broker adapters expose lazy `sync_adapter()` providers, so the
examples keep the same immutable connection configs for both native paths.
Custom async-only adapters require an explicit blocking implementation or
`SyncAdapterProvider` before they can be used by `SyncEventObserver`; no hidden
event-loop fallback is provided. See the
[adapter migration contract](../../SPEC.md#s31).

## Asynchronous tests

Install the `pytest-asyncio` extra:

```shell
python -m pip install -e '.[pytest-asyncio]'
python -m pytest -c pyproject.toml examples/pytest/test_async.py -v
```

An ordinary project `pytest.fixture` calls `async_observer_factory`: resources
open in `async with` inside the test. Parallel expectations use `asyncio.create_task`
and finish through `asyncio.gather`. They can observe the same record; a matched
event is not removed from the Journal.

`EventObserver` uses the caller's async loop; it does not share an active
native session with a blocking watcher. The same admitted history produces
the same assertion outcome through their shared expectation state.

The shared `pytest.ini` in this directory contains only pytest options, so the
raw examples also work without an async plugin. Async and full-suite commands
explicitly select `pyproject.toml`, which configures strict mode and function
loop scope. Configure pytest-asyncio accordingly when copying these examples
to your own project.

## Mark before the action and wait later

The mark examples in `test_sync.py` and `test_async.py` admit an old matching
event before capturing a marker. They then perform the application action,
receive a new matching event, and reuse the marker in a later expectation:

```python
from event_observer import ObservationMark

mark: ObservationMark = watcher.mark()
perform_operation(operation_id)
completed = operation.wait_for(lambda payload: payload == b"completed", since=mark, timeout=1)
```

`operation` is a scope on `watcher` with the operation-id filter. The earlier
matching history is excluded, while a new event already in the Journal when
the wait starts remains eligible. The examples use a completed wait as an
admission barrier, without sleeps. Async `mark()` also has no `await`; only
the wait methods are awaited.

`since` is supported by `wait_for`, `wait_for_event`, and `wait_for_count`, on
watchers and scopes. A scope keeps its parent filters and takes the later of
its lower boundary and the marker sequence. A mark is immutable, reusable,
local to one watcher, and cannot be created after close. It does not create a
consumer or change the broker position.

This is a Journal admission boundary, rather than a UTC or broker-publication
timestamp filter. An earlier publication buffered outside the Journal can
still arrive after the marker, so keep correlation in place. Markers do not
pin history: strict waits raise `IncompleteObservationError` for lost required
records; `require_complete=False` explicitly accepts incomplete retained
history. See [scopes and correlation](../../docs/scopes.md#reusable-observation-marks).

## Protobuf without compiling another schema

```shell
python -m pip install -e '.[pytest,protobuf]'
python -m pytest examples/pytest/test_protobuf.py -v
```

The standalone examples use the generated `StringValue` and `Int32Value` classes
included in Protobuf. For your schema, pass a class from your `*_pb2.py` just as
the example passes `message_type=StringValue`. The public `OperationEvent`
schema is in [../proto/example.proto](../proto/example.proto).

In the mixed stream, a unique `event-type` header selects the type.
`on_type(Int32Value, ...)` skips another type without calling the nested matcher.
The type is not guessed from the binary payload.

## A separate example with real Kafka

For `AllureReporter`, diagnostic attachments, and HTML viewing commands, see
[Kafka and AllureReporter](../allure/README.md#kafka-and-allurereporter).

Start the local Kafka instance from `compose.yaml` and provide its address. The
example creates a unique topic and deletes it in `finally`.

```shell
uv sync --all-groups --all-extras
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml examples/pytest/test_kafka.py -m integration -v
docker compose down
```

For an existing broker with permission to create and delete topics, set its
address in `KAFKA_BOOTSTRAP_SERVERS`. Port selection and resetting local data
are described in [../../docs/local-kafka.md](../../docs/local-kafka.md).

The example is skipped without `KAFKA_BOOTSTRAP_SERVERS`. An explicitly configured
but unavailable broker causes a failure. The environment variable belongs to
the test project: Observer itself does not discover connection settings.

`KafkaAdapterHarness` provides an independent producer and waits for broker
acknowledgement. An ordinary black-box test calls the application instead of
`harness.publish(...)`. The project fixtures explicitly create `KafkaAdapter`
from `KafkaConfig` through `kafka_transport_factory`, then create sync/async
observers through their factories. The provider selects a blocking bridge over the shared
serialized Kafka worker without starting an asyncio loop. See the
[Kafka guide](../../docs/kafka.md) for TLS/SASL
configuration and replay policy.

Both sync and async Kafka scenarios run with `.[kafka,pytest-asyncio]`; the
async scenario skips when pytest-asyncio is absent. Harness calls remain an
independent publisher/admin path rather than supplying the observed adapter.

## Examples with real RabbitMQ

For redacted message bodies, diagnostic attachments, and HTML viewing commands,
see [RabbitMQ and AllureReporter](../allure/README.md#rabbitmq-and-allurereporter).

Start the optional RabbitMQ Compose service and run the sync and async examples:

```shell
uv sync --all-groups --all-extras
docker compose --profile rabbitmq up --detach --wait --wait-timeout 120 rabbitmq
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest -c pyproject.toml examples/pytest/test_rabbitmq.py -m integration -v
docker compose stop rabbitmq
```

Expect **2 passed** with the full development environment. For a separate test
project, install `.[rabbitmq,pytest-asyncio]`; the async example explicitly skips
without pytest-asyncio. The RabbitMQ extra installs both aio-pika for async
observation and Pika 1.4.x for the blocking actor. The fixture creates a unique
topic exchange and cleans
up only that exchange. The harness opens independent connections per call, so
the sync example can use `asyncio.run(...)`; the async example awaits publication
directly without reusing a client tied to an earlier loop. Those explicit
runner calls belong to the publisher/admin harness; the blocking observer
itself uses Pika and creates no asyncio runtime.

The source is an exchange. Each watcher binds a dedicated exclusive auto-delete
queue and never consumes an application's queue. Entering `watch()` confirms
this queue is ready before the action. The examples inspect the routing key in
`event.raw.metadata["rabbitmq.routing_key"]`; `event.raw.key` is `None`.
Messages are acknowledged at Journal admission, independently of matching.
Only latest observation is supported, with no broker replay or reconnect.

For an existing broker, set `RABBITMQ_URL` explicitly and allow exchange
binding/read access plus declaration, consumption, and deletion of observer-owned
queues. The harness also needs to create/delete its temporary exchanges and
publish to them. Without the variable, the examples skip; a specified but
unreachable broker fails. The library does not read this variable itself.
The project's `rabbitmq_transport` fixture uses `rabbitmq_transport_factory`
with `RabbitMQConfig(url=...)`; observer fixtures use the sync/async factories. Replace
`harness.publish(...)` with the application's action.
See the [adapter guide](../../docs/rabbitmq.md) and
[local broker guide](../../docs/local-rabbitmq.md) for details.

## Examples with real Redis Streams

For JSON message bodies after redaction, timeout diagnostics, and HTML viewing,
see [Redis Streams and AllureReporter](../allure/README.md#redis-streams-and-allurereporter).

```shell
uv sync --all-groups --all-extras
docker compose --profile redis up --detach --wait --wait-timeout 120 redis
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest -c pyproject.toml examples/pytest/test_redis_streams.py -m integration -v
docker compose stop redis
```

Expect **2 passed** with the full development environment. A separate project
can install `.[redis,pytest-asyncio]`; the async example explicitly skips without
pytest-asyncio. The sync example publishes after latest is READY. The async
example starts with `StartPosition.after_stream_id(...)`, excludes the entry
at that boundary, and waits for the next event. Native stream-ID starts are
exclusive; `StartPosition.earliest()` can instead read available retained history.
Future native IDs are allowed. An explicit start below known baseline deletion
evidence fails with `StartPositionUnavailableError` rather than moving silently
to a retained position; the adapter guide details the conservative bounds.

The fixture creates a unique existing stream with an `XADD`/`XDEL` seed and
deletes only its own key. Each harness operation has an independent native
client, so separate `asyncio.run(...)` calls in sync fixtures are safe. These
explicit calls belong to the publisher/admin harness; blocking observation
uses `redis.Redis` on its I/O worker, while the async path uses `redis.asyncio`
on the caller loop. Both retain the same atomic cursor/continuity checks. A receipt
contains the server-generated `redis.stream_id`, also exposed in observed
event metadata. The default entry fields are `payload` and
`header:operation-id`; `event.raw.key` is `None`.

The production adapter reads one existing stream with independent `XREAD` and
read-only metadata transactions. It does not create streams, join groups,
acknowledge group deliveries, or change pending-entry lists. It requires Redis
7 or later and permissions for `CLIENT ID`, `INFO server`, `XINFO STREAM`,
`XREAD`, `MULTI`, and `EXEC`, without Lua or `EVAL`. The harness additionally
needs write/delete permissions
for its temporary keys. Null bodies, null/duplicate headers, and message keys
are unsupported.

Detected deletion/trim or metadata rewind fails conservatively; the observer
does not retry at a new boundary. An arbitrary metadata-equivalent delete and
recreation completed between checks cannot be guaranteed detectable. There is
no Cluster or Sentinel support claim. The same atomic snapshots compare
connection ID and server `run_id`; a changed
identity fails before candidate handoff even if the native pool opened a
replacement socket internally. See the
[adapter guide](../../docs/redis-streams.md) and
[local Redis guide](../../docs/local-redis.md) for details.

`REDIS_URL` is read explicitly by the fixture, rather than the library. Missing
configuration skips the examples; a configured but unreachable Redis fails.
Replace `harness.publish(...)` with your application's action when adapting
the examples to a service test.

Create the scope before the action. `ScopeStart.from_now()` captures the next
admission position in the runtime rather than the broker publication time.
`wait_for_count(..., 2)` counts distinct Journal records even when their payloads
are identical. Sequential `wait_for` calls do not check event ordering by themselves.
