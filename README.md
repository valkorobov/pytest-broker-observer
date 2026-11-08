# pytest-broker-observer

A library for black-box assertions on broker events in synchronous and asynchronous
Python tests. The observer connects **before the test action**, retains messages
in a bounded Journal, and lets tests wait for a result without writing a consumer loop.

The distribution is named `pytest-broker-observer`; its import namespace is
`event_observer`, as specified in [SPEC.md](SPEC.md). The current version is a
local MVP candidate, 0.1.0. Verified checks and remaining release gates are listed
in the [implementation status](docs/status.md) and [compatibility matrix](docs/compatibility.md).

## Install from this repository

Python 3.11–3.14 is required. For Kafka, Protobuf, and synchronous pytest:

```shell
python -m pip install -e '.[kafka,protobuf,pytest]'
```

For development with all optional dependencies and tools:

```shell
uv sync --all-groups --all-extras
```

Core has no runtime dependencies and works outside pytest. Extras enable the
implemented features: `kafka`, `rabbitmq`, `redis`, `protobuf`, `pytest`, `pytest-asyncio`, `allure`,
`contract-tests`, and `all`. The pytest plugin registers lazy constructor fixtures;
autoloading it creates no transports, observers, connections, or background
threads. Synchronous tests do not need `pytest-asyncio`. See
[pytest factory fixtures](#pytest-factory-fixtures) for project setup.

## Synchronous example without a broker

After installing the local package, this example demonstrates the blocking API:

```python
from event_observer import SyncEventObserver
from event_observer.testing import FakeTransport

transport = FakeTransport()
events = SyncEventObserver(transport=transport)

with events.watch(source="events") as watcher:
    transport.publish(b"operation-completed")
    result = watcher.wait_for(
        lambda payload: payload == b"operation-completed",
        timeout=1.0,
    )
    assert result == b"operation-completed"
```

`FakeTransport.publish()` is a test helper. The library's production API observes
events; your system or a separate test producer publishes them.

## Asynchronous example

```python
import asyncio

from event_observer import EventObserver
from event_observer.testing import FakeTransport


async def main():
    transport = FakeTransport()
    async with EventObserver(transport=transport).watch(source="events") as watcher:
        transport.publish(b"operation-completed")
        observed = await watcher.wait_for_event(
            lambda payload: payload == b"operation-completed",
            timeout=1.0,
        )
        assert observed.payload == b"operation-completed"
        assert observed.raw.source == "events"
        print(observed.sequence)


asyncio.run(main())
```

The two backends share models and expectation rules. In synchronous mode, a
dedicated observation thread keeps reading messages while the test performs a
blocking service action; it creates no asyncio event loop. The explicit import
paths are `event_observer.sync` and `event_observer.aio`. See
[sync/async execution and adapter migration](docs/sync-async.md).

## Mark before an action and wait later

An active watcher can capture a reusable Journal boundary before an application
action. Pass it to a later wait to exclude earlier admitted history, including
when the new matching event arrived before the wait started:

```python
from event_observer import ObservationMark
from event_observer.matchers import header_equals

operation = watcher.scope(header_equals("operation-id", operation_id.encode()))
mark: ObservationMark = watcher.mark()
service_client.perform(operation_id=operation_id)
completed = operation.wait_for(lambda payload: payload == b"completed", since=mark, timeout=5.0)
```

`since` is available on `wait_for`, `wait_for_event`, and `wait_for_count`, for
watchers and scopes. A scope keeps its filters and uses the later of its own
lower boundary and the mark's sequence. The mark is immutable and local to one
watcher; it does not pin history, create a consumer, or change broker positions.
Async `watcher.mark()` also has no `await`; await only the expectation.
The timeout begins with the wait call, rather than mark capture.

This boundary uses Journal admission, not UTC or broker publication timestamps.
An earlier publication buffered outside the Journal may be admitted after the
mark, so retain correlation. Lost required history still raises
`IncompleteObservationError` by default; `require_complete=False` is explicit.
See [observation marks](docs/scopes.md#reusable-observation-marks) and the
[runnable sync/async examples](examples/pytest/README.md#mark-before-the-action-and-wait-later).
The Kafka, RabbitMQ, and Redis Streams correlation examples also capture a
mark on the operation scope before publication and pass it to the expectation;
their transport start positions and independent readers stay unchanged.

## Kafka, Protobuf, and correlation

Runnable pytest examples with regular and yield fixtures, parametrization,
synchronous/asynchronous waits, and Protobuf are in
[examples/pytest](examples/pytest/README.md). Run them without a broker from the
repository root:

```shell
uv run pytest -c pyproject.toml examples/pytest -m 'not integration' -v
```

Create the watcher before calling the system under test. Entering the context
waits for readiness; by default, reading starts at captured `latest` boundaries.

```python
from uuid import uuid4

from event_observer import ScopeStart, SyncEventObserver
from event_observer.matchers import field_equals
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig
from example_pb2 import OperationEvent, COMPLETED


def test_operation(service_client):
    operation_id = uuid4().hex
    events = SyncEventObserver(
        transport=KafkaAdapter(KafkaConfig(bootstrap_servers="localhost:9092")),
    )
    with events.watch(
        source="example.events",
        message_type=OperationEvent,
    ) as watcher:
        operation = watcher.scope(
            field_equals("operation_id", operation_id),
            start=ScopeStart.from_now(),
        )
        service_client.perform(operation_id=operation_id)
        completed = operation.wait_for(
            field_equals("status", COMPLETED),
            timeout=10.0,
        )
        assert completed.operation_id == operation_id
```

`service_client` is a fixture from your project. Your Protobuf toolchain generates
`example_pb2`; the public example schema is in
[examples/proto/example.proto](examples/proto/example.proto).
The observer does not infer a message type from its wire format. Mixed-type
streams use explicit header/source/callable resolvers and `on_type(T, matcher)`.

## RabbitMQ exchange observation

Install `.[rabbitmq,pytest]` to use `RabbitMQAdapter` through the optional
`aio-pika` client. Each watcher binds its own exclusive auto-delete queue to an
existing exchange, leaving the application's queues independent:

```python
from uuid import uuid4

from event_observer import SyncEventObserver
from event_observer.matchers import header_equals
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig


def test_operation(service_client):
    operation_id = uuid4().hex.encode()
    events = SyncEventObserver(
        transport=RabbitMQAdapter(RabbitMQConfig(url="amqp://observer:observer@127.0.0.1:5672/"))
    )
    with events.watch(source="application.events") as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id))
        service_client.perform(operation_id=operation_id.decode())
        assert operation.wait_for(lambda payload: payload == b"completed", timeout=10) == b"completed"
```

Your application must publish the expected body and header to the existing
`application.events` exchange. Entering `watch()` confirms the observer queue,
bindings, and consumer are ready before that action. The default binding is
`#`; configure `binding_keys` for narrower routing. RabbitMQ supports latest
observation only, without broker replay or reconnect. Messages are acknowledged
when admitted to the Journal, independently of matcher outcomes.
See the [adapter guide](docs/rabbitmq.md), [local Compose guide](docs/local-rabbitmq.md),
and [sync/async examples](examples/pytest/test_rabbitmq.py).

## Redis Streams observation

Install `.[redis,pytest]` to observe an existing Redis stream through the
optional redis-py client. Each watcher has its own `XREAD` cursor; observing
does not join a consumer group, acknowledge pending entries, or write to the
application stream:

```python
from event_observer import StartPosition, SyncEventObserver
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig

events = SyncEventObserver(
    transport=RedisStreamsAdapter(RedisStreamsConfig(url="redis://:observer@127.0.0.1:6379/0"))
)

with events.watch(source="application.events", start=StartPosition.latest()) as watcher:
    # Call your application here; its XADD entry uses the payload field for the body.
    completed = watcher.wait_for_event(lambda payload: payload == b"completed", timeout=10)
    assert completed.payload == b"completed"
    stream_id = completed.raw.metadata["redis.stream_id"]
```

The default field mapping uses `payload` for the body and `header:<name>` for
byte-valued headers. Starts support latest, available earliest history, and
exclusive `StartPosition.after_stream_id("milliseconds-sequence")` boundaries.
A native stream ID remains separate from Kafka integer offsets and timestamps.
Redis 7 or later and read/transaction permissions are required, including
`XINFO STREAM`, `XREAD`, `CLIENT ID`, `INFO server`, `MULTI`, and `EXEC`. Atomic
transactions keep each batch consistent with its continuity and connection
identity metadata, without Lua or `EVAL`.

New deletion/trim evidence or metadata rewind fails conservatively, including
deletion of entries already read. The observer does not reopen at another
cursor after a known failure. Metadata-equivalent deletion and recreation
entirely between checks is not guaranteed detectable; Cluster and Sentinel
support are outside the declared scope. A replacement connection or server
identity ends the observation before its entries reach Core, even if the native
client opened that socket internally. See the
[adapter guide](docs/redis-streams.md), [local profile](docs/local-redis.md),
and [sync/async examples](examples/pytest/test_redis_streams.py).

## Pytest factory fixtures

Installing the package makes these session-scoped fixtures available automatically:

| Fixture | Returns the constructor |
| --- | --- |
| `kafka_transport_factory` | `KafkaAdapter(config: KafkaConfig)` |
| `rabbitmq_transport_factory` | `RabbitMQAdapter(config: RabbitMQConfig)` |
| `redis_streams_transport_factory` | `RedisStreamsAdapter(config: RedisStreamsConfig)` |
| `sync_observer_factory` | `SyncEventObserver(transport=..., ...)` |
| `async_observer_factory` | `EventObserver(transport=..., ...)` |

The factories are the existing classes, not wrappers. Their signatures,
validation, and optional-dependency behavior stay unchanged. Use them to define
your own fixtures, choosing configuration and scope in the test project:

```python
# conftest.py
import pytest

from event_observer.transports.kafka import KafkaConfig


@pytest.fixture(scope="session")
def kafka_transport(kafka_transport_factory):
    return kafka_transport_factory(KafkaConfig(bootstrap_servers="localhost:9092"))


@pytest.fixture
def events(sync_observer_factory, kafka_transport):
    return sync_observer_factory(transport=kafka_transport)
```

```python
def test_operation(events, service_client):
    with events.watch(source="operations") as watcher:
        service_client.perform()
        assert watcher.wait_for(lambda payload: payload == b"completed", timeout=5) == b"completed"
```

`service_client` and the source belong to your application; retain operation
correlation on shared streams. Use `async_observer_factory` with `async with`
and awaited waits for async tests. Both observer factories are ordinary sync
fixtures; neither requires pytest-asyncio just to create an observer.

Only entering `watch()` acquires resources. The plugin does not read broker
environment variables, resolve secrets, enable Allure, or set test identity.
Configure those explicitly in your fixtures. The plugin itself loads without
any broker extra; install the relevant extra to open a real observation.
With `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`, load it using `pytest -p event_observer`;
disable it using `pytest -p no:event_observer`.

See [the runnable fixture recipes](examples/pytest/README.md#factory-fixtures-and-project-configuration)
for sync/async, all three brokers, custom transports, and factory overrides.

## Pytest and Allure

[examples/allure](examples/allure/README.md) contains runnable examples with
`AllureReporter`: successful synchronous/asynchronous waits, timeouts, and JSON
payload redaction. After `uv sync --all-groups --all-extras`:

```shell
uv run pytest -c pyproject.toml examples/allure -m 'not integration' -v --alluredir=artifacts/allure-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-results --single-file --output artifacts/allure-report --report-language en
npx --yes allure@3.22.1 open artifacts/allure-report --host 127.0.0.1 --port 5050
```

Generating and viewing reports requires Node.js with npm; Node.js 24 has been
verified. The regular run produces **4 passed, 1 skipped**. The optional
`--allure-demo-failure` flag enables an intentional failure; commands and its
expected exit code are in the [guide](examples/allure/README.md). You can also
open `artifacts/allure-report/index.html` as a standalone file.

For a real broker, see the [Kafka examples with AllureReporter](examples/allure/README.md#kafka-and-allurereporter):
start Kafka through Compose, wait for a correlated event, and inspect received
JSON bodies after redaction alongside timeout diagnostics. The guide covers the
full run and viewing a separate HTML report.
The [RabbitMQ examples with AllureReporter](examples/allure/README.md#rabbitmq-and-allurereporter)
show the same successful correlation and expected-timeout diagnostics through
dedicated observer queues, including redacted JSON bodies and a separate report.
The [Redis Streams Allure examples](examples/allure/README.md#redis-streams-and-allurereporter)
show those outcomes with independent stream reads and redacted message bodies.

## MVP features

- Asynchronous and synchronous APIs: `wait_for`, `wait_for_event`, `wait_for_count`, and nested scopes.
- Reusable `ObservationMark` boundaries with `mark()` and `since` for later
  expectations over already received events.
- Kafka through `confluent-kafka`: independent sessions, manual partition assignment,
  `latest`, `earliest`, explicit offsets, and timestamp start.
- RabbitMQ through `aio-pika`: independent dedicated exchange subscriptions,
  exclusive queues, latest-only observation, and admission-time acknowledgement.
- Redis Streams through redis-py: independent `XREAD`, latest/earliest/exclusive
  native ID starts, read-only observation, and conservative continuity checks.
- Raw bytes, null payloads where supported by the transport, ordinary Protobuf,
  and explicit mixed-type resolution.
- A bounded Journal with `FAIL`, `DROP_OLDEST`, `DROP_NEWEST`, and incomplete-history tracking.
- Structured matchers, predicates, and independent copies of mutable payloads.
- Bounded diagnostics, opt-in Allure, and passive W3C trace-context extraction.
- A fake adapter and reusable contract suites for custom adapters.

Each wait reads history independently and does not consume events.
`wait_for_count(..., N)` returns the first N distinct Journal records under one
shared deadline; broker duplicates remain separate records.

Waits require a complete range by default. Known loss under a DROP policy raises
`IncompleteObservationError`; a terminal background failure raises
`ObserverFailedError`, including on normal context exit.
`ScopeStart.from_now()` explicitly selects a new range after earlier loss.

A timeout limits when an event can be admitted to the Journal. Final processing
of already admitted messages may finish later. User callbacks must be short and
perform no I/O; in synchronous mode, they run in the runtime thread. An active
synchronous handle belongs to the thread that created it and cannot be used
inside a running asynchronous event loop.

The Journal budget accounts for memory using the documented model; it does not
limit process RSS. Kafka partitions have no global order. Sequential `wait_for`
calls do not prove event order. Temporal assertions, further brokers, Schema Registry,
and reconnect with checkpoint recovery are deferred beyond this MVP.

## Guides

- [Local Kafka: Docker Compose, tests, ports, and data](docs/local-kafka.md)
- [Kafka: configuration, readiness, and start positions](docs/kafka.md)
- [Local RabbitMQ: Docker Compose, tests, management UI, and Allure](docs/local-rabbitmq.md)
- [RabbitMQ: exchange subscriptions, readiness, capabilities, and settlement](docs/rabbitmq.md)
- [Local Redis Streams: Docker Compose, tests, authentication, and Allure](docs/local-redis.md)
- [Redis Streams: field mapping, native ID boundaries, and continuity](docs/redis-streams.md)
- [Matchers and Protobuf rules](docs/matchers.md)
- [Scopes, correlation, and completeness](docs/scopes.md)
- [Separate sync/async runtimes and custom adapter migration](docs/sync-async.md)
- [Diagnostics, redaction, Allure, and trace context](docs/diagnostics.md)
- [Run pytest with AllureReporter and view HTML reports](examples/allure/README.md)
- [Compatibility](docs/compatibility.md) and [verified status](docs/status.md)
- [SOLID/DRY/KISS improvement plan](docs/improvement-plan.md) and [follow-up review](docs/solid-dry-kiss-review.md)
- [Technical specification](SPEC.md), [changelog](CHANGELOG.md),
  [contributing](CONTRIBUTING.md), and [security](SECURITY.md)

## Checks and builds

After preparing the environment:

```shell
uv run pytest -m 'not integration'
uv run pytest -n 2 -m 'not integration'
uv run ruff check .
uv run ruff format --check .
uv run pyrefly check
uv run pyright
uv build --no-sources
uv run twine check dist/*
```

Local Kafka requires Docker Compose v2.18+. Start the broker and wait for its
health check:

```shell
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml tests/integration/test_kafka*.py examples/pytest/test_kafka.py -v
```

Compose starts Kafka 3.9.2 in KRaft mode at `127.0.0.1:9092`. Integration tests and
the pytest example create and delete their own unique topics. Stop the broker
while retaining its data with `docker compose down`. Port selection, data reset,
and individual test suites are covered in the [local Kafka guide](docs/local-kafka.md).

Start the optional RabbitMQ profile separately and run its contracts and examples:

```shell
docker compose --profile rabbitmq up --detach --wait --wait-timeout 120 rabbitmq
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest -c pyproject.toml tests/integration/test_rabbitmq*.py examples/pytest/test_rabbitmq.py -v
docker compose stop rabbitmq
```

The profile pins RabbitMQ 4.4.0 with management UI at `127.0.0.1:15672` and AMQP
at `127.0.0.1:5672`. The harness creates unique topic exchanges; watchers create
their own isolated queues. Stopping only RabbitMQ preserves its data and any
running Kafka service. The `rabbitmq` CI job runs the same broker profile and
retains a separate Allure report. Details are in the
[local RabbitMQ guide](docs/local-rabbitmq.md).

Start the optional Redis profile and run its selected contracts and examples:

```shell
docker compose --profile redis up --detach --wait --wait-timeout 120 redis
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest -c pyproject.toml tests/integration/test_redis_streams*.py examples/pytest/test_redis_streams.py -v
docker compose stop redis
```

The profile pins Redis 8.2.11 with authentication, append-only persistence,
and a loopback-only port 6379. The harness owns unique temporary stream keys;
the runtime only reads existing streams. The `redis-streams` CI job uses the
same profile and retains a separate Allure report. Stopping only Redis preserves
other broker services and its named data volume. See the
[local Redis guide](docs/local-redis.md).

Integration tests are skipped when the environment variable is missing. If a
broker is configured, connection errors fail the tests. A command or CI workflow
alone does not establish that an environment has been verified; results are
recorded in the [status document](docs/status.md).

The project is licensed under the [MIT License](LICENSE), copyright 2026
Valerii Korobov. The candidate is not yet published; see the
[publishing guide](docs/publishing.md) and [release gate plan](docs/release-gates-plan.md).
