# Pytest usage examples

Run all commands below from the repository root. Prepare the development
environment for the complete set of examples:

```shell
uv sync --all-groups --all-extras
uv run pytest -c pyproject.toml examples/pytest -m 'not integration' -v
```

Nine scenarios use `FakeTransport` and run without a broker. It simulates events
from the service under test; your service or a separate producer performs real
publication. These examples demonstrate the observation API; separate integration
examples use real Kafka and RabbitMQ brokers.

See [../allure](../allure/README.md) for examples with `AllureReporter`, diagnostic
JSON attachments, and commands to generate and view HTML reports.

| File | Demonstrates |
| --- | --- |
| [conftest.py](conftest.py) | Function-scoped factory fixtures, a yield fixture for an active watcher, and a simulated service action. |
| [test_sync.py](test_sync.py) | Ordinary `def test_*` functions, header correlation, an envelope with key/headers, and `pytest.mark.parametrize`. |
| [test_async.py](test_async.py) | `pytest.mark.asyncio`, `async with`, parallel waiters, and reading history with `wait_for_count`. |
| [test_protobuf.py](test_protobuf.py) | `message_type`, `field_equals`, an explicit resolver for mixed types, and the `on_type` guard. |
| [test_kafka.py](test_kafka.py) | A real broker, a unique topic, an independent producer, and cleanup through a yield fixture. |
| [test_rabbitmq.py](test_rabbitmq.py) | Sync and async observation of an exchange through dedicated observer queues, header correlation, and publisher confirmation. |

## Ordinary synchronous tests

Only pytest and the base library are needed to run the synchronous raw examples;
`pytest-asyncio`, Kafka, RabbitMQ, and Protobuf are optional:

```shell
python -m pip install -e '.[pytest]'
python -m pytest examples/pytest/test_sync.py -v
```

The `sync_events` fixture in `conftest.py` creates a lazy factory. The test opens
observation with `with sync_events.watch(...)`. Entering the context waits for
READY, so the service action belongs inside `with`, after creating the scope.

The `watched_events` fixture shows another approach: it opens `watch()` before
the test, passes the watcher through `yield`, and closes the context during
teardown, including after a failed assertion. The parametrized test gets an
independent transport, watcher, and history for each parameter value.

`perform_operation` is an ordinary callback fixture called by the test. Replace
it with your service client in your own project. Library matchers and decoders
should remain short callbacks without I/O.

## Asynchronous tests

Install the `pytest-asyncio` extra:

```shell
python -m pip install -e '.[pytest-asyncio]'
python -m pytest -c pyproject.toml examples/pytest/test_async.py -v
```

An ordinary `pytest.fixture` also provides the async factory: resources open in
`async with` inside the test. Parallel expectations use `asyncio.create_task`
and finish through `asyncio.gather`. They can observe the same record; a matched
event is not removed from the Journal.

The shared `pytest.ini` in this directory contains only pytest options, so the
raw examples also work without an async plugin. Async and full-suite commands
explicitly select `pyproject.toml`, which configures strict mode and function
loop scope. Configure pytest-asyncio accordingly when copying these examples
to your own project.

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
`harness.publish(...)` and explicitly passes a `KafkaAdapter` with `KafkaConfig`
to `SyncEventObserver`. See the [Kafka guide](../../docs/kafka.md) for TLS/SASL
configuration and replay policy.

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
without pytest-asyncio. The fixture creates a unique topic exchange and cleans
up only that exchange. The harness opens independent connections per call, so
the sync example can use `asyncio.run(...)`; the async example awaits publication
directly without reusing a client tied to an earlier loop.

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
Use `RabbitMQAdapter(RabbitMQConfig(url=...))` in your own factory and replace
`harness.publish(...)` with the application's action.
See the [adapter guide](../../docs/rabbitmq.md) and
[local broker guide](../../docs/local-rabbitmq.md) for details.

Create the scope before the action. `ScopeStart.from_now()` captures the next
admission position in the runtime rather than the broker publication time.
`wait_for_count(..., 2)` counts distinct Journal records even when their payloads
are identical. Sequential `wait_for` calls do not check event ordering by themselves.
