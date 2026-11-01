# Pytest usage examples

Run all commands below from the repository root. Prepare the development
environment for the complete set of examples:

```shell
uv sync --all-groups --all-extras
uv run pytest -c pyproject.toml examples/pytest -m 'not integration' -v
```

Nine scenarios use `FakeTransport` and run without a broker. It simulates events
from the service under test; your service or a separate producer performs real
publication. These examples demonstrate the observation API rather than verify
Kafka integration.

See [../allure](../allure/README.md) for examples with `AllureReporter`, diagnostic
JSON attachments, and commands to generate and view HTML reports.

| File | Demonstrates |
| --- | --- |
| [conftest.py](conftest.py) | Function-scoped factory fixtures, a yield fixture for an active watcher, and a simulated service action. |
| [test_sync.py](test_sync.py) | Ordinary `def test_*` functions, header correlation, an envelope with key/headers, and `pytest.mark.parametrize`. |
| [test_async.py](test_async.py) | `pytest.mark.asyncio`, `async with`, parallel waiters, and reading history with `wait_for_count`. |
| [test_protobuf.py](test_protobuf.py) | `message_type`, `field_equals`, an explicit resolver for mixed types, and the `on_type` guard. |
| [test_kafka.py](test_kafka.py) | A real broker, a unique topic, an independent producer, and cleanup through a yield fixture. |

## Ordinary synchronous tests

Only pytest and the base library are needed to run the synchronous raw examples;
`pytest-asyncio`, Kafka, and Protobuf are optional:

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

Create the scope before the action. `ScopeStart.from_now()` captures the next
admission position in the runtime rather than the broker publication time.
`wait_for_count(..., 2)` counts distinct Journal records even when their payloads
are identical. Sequential `wait_for` calls do not check event ordering by themselves.
