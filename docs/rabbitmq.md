# RabbitMQ adapter

Install the optional native clients from the local project:

```shell
python -m pip install -e '.[rabbitmq]'
```

The import namespace is `event_observer`. For a local broker, see the
[Docker Compose guide](local-rabbitmq.md). Runnable [pytest examples](../examples/pytest/test_rabbitmq.py)
and [Allure examples](../examples/allure/README.md#rabbitmq-and-allurereporter)
show publication after readiness and cleanup.

```python
from event_observer import StartPosition
from event_observer.sync import SyncEventObserver
from event_observer.matchers import header_equals
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig

adapter = RabbitMQAdapter(
    RabbitMQConfig(
        url="amqp://observer:observer@127.0.0.1:5672/",
        binding_keys=("operations.#",),
        prefetch_count=100,
    )
)
events = SyncEventObserver(transport=adapter)

with events.watch(source="application.events", start=StartPosition.latest()) as watcher:
    operation = watcher.scope(header_equals("operation-id", b"operation-42"))
    # Invoke the application here, after the observer's queue and binding are READY.
    completed = operation.wait_for(lambda payload: payload == b"completed", timeout=5.0)
```

Replace the comment with your application action. The adapter observes events;
it does not publish them. For async tests, import `EventObserver` from
`event_observer.aio`, use `async with`, and await `operation.wait_for(...)` with
the same matcher and decoder API. Existing top-level imports stay available.

## Explicit native runtimes

The `rabbitmq` extra includes `aio-pika>=9.5,<10` for the async path and
`pika>=1.4.4,<1.5` for blocking observation. The local environment resolves
aio-pika 9.6.2 and Pika 1.4.4; candidate dependency minima and the wider
compatibility matrix still require their separate release checks. The Pika 1.5
API is a separate upgrade rather than an automatically selected backend.

`RabbitMQAdapter.sync_adapter()` lazily returns `SyncRabbitMQAdapter` with the
same immutable `RabbitMQConfig`. `SyncEventObserver` selects this provider;
`EventObserver` retains the asynchronous adapter. You may instead pass the
blocking adapter directly from `event_observer.transports.sync`. Neither
construction nor provider selection opens a connection or imports a native
client.

Async connections belong to the caller event loop. The blocking connection,
channel, consumer, and acknowledgements belong to the watcher actor thread;
it pumps finite Pika I/O calls without creating an asyncio loop. Both paths
share header normalization and preserve the same queue, readiness, and
post-admission settlement rules. User decoders and matchers run on their
respective runtime owner and must remain fast; a blocking callback can also
delay heartbeat processing.

The blocking driver checks the supported Pika 1.4 native timer/abort boundary
needed to bound RPCs and cleanup. An incompatible client fails startup rather
than silently removing those deadlines. A cleanup timeout remains a visible
failure; arbitrary native threads are not forcibly killed. See the
[runtime migration contract](../SPEC.md#s31).

## Sources, ownership, and readiness

The source is an **existing exchange**, rather than an application queue. Each
watcher creates a dedicated exclusive, auto-delete queue and binds it to the
exchange using `binding_keys`. The default `("#",)` observes all routing keys
on a topic exchange. `prefetch_count` defaults to 100.

The observer consumes its own queue, so it does not compete with your
application's consumers for their queue's messages. Each independent watcher
has its own queue and can observe a separate copy of the same publication.
Entering `watch()` waits for that watcher's queue, bindings, and consumer to
be ready. Publish only after entering the context. Closing the watcher releases
its owned queue; startup or terminal failures also close the owned resources.
The observer does not delete the source exchange or application queues.
The runtime verifies the exchange exists; it does not declare or reconfigure
an operator-owned exchange.

The connection account needs read/bind access to the source exchange and
permission to declare, consume, and delete the observer's own queues. It does
not need to consume an application's existing queue. A test harness additionally
needs permission to create/delete its temporary exchanges and publish to them.

`RabbitMQConfig.url` is required and excluded from the configuration's `repr`
because connection URLs can contain credentials. Pass it explicitly from your
test project's configuration. The library does not discover `RABBITMQ_URL` or
other environment variables automatically.

## Latest-only observation and acknowledgement

Only `StartPosition.latest()` is supported. RabbitMQ does not replay earlier
exchange publications to a newly created queue. Earliest, offset, and timestamp
starts are unsupported. The observer does not reconnect after a connection
failure or claim to recover a disconnected interval.

Once a delivered event is admitted to the Journal, the adapter acknowledges it
to RabbitMQ. Acknowledgement therefore does not wait for a matcher to succeed
or for the whole test to pass. A matching or mismatching event already in the
Journal remains available to independent expectations until retention removes
it. Rechecking that retained history is a Core feature, rather than broker
replay. Keep the watcher open while the application action runs and the
expectations complete.

Explicitly filtered or dropped events are acknowledged only after Core records
that outcome. Decode failures, hard oversize, and `FAIL` overflow are not
acknowledged as successful processing; closing the isolated subscription removes
its private queue. Pending delivery count and bytes include messages awaiting
settlement and are bounded by `TransportBufferLimits`. Prefetch limits native
unacknowledged deliveries; it does not cap broker queue storage or native RSS.

Consumer cancellation and channel/connection loss are terminal. The async
client exposes cancellation through its consumer registry, checked every 50 ms,
with connection/channel close callbacks for immediate failure signals. The
blocking path handles native broker-cancellation callbacks and connection/channel
state while pumping finite I/O on its owning actor. Failure inspection does
not wait for free delivery-buffer capacity.

## Payload and metadata

Payloads are bytes, including an empty `b""`. Null payloads are unsupported.
`RawEvent.source` is the exchange name. `RawEvent.key` is always `None`; the
routing key is available as `event.raw.metadata["rabbitmq.routing_key"]`.
Routing keys are not normalized into Kafka-style message keys.

Headers are unique name/value pairs with bytes or null values. Native text values
become UTF-8 bytes, and native bytearrays become detached immutable bytes.
Numeric or nested AMQP header values are unsupported and fail explicitly.
Duplicate header names are unsupported. Use `header_equals("operation-id", operation_id)` for
correlation and a decoder for structured bodies. The adapter does not provide
Kafka offsets, partitions, transactional boundaries, or replay guarantees.

## Test harness and integration checks

`RabbitMQAdapterHarness(url, timeout=15)` creates unique topic exchanges and
removes only exchanges it owns. Its independent publisher waits for broker
publisher confirmation before returning an accepted receipt. Publisher
confirmation describes publication, while the observation's READY barrier
establishes that the observer queue is ready before that publication.

Harness operations open independent native connections per call, so synchronous
tests can use `asyncio.run(...)` for setup, publication, and cleanup without
retaining resources tied to an earlier event loop. Async tests can await the
same operations directly. These explicit runner calls belong to the publisher/
admin harness; `SyncEventObserver` itself uses the blocking native client and
starts no asyncio runtime. Replace harness publication with your application's
action when testing an external service.

```shell
uv sync --all-groups --all-extras
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest tests/integration/test_rabbitmq_contracts.py tests/integration/test_rabbitmq_observer.py -v
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest -c pyproject.toml examples/pytest/test_rabbitmq.py -m integration -v
```

The reusable contracts identify unsupported capabilities explicitly. Additional
observer tests cover sync and async observation, Protobuf, unique headers,
custom routing bindings, and independent application queues.

The examples explicitly skip if `RABBITMQ_URL` is absent. An explicitly specified
but unreachable broker fails instead of silently skipping. Local startup,
integration commands, and report viewing are documented in
[local-rabbitmq.md](local-rabbitmq.md). A full production permissions, TLS,
failure, and broker-version matrix requires separate verification.
