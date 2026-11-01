# Redis Streams adapter

Install the optional Redis client from the local project:

```shell
python -m pip install -e '.[redis]'
```

The public adapter is `event_observer.transports.redis_streams.RedisStreamsAdapter`.
It observes an existing Redis stream through independent reads, without joining
a consumer group. The development environment currently resolves redis-py 8.1.0;
dependency minima remain subject to the project's separate compatibility gates.
See the [local Compose guide](local-redis.md),
[pytest examples](../examples/pytest/test_redis_streams.py), and
[Allure examples](../examples/allure/README.md#redis-streams-and-allurereporter).

```python
from event_observer import ScopeStart, SyncEventObserver
from event_observer.matchers import header_equals
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig

adapter = RedisStreamsAdapter(
    RedisStreamsConfig(
        url="redis://:observer@127.0.0.1:6379/0",
        payload_field="payload",
        header_prefix="header:",
        read_count=100,
        poll_interval=0.05,
        socket_timeout=1,
    )
)
events = SyncEventObserver(transport=adapter)

with events.watch(source="application.events") as watcher:
    operation = watcher.scope(
        header_equals("operation-id", b"operation-42"),
        start=ScopeStart.from_now(),
    )
    # Invoke the application here, after the stream and latest boundary are READY.
    completed = operation.wait_for(lambda payload: payload == b"completed", timeout=5.0)
```

The application must publish a `payload` field and, for this correlation,
a `header:operation-id` field. Replace the comment with the application action.
The production adapter does not create streams or publish entries. For async
tests, use `EventObserver`, `async with`, and `await operation.wait_for(...)`.

## Source format and configuration

`source` names an existing key of type stream. An existing empty stream is
valid and can reach READY without publishing a probe. A missing key or a key
of another type is a startup failure, rather than an empty stream to create.

`RedisStreamsConfig.url` is required and excluded from `repr`, since a URL can
contain credentials. The library does not read `REDIS_URL` automatically;
fixtures or application configuration supply it explicitly. The remaining
configuration defaults are:

| Setting | Default | Meaning |
| --- | --- | --- |
| `payload_field` | `"payload"` | Native stream field containing the body bytes |
| `header_prefix` | `"header:"` | Native field prefix stripped to produce normalized header names |
| `read_count` | `100` | Requested maximum entries per read batch, subject to transport limits |
| `poll_interval` | `0.05` | Delay between empty read polls, in seconds |
| `socket_timeout` | `1` | Native socket operation timeout, in seconds |

A native entry can contain `payload=b"completed"` and
`header:operation-id=b"operation-42"`. The normalized event has byte payload,
header `("operation-id", b"operation-42")`, source equal to the stream key,
and `key=None`. Its native ID is in `event.raw.metadata["redis.stream_id"]`.
That ID is a stream position, rather than a separate message key.

Empty byte bodies are valid. Null bodies, null header values, duplicate header
names, and a separate message key are unsupported. Redis field values are bytes;
unsupported null values must not be replaced with an empty byte string.
The default field mapping is explicit; the adapter does not guess a JSON schema
or deserialize the body without a decoder.

## Start positions and readiness

```python
from event_observer import StartPosition
```

| Start | Behavior |
| --- | --- |
| `StartPosition.latest()` | Capture the existing stream's last-generated ID once, then read entries with greater IDs |
| `StartPosition.earliest()` | Read available retained history from the beginning, then continue with new entries |
| `StartPosition.after_stream_id("1234-5")` | Read entries strictly after this native ID; the boundary is exclusive |

Native IDs use the full `milliseconds-sequence` form. Numeric Kafka offsets,
timestamp starts, partition selection, and arbitrary cross-broker native
positions are unsupported. The millisecond part of a stream ID does not make
it a timestamp-based selector.

Future native IDs are allowed: observation waits for entries strictly greater
than the requested ID. A request below the stream's baseline
`max-deleted-entry-id` raises `StartPositionUnavailableError`. If baseline
deletion evidence exists, a request below the first retained entry also fails
conservatively; for an empty stream, that bound is `last-generated-id`.
The adapter does not silently move an unavailable requested start.

Latest captures the last-generated ID even if that entry has been deleted or
the typed stream currently has no retained entries. The reader advances its
cursor with IDs it has actually received; it does not send `$` on every poll.
Entering `watch()` confirms that the existing stream and its initial metadata
can be read before the caller performs an action. A reader can therefore
observe an immediate publication after READY.
The resolved initial cursor is `watcher.readiness.source_metadata["redis.start_id"]`.
READY diagnostics retain `requested_stream_id` and `resolved_stream_id` as
cursor metadata, separate from payload content and connection credentials.

Earliest reads available retained history; it does not prove that older entries
were never trimmed before the observation began. Repeated expectations scan
the retained Journal independently and do not remove entries from Redis.

## Independent reads and continuity

The adapter requires Redis 7 or later, including stream metadata needed for
continuity checks. Each watcher owns an independent client and cursor. Reads
use a read-only transaction containing `CLIENT ID`, `INFO server`,
`XINFO STREAM`, and nonblocking `XREAD` so a returned batch, its connection
identity, and its continuity metadata describe a consistent Redis state.
Empty reads are polled at the configured interval, 50 ms by default.
The connection account needs permission for all four commands and `MULTI`/`EXEC`.
No Lua script or `EVAL` permission is required. This adapter observes one stream
key per watcher; it does not claim Redis Cluster or Sentinel support.

The runtime does not use `XREADGROUP`, create or advance a consumer group,
acknowledge pending entries, or write to the application stream. Existing
consumer-group positions and pending-entry lists remain independent of the
observer. Native read batches and the handoff remain subject to configured
transport limits; those limits do not establish a total process RSS bound.

New deletion/trim evidence or a rewind of observed stream metadata terminates
the observation conservatively. A deletion can be fatal even when an affected
entry was already read: the adapter does not silently claim an uninterrupted
range after a source change. Missing/wrong-type sources, known connection
failures, and continuity failures remain visible; the adapter does not retry a
known failure by reopening at a new boundary.

Redis-py's pool can internally open a replacement socket even when retry counts
are zero. The adapter compares both connection ID and server `run_id` from
atomic snapshots before handing entries to Core. Any change is terminal,
including server restart with a reused client ID. Opening a native replacement
socket therefore does not resume the observation across that connection.

These checks do not guarantee detection of an arbitrary `DEL` and identical
recreation completed entirely between checks. No stronger stream-incarnation,
exactly-once, durability, or absence-of-loss guarantee is implied. Use an
application-owned stream whose lifecycle and retention are suitable for the
assertion, and keep those limits separate from Journal retention.

## Harness and integration checks

`RedisStreamsAdapterHarness(url, timeout=15)` creates unique test stream keys.
It uses `XADD` followed by `XDEL` of its seed entry to leave an existing empty
key of type stream. The harness publisher uses `XADD` through an independent
connection and returns the server-generated ID as
`receipt.metadata["redis.stream_id"]`. Cleanup deletes only owned test keys.
The harness requires write/delete permissions for those temporary keys; the
production adapter does not acquire those responsibilities.

Each harness operation opens and closes its own native client, so synchronous
fixtures can call setup, publication, and cleanup with separate
`asyncio.run(...)` calls. Async tests await the same operations directly.
The examples create their scope before calling the independent producer;
replace that publication with your application's action in black-box tests.

```shell
uv sync --all-groups --all-extras
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest tests/integration/test_redis_streams_contracts.py tests/integration/test_redis_streams_observer.py -v
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest -c pyproject.toml examples/pytest/test_redis_streams.py -m integration -v
```

Shared contracts explicitly mark null payload, duplicate/null headers, and key
checks as not applicable, while earliest replay remains applicable. Dedicated
observer scenarios cover sync/async Protobuf, header correlation, exclusive
native ID boundaries, existing groups and pending entries, deleted-tail latest,
and conservative deletion/trim failures. Real nodes explicitly skip only when
`REDIS_URL` is absent; an explicitly configured but unreachable Redis fails.
