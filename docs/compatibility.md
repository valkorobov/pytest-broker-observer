# Candidate 0.1.0 compatibility

The code supports Python 3.11–3.14. Local verification was performed on
CPython 3.14.7, macOS arm64. CI defines a 3.11–3.14 matrix on Linux;
the presence of a workflow does not mean that this matrix has passed.

Application dependencies are optional. The async runtime and blocking actor
use only the stdlib. Extension code imports Kafka, RabbitMQ, Redis, Protobuf,
and Allure lazily.
The pytest plugin registers five session-scoped constructor fixtures without
creating transports, observers, connections, or changing the asyncio mode.
All fixtures register without broker clients or pytest-asyncio; both observer
factories are ordinary synchronous fixtures. Native clients are needed only
when a real watch context is entered. The entry-point module itself remains
stdlib-only. See the [fixture recipes](../examples/pytest/README.md#factory-fixtures-and-project-configuration).
The `rabbitmq` extra declares `aio-pika>=9.5,<10` and `pika>=1.4.4,<1.5`;
the current environment resolves `aio-pika` 9.6.2 with `aiormq` 6.9.4 and
`pamqp` 3.3.0, and Pika 1.4.4 for the blocking path. The 1.5 series is outside
the selected native API range. These optional clients
are not loaded by the base import, adapter construction, or provider selection.
The `redis` extra declares `redis>=7.1,<9`; the current environment resolves
redis-py 8.1.0. That resolution does not verify the candidate minimum version.

The specific development environment is pinned in `uv.lock`. The declared
lower bounds for extras are still candidates: full verification of minimum
versions, all platforms, and generated-code/runtime combinations remains a
release gate. A broad metadata range should not be treated as the result of
such verification.

The async API is explicit in `event_observer.aio`; existing top-level
`EventObserver`, `EventWatcher`, and `EventScope` imports remain aliases to
those same async classes. `event_observer.sync` exposes ordinary blocking
methods backed by a watcher actor without an asyncio loop. Both schedulers
drive shared incremental expectation and admission logic.

Built-in `KafkaAdapter`, `RabbitMQAdapter`, and `RedisStreamsAdapter` expose
ordinary `sync_adapter()` providers. Custom adapters used with
`SyncEventObserver` must implement `SyncTransportAdapter` directly or the
`SyncAdapterProvider` interface; an async-only `open` is rejected with
`ConfigurationError` before connection startup. No private-loop fallback is
provided. The explicit blocking interfaces and adapters are re-exported from
`event_observer.transports.sync`. See [the migration contract](../SPEC.md#s31).

The historical verification dates, client lists, and result counts in
[compatibility.json](compatibility.json) remain scoped to their recorded runs.
Its `runtime_migration` entry describes the selected backend model without
relabeling those historical runs as new verification. Current local versions
and the status of real broker tests are recorded in
[status.md](status.md). Transport-specific conditions are described in
[kafka.md](kafka.md), [rabbitmq.md](rabbitmq.md), and
[redis-streams.md](redis-streams.md). The optional local RabbitMQ
profile pins the RabbitMQ 4.4.0 management image by digest; its configuration
and commands are in [local-rabbitmq.md](local-rabbitmq.md).
The independent Redis profile pins Redis 8.2.11 Alpine by digest, authenticates
the default user with its example password, and stores append-only data in a
named volume. See [local-redis.md](local-redis.md).

RabbitMQ observes an existing exchange through an exclusive auto-delete queue
per watcher. It supports latest-only dedicated subscriptions, bounded prefetch,
and settlement after Journal admission, filtering, or an explicit DROP outcome.
It does not replay, reconnect, consume an application queue, or declare the
application's exchange. Headers support unique byte/null pairs (native string
values normalize to UTF-8); empty byte bodies are valid. Null bodies, duplicate
header names, message keys, Kafka-style positions, and topology tracking are
unsupported. The routing key is separate metadata. Capability-based contract
preconditions record those limits explicitly while preserving Core data rules.

Redis Streams requires Redis 7 or later and read/transaction permissions for
atomic `XINFO STREAM` plus nonblocking `XREAD` batches, with `CLIENT ID` and
`INFO server` identity checks. It uses no Lua, writes,
consumer groups, pending-entry acknowledgement, or automatic retries after
known errors. Each watcher reads one existing stream with an independent cursor;
Cluster and Sentinel support are not claimed. Latest captures last-generated
ID once, earliest reads available retained history, and
`StartPosition.after_stream_id(...)` is an exclusive native-ID boundary.
Future IDs are allowed. Known baseline deletion evidence makes an explicit
start unavailable when it falls below the deletion watermark or conservatively
below retained history; the adapter raises `StartPositionUnavailableError`
rather than silently moving it. Native IDs remain separate from Kafka integer
offsets and timestamp starts.
The native client's pool may internally open a replacement socket; the adapter
compares connection ID and server `run_id` in atomic snapshots and fails before
handoff across any such change. This is detection, rather than recovery.

Redis payloads and unique headers are bytes; empty bodies are valid, while
null bodies, null header values, duplicate headers, keys, timestamps, and
partition selection are unsupported. `supports_null_header_values` defaults
to true for existing transports and is false for Redis Streams. The combined
header contract uses separate headers, duplicate-name, and null-value guards,
without weakening Core, Kafka, or RabbitMQ preservation. New deletion/trim
evidence or metadata rewind terminates observation conservatively, even for
already-read entries. An exact metadata-equivalent delete/recreate entirely
between checks is not guaranteed detectable.

`ObservationMark`, `mark()`, and `since` are transport-independent Core APIs
with no extra dependency. A mark captures the next Journal admission sequence
inside an active watcher; its UTC and monotonic timestamps are informational.
It does not seek, replay, add a consumer, or filter by broker publication time.
An earlier publication buffered outside the Journal can still arrive after
the mark. The same mark can be reused by that watcher's scoped or unscoped
waits, retaining parent filters and the later lower boundary.

The wait timeout starts with the wait call, not marker creation. Marker capture
does not register a scope or consume waiter/result quotas, and snapshots keep
their existing types and scope definitions. Normal per-wait limits still
apply. A mark does not pin history; strict waits fail on known loss in the
required marked range. Foreign-watcher marks, invalid future sequences, and
new mark creation after close are rejected under the ordinary lifecycle and
affinity rules. See [scopes and marks](scopes.md#reusable-observation-marks).

`max_bytes` is a deterministic estimate of retained data, not an RSS limit.
It includes raw payload/key/UTF-8 source lengths, 128 bytes for the envelope,
32 bytes per header/metadata pair, UTF-8 header-name lengths and raw header-value
lengths, UTF-8 metadata-name lengths and metadata-value lengths (raw bytes or
UTF-8 `str(value)`), a decoded payload estimate, and 128 bytes per record.
Null payloads, keys, and header values contribute zero content bytes.
The Protobuf estimate uses an adjusted
wire size and is not equivalent to native heap size. Native queues, snapshots,
and user results are accounted for separately from the retained journal.

One active sync watcher has one actor thread; Kafka and Redis Streams add
owned serialized native I/O workers, while blocking RabbitMQ owns Pika on the
actor. Native SDKs may create additional internal threads. Async sessions
retain caller-loop ownership. User decoders, matchers, redactors, and payload
copying run on the respective runtime owner; blocking results and Allure
rendering return to the original test thread. An arbitrary blocking callback
cannot be forcibly interrupted.
If cleanup hangs, an incomplete/abandoned outcome is recorded; the library
does not claim to have forcibly stopped the thread.

Temporal assertions, reconnect/checkpoint recovery, and further brokers are
deferred. `wait_for` checks do not prove ordering, the absence of future events,
exactly-once delivery, or a global order across Kafka partitions.
