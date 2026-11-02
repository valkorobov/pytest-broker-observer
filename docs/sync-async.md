# Separate sync and async execution

The library has two explicit execution backends. They share event models,
decoders, matchers, Journal retention, expectation rules, and diagnostic policies.
The blocking backend does not create or run an asyncio event loop.

```python
from event_observer.aio import EventObserver
from event_observer.sync import SyncEventObserver
```

The existing top-level imports remain available with the same meaning.
`EventObserver` is always asynchronous; `SyncEventObserver` is always blocking.
There is no automatic mode detection.

## Ownership and continuous collection

An async watcher owns tasks and its native async session in the caller's event
loop. A sync watcher starts one observation actor thread. Its bounded command
queue and actual completion signals coordinate the creating caller thread with
the worker. Kafka and Redis Streams have additional serialized native I/O
workers; librdkafka may also create internal threads. Blocking RabbitMQ owns
Pika directly on the observation worker. These resources are included in the
documented thread ownership model.

The sync worker owns decoding, Journal admission, scope construction, marks,
matching, result reservations, and diagnostic capture. It alternates finite
transport polls with bounded expectation scans and control requests. A pending
expectation does not block reception. Messages continue to arrive while the
test thread performs an application action.

```text
test thread                         sync observation worker
-----------                         -----------------------
enter watch ----------------------> open independent blocking session
             <--------------------- READY
perform application action          poll -> decode -> admit -> settle
wait_for(..., since=mark) ---------> scan retained history and new events
             <--------------------- result after terminal/completeness checks
render Allure export                continue collection
exit watch -----------------------> close session and acknowledge cleanup
```

`ExpectationState` is shared by both runtimes. It has no asyncio or threading
dependency. It controls cursors, filtering, distinct counting, deadline cutoffs,
completeness, result-byte reservations, and outcome summaries. Shared admission
assigns sequence/time and commits the Journal before settlement is allowed.

## Transport selection

| Broker | Async path | Blocking path |
| --- | --- | --- |
| Kafka | Async bridge over the shared native Consumer worker | Condition-based bridge over the same worker |
| RabbitMQ | aio-pika session | Pinned stable Pika blocking session with finite I/O pumping |
| Redis Streams | redis.asyncio client | redis.Redis client |
| FakeTransport | Async synthetic reader | Condition-based synthetic reader over the same publications |

Native clients remain optional and lazy. The RabbitMQ extra includes both
clients; Pika is constrained to `>=1.4.4,<1.5`. Its startup/cleanup RPC timeout
guard is an explicitly tested native-client boundary. Redis modes share atomic
metadata/read parsing and continuity checks, with no consumer groups or writes
from the observer.

Existing built-in adapter objects can be passed to either factory. Their lazy
`sync_adapter()` provider selects the blocking implementation without opening
resources. Explicit blocking classes are also available:

```python
from event_observer.sync import SyncEventObserver
from event_observer.transports.kafka import KafkaConfig
from event_observer.transports.sync import SyncKafkaAdapter

events = SyncEventObserver(transport=SyncKafkaAdapter(KafkaConfig("localhost:9092")))
```

## Custom adapter migration

The existing `TransportAdapter`, `TransportSession`, and `SettlementSession`
interfaces remain async interfaces. A third-party async-only adapter cannot be
used by the blocking factory without implementing a blocking backend.

The blocking contracts are exported from `event_observer.transports.sync`:

- `SyncTransportAdapter`: capabilities and synchronous `open(...)`.
- `SyncTransportSession`: readiness, finite `poll(timeout)`, non-consuming
  `failure()`, and idempotent `close(deadline=...)`.
- `SyncSettlementSession`: optional synchronous post-admission settlement.
- `SyncAdapterProvider`: optional synchronous lazy `sync_adapter()` factory.

`poll(0)` inspects available data without waiting; poll budgets must be finite
and nonnegative. Failure inspection must work without a business message and
before handing off buffered data. Startup and cleanup use the supplied shared
clock domain and absolute deadlines.

An existing adapter can expose a provider returning a separate blocking adapter,
or callers can pass the blocking implementation directly. Returning a coroutine
from `sync_adapter()` or blocking `open()` is rejected; the library never drives
an async adapter with a hidden event loop. Custom FakeTransport subclasses that
override `async open()` also need an explicit provider override when their fault
or instrumentation should affect blocking sessions.

## Callbacks, cancellation, and reports

Sync callbacks remain on the observation worker, with startup context used by
decoders and a copied caller context per expectation. They must be synchronous
and must not reenter the same blocking watcher. A sync handle retains creating
thread/process affinity and is rejected inside an active async context. The
async affinity rules are unchanged.

Public wait deadlines start before dispatch. A mark is captured on the worker
before returning to the caller and selects Journal admission order. Capturing
a mark does not pin history or emit a report.

Caller interruption requests cooperative cancellation and waits for actual
completion within the grace budget. An unacknowledged command keeps its slot;
subsequent requests cannot accumulate behind it. Native and arbitrary Python
callbacks cannot be forcibly killed. A worker that cannot finish cleanup within
its join budget is reported as abandoned rather than claimed closed.

The worker prepares redacted bounded diagnostics. Allure rendering still occurs
in the original caller/reporting thread. Completed waits are reported once;
normal close does not repeat them. Primary failures survive ordinary cleanup
failures, while a new caller interruption remains an interruption.

## Verification

Shared state-machine tests, public facade parity, blocking and async transport
contracts, native doubles, and all three real brokers cover the two backends.
Blocking tests forbid asyncio startup/scheduling APIs and check continuous
collection, actual cancellation acknowledgement, context isolation, and cleanup.
Independent async publisher/admin harnesses used by integration fixtures are
test infrastructure; they do not implement the observer's blocking runtime.

See the [migration plan](sync-async-migration-plan.md),
[independent review](sync-async-migration-review.md), and [verified status](status.md)
for the current results and practical boundaries.
