# Kafka adapter

Install the local project with `python -m pip install '.[kafka]'`. The import
namespace follows the specification: `event_observer`.

For a local broker, use the repository's [Docker Compose guide](local-kafka.md).
The same profile runs the integration tests and Kafka pytest example in CI.

```python
from event_observer import StartPosition
from event_observer.sync import SyncEventObserver
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig

events = SyncEventObserver(transport=KafkaAdapter(KafkaConfig(bootstrap_servers="localhost:9092")))

with events.watch(source="example.events", start=StartPosition.latest()) as watcher:
    # Invoke your application only after entering the context.
    result = watcher.wait_for(lambda value: value == b"completed", timeout=5.0)
```

The adapter does not publish messages. Replace the comment with the application
action that produces the expected event. Use `EventObserver` from
`event_observer.aio` and `async with` for asynchronous tests; the decoder and
matcher API stays the same. Existing top-level imports remain available.

## Configuration and ownership

`KafkaAdapter` supports both explicit runtimes. `SyncEventObserver` calls its
ordinary `sync_adapter()` provider to obtain `SyncKafkaAdapter`; a blocking
adapter can also be passed directly from `event_observer.transports.sync`.
Both bridges share the same serialized native worker, numeric boundary
resolution, Fetch probe, topology checks, and envelope normalization. The async
bridge belongs to the caller loop; the blocking bridge uses thread conditions
and creates no asyncio event loop.

Each watcher has its own native Consumer and polling thread. Construction,
provider selection, and `watch()` perform no network operations or optional
client imports. All native Consumer operations execute on that polling thread.
The factory can be reused across independent watchers.
Boundary resolution briefly creates an AdminClient on the same worker; its
security/network settings match the reader. Completed bounded admin requests
release that native handle before reading begins.

The adapter always uses manual numeric assignment, a unique diagnostic group ID,
`enable.auto.commit=false`, `enable.auto.offset.store=false`,
`auto.offset.reset=error`, and `allow.auto.create.topics=false`. It does not commit
offsets or join an application consumer group. `client_options` cannot override
these settings or other observer-managed security, callback and budget settings.

`KafkaConfig` accepts a nonempty bootstrap string or tuple of addresses. Supported
security protocols are `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` and `SASL_SSL`.
Configure SASL with `sasl_mechanism`, `sasl_username` and `sasl_password`; configure
TLS with `ssl_ca_location` and, when needed, both `ssl_certificate_location` and
`ssl_key_location`. Secret fields are excluded from the configuration's `repr`.
TLS/SASL failures never cause a fallback to plaintext.

The explicit default is `isolation_level="read_committed"`.
`topology_check_interval=1.0` controls periodic metadata checks. A detected
partition-set change ends the observation with `TopologyChangedError`; the
adapter does not silently add partitions or reconnect at a new offset.

## Start positions and readiness

| Start | Behavior |
| --- | --- |
| `StartPosition.latest()` | Capture each selected partition's uncached physical numeric high watermark. |
| `StartPosition.earliest()` | Capture each partition's available low watermark. |
| `StartPosition.at_offset({0: 100, 1: 200})` | Inclusive lower bounds covering exactly all selected partitions. |
| `StartPosition.at_timestamp(aware_datetime)` | Resolve each partition through `offsets_for_times`. |
| `StartPosition.at_timestamp(EpochMilliseconds(1234))` | Explicit integer epoch milliseconds; import `EpochMilliseconds` from `event_observer`. |

Offsets outside the available range fail. An offset equal to the high watermark
is valid and waits for future records. Missing timestamp results use the captured
high watermark unless `require_existing=True`, which fails the entire startup.
Timestamp selection chooses a starting position; it does not filter future
messages by their timestamps. Offset jumps are not treated as proof of loss.

Entering the context confirms the Fetch path for every selected partition, even
on an empty topic. The same native reader probes at high watermarks, using
successful records, partition EOF or successful Fetch statistics as evidence,
then assigns the original requested numeric boundary. Records produced during
the probe are read from that boundary. Metadata and assignment alone do not
produce optimistic READY. A missing Fetch confirmation fails within the startup
budget. EOF is a readiness/diagnostic signal and never a business event.

For `read_committed`, latest means new log records at or above the captured
numeric high boundary. An older open transaction committing after readiness does
not make its records below that boundary eligible. Readiness does not need to
wait for an existing transaction to commit.

The adapter explicitly resolves physical highs through `AdminClient.list_offsets`
with `READ_UNCOMMITTED`; the consuming reader remains `read_committed`.
Consumer watermark queries themselves honor isolation and can return LSO, which
would incorrectly include an older transaction after its later commit. This
native behavior is documented in the
[librdkafka changelog](https://github.com/confluentinc/librdkafka/blob/master/CHANGELOG.md)
and covered by both an independent native-client regression and a real-broker
transaction test.

## Buffers and limits

The Python handoff conservatively keeps one record in flight, including the
producer-side record. It remains within `TransportBufferLimits.max_events` and
`max_bytes`; the implementation currently does not use all 256 default event
slots. Pause/resume provides flow control. Stop and failure notifications have a
separate path, so a full handoff cannot prevent shutdown or reporting a detected
failure. An event that cannot fit the handoff fails with
`TransportBackpressureError`; it is never silently discarded.

Native queue and fetch budgets are configured separately through
`native_queue_bytes` (16 MiB) and `native_fetch_bytes` (1 MiB). librdkafka can
temporarily exceed fetch limits to return a large record. These settings and the
Journal's accounted bytes do not bound total process RSS. A stuck native call
cannot be forcibly killed safely; cleanup exposes an exhausted budget.

In the blocking runtime, application predicates, decoders, and payload-copy
callbacks run on the watcher actor, while the Kafka client runs on its separate
native polling thread. The actor keeps collecting while the application call
blocks the test thread. Callbacks must remain fast and must not call their own
blocking watcher recursively. Parallel assertions use the async API.

The blocking transport protocol uses finite `poll(timeout)`, independent
`failure()`, and bounded `close(deadline=...)`; custom async-only adapters cannot
be converted by starting a hidden loop. See the
[runtime migration contract](../SPEC.md#s31).

## Validation status

The native-client contract suite covers explicit boundaries, multi-partition
readiness, publication during startup, null/empty payloads, duplicate headers,
noncontiguous offsets, timestamp fallback, authorization normalization, bridge
pressure, topology failures, independent sessions and cleanup. It runs without
Kafka through a deterministic client double.

Real Kafka integration scenarios are implemented separately:

```bash
uv sync --locked --all-groups --all-extras
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest tests/integration -v
```

They create and delete uniquely named topics. Use an isolated broker with topic
creation/deletion and transaction permissions. An explicitly configured but
unavailable broker fails the suite; only missing configuration causes skips.

On 1 November 2026, all **22 real Kafka integration tests passed** in 27.47 seconds:
five Kafka semantics cases, fifteen reusable transport contracts, and synchronous
and asynchronous Protobuf observer scenarios. The tested profile was CPython
3.14.7 on macOS arm64, `confluent-kafka` 2.16.1 / librdkafka 2.16.0, and Apache
Kafka 3.9.2 in a Linux arm64 KRaft container. That historical validation included
33 passing native-client unit cases. See [the validation record](kafka-validation.json)
and [current verification status](status.md) for subsequent runtime checks.

The CI image is pinned to the independently verified Docker Hub multi-platform
manifest
`apache/kafka:3.9.2@sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02`.
The upstream arm64 content was resolved from manifest
`sha256:ddc235acd57d0fec5b21eedd46dcaf4e89a7569ff1c9890c5597c6d9561c59b0`.
For the local run, registry blob transfer through Docker stalled, so those
official configuration/layers were downloaded and hash-verified on the host,
then imported into Docker. This changed only the manifest representation to
Docker v2; the local wrapper ID was
`sha256:55462afb3063696dfa1f6ce6f2c05b16e5b51e94ac4bcc73692c8087733c207d`.
Every compressed layer and uncompressed layer hash matched the upstream image.

The first real run exposed the LSO/high-watermark distinction described above:
21 cases passed and the transaction boundary case failed. The implementation was
corrected, an offline regression was added, and the entire real suite passed on
the corrected implementation. This is a resolved implementation defect, rather
than a retried flaky test.

The full broker/dependency/OS compatibility matrix, ACL/TLS profiles,
compaction/fault-proxy scenarios, and real-broker benchmark baselines remain
release gates. The tested profile above does not imply that those gates passed.

Native semantics are based on the primary
[Confluent Python API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html),
[librdkafka configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html),
and [Fetch statistics](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_STATISTICS.html).
The CI broker configuration follows Apache's
[Docker image guide](https://hub.docker.com/r/apache/kafka).
