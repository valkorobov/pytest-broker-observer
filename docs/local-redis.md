# Local Redis Streams through Docker Compose

The repository's [compose.yaml](../compose.yaml) provides an optional `redis`
profile for local Redis Streams tests. It pins
`redis:8.2.11-alpine@sha256:489a769e0a54dc245896132a15c72d2bc7296719954da99398f43a3877e5f8eb`.
It exposes Redis only on the host's
loopback port 6379, enables append-only persistence in a named `/data` volume,
and checks readiness with an authenticated `PING`. Docker and Compose with
`up --wait --wait-timeout` are required.
Run all commands from the repository root.

## Start Redis

```shell
uv sync --all-groups --all-extras
docker compose --profile redis up --detach --wait --wait-timeout 120 redis
docker compose ps redis
```

The local connection URL is `redis://:observer@127.0.0.1:6379/0`. The profile
requires the example password `observer` for the default Redis user and passes
it to the health check through `REDISCLI_AUTH`. Starting only `redis`
keeps the Kafka and RabbitMQ services independent. The broker health check and
watcher readiness serve separate purposes: entering `watch()` verifies the
specific existing stream and captures its initial observation boundary before
the test's application action.

## Integration tests and pytest examples

```shell
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest tests/integration/test_redis_streams_contracts.py tests/integration/test_redis_streams_observer.py -v
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest -c pyproject.toml examples/pytest/test_redis_streams.py -m integration -v
```

With the full development environment, the usage examples should produce
**2 passed**. The synchronous example watches latest, publishes after READY,
and correlates through an `operation-id` header. The asynchronous example
starts strictly after a known native stream ID and confirms that the next
eligible event is returned. Without pytest-asyncio, that async example is
explicitly skipped.

Fixtures create a unique typed stream per test and delete only their own keys
in teardown, including after failures. The seed `XADD`/`XDEL` creates an existing
empty stream before observation; the production adapter never creates an
application stream. Each harness call has its own client, so independent
`asyncio.run(...)` calls in sync tests do not share loop-bound connections.

Missing `REDIS_URL` produces explicit integration skips. A provided but
unreachable Redis causes failures. The environment variable belongs to the
test fixtures and is passed to the adapter explicitly.

## Redis Streams and AllureReporter

```shell
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest -c pyproject.toml examples/allure/test_redis_streams.py -m integration -v --alluredir=artifacts/allure-redis-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-redis-results --single-file --output artifacts/allure-redis-report --report-language en
npx --yes allure@3.22.1 open artifacts/allure-redis-report --host 127.0.0.1 --port 5054
```

Expect **2 passed**: correlated success and an expected timeout after a real
`started` event. Generating the report requires Node.js with npm and the
Allure CLI from the [setup guide](../examples/allure/README.md#setup).
The portable report is `artifacts/allure-redis-report/index.html`; opening it
does not require Redis to remain running.

Inside a test, open **Event observation summary [RedisStreamsAdapter: <stream>]**.
`expectation-result.json` and `expectation-result.txt` explain the result;
`mismatches.json` describes the unmatched `started` event.
`message-content.json` shows readable JSON bodies for matched and mismatched
samples, with `expectation_id`, `event_sequence`, and `match_status` linking
them to the expectation. All JSON attachments use `indent=2`.

The fixture explicitly opts into `REDACTED_PAYLOAD` and removes the synthetic
`access_token` from independent report copies. Original test payloads retain
that field. The default `METADATA_ONLY` policy omits message bodies; text
explanations do not include bodies under either policy. Capture and export
budgets still apply. The successful scenario creates one summary, and the
expected-timeout scenario creates two; normal close does not duplicate them.
See the [Redis Streams Allure guide](../examples/allure/README.md#redis-streams-and-allurereporter).

## Supported semantics and permissions

The source must be an existing Redis stream. Default native fields are
`payload` for the body and `header:<name>` for unique byte-valued headers.
The routing position is `event.raw.metadata["redis.stream_id"]`, while
`event.raw.key` is `None`. Null bodies, null/duplicate headers, keys, partitions,
and Kafka-style offsets are unsupported. Starts support latest, earliest, and
exclusive `StartPosition.after_stream_id("milliseconds-sequence")`.
Future IDs are allowed. An explicit ID below the baseline deletion watermark,
or conservatively below retained history when baseline deletions are known,
raises `StartPositionUnavailableError` rather than silently moving the boundary.

Every watcher performs independent `XREAD` with its own cursor. The read-only
runtime uses atomic `MULTI`/`EXEC` batches of `CLIENT ID`, `INFO server`,
`XINFO STREAM`, and nonblocking `XREAD`, requires Redis 7 or later, and leaves
consumer groups and their pending-entry lists unchanged. It does not write to the application stream or
acknowledge group deliveries. The account needs permission for these read and
transaction commands; the test harness separately needs `XADD`, `XDEL`, and
key deletion for its temporary sources.
The runtime does not need Lua or `EVAL` permission. A watcher observes one key;
Redis Cluster and Sentinel are outside the declared support.

Detected new deletion/trim evidence or metadata rewind ends observation
conservatively, without reopening at another cursor. An arbitrary delete and
identical recreation completed between checks is not guaranteed to be detected.
Connection ID and server `run_id` changes are checked before candidate handoff.
The native pool may open a replacement socket internally, but the observation
fails across that identity change instead of recovering silently.
The [adapter guide](redis-streams.md) explains boundaries and continuity limits.

## Stop Redis and troubleshoot

Stop only Redis, preserving its volume and other broker services:

```shell
docker compose stop redis
```

If port 6379 is occupied, set `REDIS_PORT` when starting the service and use
that port in the test URL. The local password stays the same:

```shell
REDIS_PORT=6380 docker compose --profile redis up --detach --wait --wait-timeout 120 redis
REDIS_URL=redis://:observer@127.0.0.1:6380/0 uv run pytest -c pyproject.toml examples/pytest/test_redis_streams.py -m integration -v
```

Restart with the same `up` command. Inspect startup and connection problems with:

```shell
docker compose ps redis
docker compose logs --no-color --tail 200 redis
```

Check that port 6379 is available and the test URL points to the running Redis.
A configured connection or health-check failure must remain visible rather
than turning into an infrastructure skip. Test teardown removes temporary
streams; stopping the broker preserves the remaining volume data.
