# Local RabbitMQ through Docker Compose

The repository's [compose.yaml](../compose.yaml) includes an optional `rabbitmq`
profile. It pins RabbitMQ 4.4.0 with the management interface by its image
digest and provides loopback-only AMQP
port 5672 and management port 15672, a named data volume, and a diagnostic
health check. Docker and Compose with `up --wait --wait-timeout` are required.
Run all commands from the repository root.

## Start the broker

```shell
uv sync --all-groups --all-extras
docker compose --profile rabbitmq up --detach --wait --wait-timeout 120 rabbitmq
docker compose ps rabbitmq
```

The service is ready when its RabbitMQ diagnostic health check passes.
The AMQP URL for tests is `amqp://observer:observer@127.0.0.1:5672/`.
Open the [management interface](http://127.0.0.1:15672) and sign in with
username `observer` and password `observer` to inspect the local broker.
These credentials belong to the local example environment.

Starting only the `rabbitmq` service leaves the Kafka service independent.
Broker readiness and observation readiness are separate: entering `watch()`
waits for the specific observer's private queue, bindings, and consumer before
your test publishes an event.

## Run the pytest examples

Run the reusable contracts and observer integration tests:

```shell
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest tests/integration/test_rabbitmq_contracts.py tests/integration/test_rabbitmq_observer.py -v
```

The contracts explicitly mark unsupported capabilities as not applicable.
Observer scenarios cover sync/async observation, Protobuf, unique headers,
custom routing bindings, and preservation of an application's separate queue.
Run the two standalone usage examples separately:

```shell
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest -c pyproject.toml examples/pytest/test_rabbitmq.py -m integration -v
```

With the full development environment, expect **2 passed**: a synchronous
example and an asynchronous example. Without pytest-asyncio, the asynchronous
example is explicitly skipped. Each fixture creates a unique topic exchange;
each watcher binds its own exclusive auto-delete queue to that exchange.
The examples publish after READY and correlate by the `operation-id` header.
The harness waits for publisher confirmation and deletes only its own
exchanges during teardown, including after failure.

Without `RABBITMQ_URL`, these tests explicitly skip. A provided but unreachable
broker causes failure. The environment variable is read by test fixtures and
passed to the library explicitly.

## RabbitMQ and AllureReporter

```shell
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest -c pyproject.toml examples/allure/test_rabbitmq.py -m integration -v --alluredir=artifacts/allure-rabbitmq-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-rabbitmq-results --single-file --output artifacts/allure-rabbitmq-report --report-language en
npx --yes allure@3.22.1 open artifacts/allure-rabbitmq-report --host 127.0.0.1 --port 5053
```

Expect **2 passed**. The scenarios demonstrate correlated success and an
expected timeout after a real `started` event. The Allure CLI requires Node.js
with npm; full setup is described in the
[Allure examples guide](../examples/allure/README.md#setup).
The standalone report is `artifacts/allure-rabbitmq-report/index.html`.
Viewing a generated report does not require a running RabbitMQ broker.

Open **Event observation summary [RabbitMQAdapter: <exchange>]** inside a test.
The `expectation-result.json` and `expectation-result.txt` attachments explain
the outcome; `mismatches.json` identifies why `started` did not satisfy the
expectation for `completed`. `message-content.json` shows redacted JSON bodies
for matched and mismatched samples, with `match_status`, `expectation_id`, and
`event_sequence` linking each body to its result. The fixture explicitly uses
`REDACTED_PAYLOAD` and removes the synthetic `access_token` from report copies.
The original decoded test values retain that field. The default
`METADATA_ONLY` policy omits message bodies, and text explanations never
include them. All JSON attachments use `indent=2`.

The success scenario creates one summary. The timeout scenario creates two:
one for the successful `started` expectation and one for the expected timeout.
Normal watcher closure does not duplicate either summary. See the
[RabbitMQ Allure guide](../examples/allure/README.md#rabbitmq-and-allurereporter)
for details.

## Ownership and supported semantics

The observation source is an exchange, rather than an application queue.
The observer consumes only its own dedicated queue, preserving the application's
queue delivery. Only latest observation is supported; there is no replay of
messages published before the queue was bound and no automatic reconnect.
Messages are acknowledged when admitted to the Journal, independently of
matcher outcomes. Retained Journal history can satisfy another expectation
without asking RabbitMQ to redeliver a message.
The runtime requires the source exchange to exist and does not declare or
reconfigure an operator-owned exchange. Only the test harness creates the
temporary exchanges used in these examples.

An external broker account needs access to bind/read the exchange and to
declare, consume, and delete the observer's own queues. The test harness also
needs exchange creation/deletion and publication permissions. See
[the RabbitMQ adapter guide](rabbitmq.md) for configuration, headers, metadata,
and capability limits.

## Stop RabbitMQ and inspect problems

Stop only RabbitMQ, preserving its data and any running Kafka service:

```shell
docker compose stop rabbitmq
```

Start it again with the same `up` command. The named data volume preserves
broker state; test-created temporary exchanges are removed by the fixtures.
Observer-owned exclusive queues close with their connections, including on
startup or terminal failures.

If the default ports are occupied, set `RABBITMQ_PORT` and
`RABBITMQ_MANAGEMENT_PORT` before starting the service, and update the test URL
and browser address accordingly. For example:

```shell
RABBITMQ_PORT=5673 RABBITMQ_MANAGEMENT_PORT=15673 docker compose --profile rabbitmq up --detach --wait --wait-timeout 120 rabbitmq
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5673/ uv run pytest -c pyproject.toml examples/pytest/test_rabbitmq.py -m integration -v
```

For startup or connection problems:

```shell
docker compose ps rabbitmq
docker compose logs --no-color --tail 200 rabbitmq
```

Check that ports 5672 and 15672 are available and that the AMQP URL matches the
running broker. A health check failure is a startup failure, rather than a
reason to treat configured integration tests as skipped.
