# Local Kafka through Docker Compose

The project root contains [compose.yaml](../compose.yaml) with one Apache
Kafka 3.9.2 broker in KRaft mode. A separate ZooKeeper is not required. The
image is pinned by SHA-256 manifest; CI uses the same Compose file.

A running Docker and Compose v2.18+ with `up --wait --wait-timeout` are required.
Run all commands from the repository root.

## Startup and readiness

```shell
uv sync --all-groups --all-extras
docker compose up --detach --wait --wait-timeout 120
docker compose ps
```

`up --wait` returns after a successful health check: the Kafka CLI lists topics
through the internal listener. The broker is available to tests on the host at
`127.0.0.1:9092`. The port is published only on the host's loopback interface.
Inside the Compose network, the broker address is `kafka:29092`; the controller
port is not published to the host. The local profile uses PLAINTEXT.

The health check verifies broker readiness. Entering `watch()` additionally
confirms that the specific reader is ready according to the library contract.

## Integration tests and pytest example

Library integration checks together with the Kafka example without Allure:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml tests/integration examples/pytest/test_kafka.py -v
```

Library integration tests only:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest tests/integration -v
```

Pytest usage example only:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml examples/pytest/test_kafka.py -m integration -v
```

The harness creates unique topics and deletes only its own resources during
teardown. Automatic topic creation by the broker is disabled: the harness
creates test sources. Without `KAFKA_BOOTSTRAP_SERVERS`, Kafka scenarios are
explicitly skipped. A specified but unreachable broker causes an error.
Connection settings are passed to the library explicitly; reading this
variable is the responsibility of the test fixtures.

## Kafka with an Allure report

Two integration examples in [examples/allure/test_kafka.py](../examples/allure/test_kafka.py)
use `AllureReporter`: successful event correlation and an expected timeout
with a mismatched message. With the broker already running:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml examples/allure/test_kafka.py -m integration -v --alluredir=artifacts/allure-kafka-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-kafka-results --single-file --output artifacts/allure-kafka-report --report-language en
npx --yes allure@3.22.1 open artifacts/allure-kafka-report --host 127.0.0.1 --port 5051
```

Expect **2 passed**. Node.js with npm is required for the Allure CLI; the
development environment from the `uv sync` command above includes the Python
dependencies. Full setup, attachment descriptions, and viewing HTML without a
running Kafka instance are covered in the
[Kafka and AllureReporter guide](../examples/allure/README.md#kafka-and-allurereporter).
In CI, the `kafka` job saves this report as `allure-kafka-example-report` for 7 days.

## Custom port

If 9092 is busy, choose a free port and use it in both commands:

```shell
export KAFKA_PORT=19092
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 uv run pytest -c pyproject.toml tests/integration examples/pytest/test_kafka.py -v
docker compose down
unset KAFKA_PORT
```

`KAFKA_PORT` changes both the published port and Kafka's advertised listener.
This lets the client connect to the address from metadata after bootstrap.
The internal listener and health check keep the `kafka:29092` address.
Tests do not receive the address automatically from Compose: when changing
the port, also update `KAFKA_BOOTSTRAP_SERVERS`.

## Data storage and shutdown

```shell
docker compose down
```

This command stops and removes the container and network. Broker data remains
in this Compose project's named `kafka-data` volume. A persistent `CLUSTER_ID`
and the `/var/lib/kafka/data` directory allow restarting the same broker with
its retained data using `docker compose up --detach --wait --wait-timeout 120`.

To fully reset the local broker, including all its topics and records:

```shell
docker compose down --volumes
```

For concurrent independent environments, specify a different project name with
`docker compose -p my-kafka ...` and a separate `KAFKA_PORT` for each environment.
Use the same `-p` name for startup, log viewing, and shutdown.

## Troubleshooting startup

```shell
docker compose ps
docker compose logs --no-color --tail 200 kafka
```

For a port conflict, choose a different `KAFKA_PORT`. After changing listeners,
subsequent `up` recreates the container with the new configuration; the volume
is preserved. If changing the cluster ID or storage settings for a new test
environment, use a new Compose project or explicitly reset its data.

The profile is intended for local tests with one broker. Multiple-broker,
ACL/TLS, and fault-proxy checks remain separate profiles listed in the
[implementation status](status.md).

## Confirmed verification

On macOS arm64 with CPython 3.14.7 and Docker Compose 5.5.1, all 23 scenarios
passed: the library's integration tests and the Kafka pytest example. Checks
covered startup on a custom host port, the health check, reading a retained
record after `down`/`up`, and deleting only the temporary Compose project and
its volumes. Two Kafka scenarios with AllureReporter also passed separately:
successful correlation and an expected timeout; JSON attachments were verified
and a standalone HTML report was built. This is local verification of the
profile; a successful GitHub Actions run is confirmed separately by CI results.
