# Contributing

The original contract is [SPEC.md](SPEC.md). Verified implementation status and
limitations are documented in [docs/status.md](docs/status.md). Run the commands
below from the repository root.

## Project language

Use English for project documentation, code comments, docstrings, examples, test
descriptions, and report labels. Unicode test data may contain non-ASCII characters
when needed to verify encoding and byte limits; explain its purpose in English.
Generate Allure reports with `--report-language en`.

## Source docstrings

Document every class and named function in `src`, including private, nested,
async, special, protocol, and overload definitions. Use a meaningful English
summary followed by Sphinx fields: `:param name:`, `:returns:`, and
`:raises ErrorType:` where applicable. Class docstrings describe explicitly
declared class and instance attributes, including properties, with `:ivar name:`.
Inherited attributes are documented by their declaring class.

Run `uv run python tools/check_docstrings.py` to check definition, parameter,
and attribute coverage without importing optional dependencies. CI runs the
same audit. Lambdas and generated dataclass methods have no source docstring
requirement. The checker verifies coverage; review the text for accuracy too.

## Setup

Use Python 3.11–3.14 and install the environment:

```shell
uv sync --all-groups --all-extras
```

Install the hooks inside the Git repository before committing:

```shell
uv run --locked pre-commit install
```

The hooks check Ruff lint, Ruff formatting, and types through Pyrefly. All three
use tool versions from `uv.lock` and check the entire project. They do not modify
files automatically; fix formatting separately.

Core uses only the standard library. Do not add mandatory Kafka, RabbitMQ, Redis,
Protobuf, pytest, Allure, or OpenTelemetry imports to a plain
`import event_observer`; `aio_pika`, its AMQP dependencies, and `redis` stay lazy. The runtime
must not read credentials or open connections before entering a watcher context.

Sync and async orchestration are separate. Blocking adapters implement
`transports.sync_base` without creating an asyncio loop or task; built-ins offer
lazy `sync_adapter()` providers. Shared expectation/admission helpers must stay
independent from either scheduler. Preserve both sets of transport contracts,
worker-owned callbacks, caller-thread report delivery, and actual cancellation
acknowledgements. See [the execution guide](docs/sync-async.md).

## Checks

```shell
uv run --locked pre-commit run --all-files
uv run python tools/check_docstrings.py
uv run --locked coverage run -m pytest tests -m 'not integration'
uv run --locked coverage report
uv run pytest -m 'not integration'
uv run pytest -n 2 -m 'not integration'
uv run ruff check .
uv run ruff format --check .
uv run --locked pyrefly check
uv run pyright
uv build --no-sources
uv run twine check dist/*
```

Unit coverage must reach **80%** across the complete `event_observer` package,
including transport adapters and public testing helpers. Branch measurement is
enabled; the gate uses Coverage.py's combined statement/branch percentage.
Run coverage before pytest so the passive pytest entrypoint's imports are
measured too. Keep examples and real-broker runs separate from this measurement.
See [the coverage guide](docs/testing.md#unit-coverage) for reports and CI behavior.

Use `uv run ruff format .` to fix formatting. For Kafka integration tests and the
Kafka pytest example, start the local broker from `compose.yaml`
(Docker Compose v2.18+):

```shell
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml tests/integration/test_kafka*.py examples/pytest/test_kafka.py -v
docker compose down
```

These tests create and delete topics. An explicitly configured but unreachable
broker causes a failure; a missing environment variable causes an explicit skip.
Docker is not required for ordinary Core tests or for an existing broker.
Ports and data retention are covered in [docs/local-kafka.md](docs/local-kafka.md).

For RabbitMQ, start only its optional profile and run the dedicated contracts
and sync/async examples:

```shell
docker compose --profile rabbitmq up --detach --wait --wait-timeout 120 rabbitmq
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest -c pyproject.toml tests/integration/test_rabbitmq*.py examples/pytest/test_rabbitmq.py -v
docker compose stop rabbitmq
```

The harness creates and deletes only its own topic exchanges. The production
adapter verifies an existing exchange and binds a dedicated exclusive queue;
it does not create the application's exchange or consume an application queue.
Missing `RABBITMQ_URL` explicitly skips real tests; configured connection failures
must fail. The [RabbitMQ guide](docs/rabbitmq.md) and
[local Compose guide](docs/local-rabbitmq.md) describe permissions and semantics.

For Redis Streams, use the authenticated optional profile:

```shell
docker compose --profile redis up --detach --wait --wait-timeout 120 redis
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest -c pyproject.toml tests/integration/test_redis_streams*.py examples/pytest/test_redis_streams.py -v
docker compose stop redis
```

The harness creates existing empty typed streams using its own `XADD`/`XDEL`
seed and deletes only owned keys. The runtime must remain read-only: no
`XREADGROUP`, group creation, pending-entry acknowledgement, or application
stream writes. It uses independent cursors and atomic `XINFO STREAM`/`XREAD`
transactions with `CLIENT ID`/`INFO server` identity checks, `MULTI`/`EXEC`
permission, and no Lua or `EVAL` requirement.
Keep native `stream_id` starts exclusive and distinct from Kafka offsets.
Missing `REDIS_URL` skips real tests; configured failures must remain failures.
See [the adapter guide](docs/redis-streams.md) and
[local Redis guide](docs/local-redis.md).

To check AllureReporter with the real pytest plugin and view HTML, use
[examples/allure](examples/allure/README.md). The regular run passes; an intentional
demonstration failure is enabled with a separate flag.

## Core and adapter changes

Keep expectation logic shared by the synchronous and asynchronous facades.
Predicates receive detached payloads; a scope does not create a consumer;
independent waiters do not consume Journal records from one another. Known
failures and incomplete observation must not turn into ordinary timeouts or
successful results.

For race regressions, use a controlled barrier or Clock rather than random sleeps.
Assert observable contracts: failure reason, completeness, reader independence,
resource cleanup, or deadline eligibility.

A new adapter implements `event_observer.transports.base.TransportAdapter` and
creates loop-bound resources inside `open`. Declare actual capabilities and
connect the contracts through your own `adapter_harness` fixture:

```python
from event_observer.testing.contracts import (
    IsolationContract,
    LifecycleContract,
    ReadinessContract,
)


class TestCustomAdapter(LifecycleContract, ReadinessContract, IsolationContract):
    pass
```

The publisher harness must confirm broker acceptance. Fault, topology, and
position controls belong in separate extension protocols. Do not hide unverified
capability claims behind skips.

`supports_null_payload`, `supports_duplicate_headers`, and
`supports_null_header_values` are explicit transport
capabilities, alongside replay and key support. Contracts record these optional
preconditions and `NOT_APPLICABLE` when an adapter does not claim them. Core
data preservation still applies to accepted values, and a claimed capability
must be verified. RabbitMQ supports unique byte/null headers and empty byte
bodies, with neither null bodies nor duplicate header names. Verify dedicated
queue isolation and admission-time settlement without weakening Kafka contracts
or introducing reconnect semantics.

Redis Streams additionally rejects null header values while supporting unique
byte-valued headers and empty bodies. The combined duplicate/null header
contract checks all three header preconditions; Kafka and RabbitMQ keep their
existing supported domains. Continuity tests must distinguish initial retained
history from new deletion/trim or metadata rewind. Detected new loss fails
conservatively even for entries already read, without claiming detection of
an exact metadata-equivalent delete/recreate entirely between checks. Use
controlled state changes rather than silently retrying a known failed read.
Connection ID or server `run_id` changes must fail before candidate handoff,
including when the native client internally creates a replacement socket.

## Documentation and releases

Document actual API behavior and update the changelog and verification status.
Changing a mandatory contract requires an explicit SPEC/ADR revision rather than
silently weakening readiness, copying, or completeness.

Before release, candidate artifacts and the SPEC release gates need reproducible
verification. The public name, license, publishing identity, and Trusted Publishing
require separate owner confirmation. A local build does not publish the package.
