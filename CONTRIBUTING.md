# Contributing

The original contract is [SPEC.md](SPEC.md). Verified implementation status and
limitations are documented in [docs/status.md](docs/status.md). Run the commands
below from the repository root.

## Project language

Use English for project documentation, code comments, docstrings, examples, test
descriptions, and report labels. Unicode test data may contain non-ASCII characters
when needed to verify encoding and byte limits; explain its purpose in English.
Generate Allure reports with `--report-language en`.

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

Core uses only the standard library. Do not add mandatory Kafka, Protobuf, pytest,
Allure, or OpenTelemetry imports to a plain `import event_observer`. The runtime
must not read credentials or open connections before entering a watcher context.

## Checks

```shell
uv run --locked pre-commit run --all-files
uv run pytest -m 'not integration'
uv run pytest -n 2 -m 'not integration'
uv run ruff check .
uv run ruff format --check .
uv run --locked pyrefly check
uv run pyright
uv build --no-sources
uv run twine check dist/*
```

Use `uv run ruff format .` to fix formatting. For Kafka integration tests and the
Kafka pytest example, start the local broker from `compose.yaml`
(Docker Compose v2.18+):

```shell
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml tests/integration examples/pytest/test_kafka.py -v
docker compose down
```

These tests create and delete topics. An explicitly configured but unreachable
broker causes a failure; a missing environment variable causes an explicit skip.
Docker is not required for ordinary Core tests or for an existing broker.
Ports and data retention are covered in [docs/local-kafka.md](docs/local-kafka.md).

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

## Documentation and releases

Document actual API behavior and update the changelog and verification status.
Changing a mandatory contract requires an explicit SPEC/ADR revision rather than
silently weakening readiness, copying, or completeness.

Before release, candidate artifacts and the SPEC release gates need reproducible
verification. The public name, license, publishing identity, and Trusted Publishing
require separate owner confirmation. A local build does not publish the package.
