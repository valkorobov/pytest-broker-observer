# Repository guidance

## Boundaries that must survive changes

- Distribution: `pytest-broker-observer`; Python package: `src/event_observer/`.
- `aio.py` re-exports the async implementation in `observer.py`. `sync.py` uses a
  blocking actor, not an asyncio bridge. Never introduce a private event loop into
  sync observation; use a blocking adapter or an explicit `sync_adapter()` provider.
- Keep shared semantics in `_expectations.py` and `_admission.py` scheduler-independent.
  See `docs/sync-async.md` before changing runtime ownership or callbacks.
- Core and `integrations/pytest_plugin.py` must import with stdlib only. Broker
  clients, Protobuf, pytest, Allure, and OpenTelemetry must stay optional/lazy.
  Constructors and `watch()` creation must not start I/O, threads, or event loops;
  context entry opens resources and establishes READY before the test action.
- The pytest entry point delegates to `_pytest_factories.py`: five session-scoped
  fixtures return existing constructors, not configured instances. Projects own
  configuration, credentials, observer fixtures, and active watcher lifetimes.
  Preserve pytest-only sync operation without pytest-asyncio; no autouse connections.
- `SPEC.md` is the behavioral contract; essential contract changes need a revision
  and `CHANGELOG.md` entry. Record actual verification in `docs/status.md`, not
  inferred success from configured CI jobs or dependency ranges.

## Development and verification

Run from the repository root; Python 3.11–3.14 is supported.

```sh
uv sync --locked --all-groups --all-extras
uv run --locked pytest tests/test_pytest_plugin.py -q  # focused import/autoload/factory checks
uv run --locked pre-commit run --all-files           # Ruff lint/format + Pyrefly
uv run --locked pyright                             # separate from the hooks
uv run --locked python tools/check_docstrings.py     # separate from the hooks
uv run --locked pytest -n 2 -m 'not integration'
```

- Hooks check the whole project and do not fix formatting; use `uv run --locked ruff format .`.
- Use English for docs, docstrings, comments, examples, and report labels.
  The docstring audit includes private/nested functions, overloads, and protocols
  in `src/`, requiring Sphinx `:param name:` and attribute `:ivar name:` fields;
  see `CONTRIBUTING.md` for the full convention.
- Coverage must start **before pytest plugin loading**, not via a late `--cov`:

  ```sh
  uv run --locked coverage run -m pytest tests -m 'not integration'
  uv run --locked coverage report
  ```

  The gate is 80% combined statements/branches for the entire package. Do not mix
  examples or broker integration runs into this measurement.
- Default `testpaths` excludes examples. Their nested `pytest.ini` omits async
  settings for pytest-only usage; select the root config for the full async suite:
  `uv run --locked pytest -c pyproject.toml examples/pytest examples/allure -m 'not integration'`.
- For import/plugin/packaging changes, use `uv build --no-sources` and
  `uv run --locked twine check dist/*`, then repeat the packaging job's clean
  wheel/sdist and pytest-only checks in `.github/workflows/ci.yml`. The all-extras
  development environment can hide eager-import regressions.

## Real brokers

Missing broker environment variables **skip** integration tests; configured but
unreachable brokers **fail**. Variables below are test-harness conventions, not
library auto-configuration. Docker Compose v2.18+ provides all three services:

```sh
docker compose --profile rabbitmq --profile redis up --detach --wait --wait-timeout 120 kafka rabbitmq redis
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ \
RABBITMQ_MANAGEMENT_URL=http://127.0.0.1:15672 \
REDIS_URL=redis://:observer@127.0.0.1:6379/0 \
uv run --locked pytest -c pyproject.toml tests/integration examples/pytest examples/allure -m integration
```

- The management URL enables RabbitMQ's targeted connection-failure test.
- Harnesses create/delete unique owned topics, exchanges, or stream keys; do not
  substitute application resources. Compose uses persistent named volumes: routine
  verification must not reset them. See `docs/local-*.md` and the adapter guides for setup/ACLs.
