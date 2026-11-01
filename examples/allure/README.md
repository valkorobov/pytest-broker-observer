# Pytest examples with AllureReporter

The `FakeTransport` scenarios in `test_allure_reporter.py` demonstrate sync/async
expectations, timeout diagnostics, JSON payload redaction, and an intentional
failure. The integration tests in `test_kafka.py` and `test_rabbitmq.py` use real
Kafka and RabbitMQ through Docker Compose. Run all commands from the repository root.

## Setup

Install the development and example dependencies:

```shell
uv sync --all-groups --all-extras
```

For the broker-free examples in your own project, the `allure,pytest-asyncio`
extras are sufficient:

```shell
python -m pip install -e '.[allure,pytest-asyncio]'
```

HTML reports use Allure 3.22.1. The `npx` commands below require Node.js with npm;
Node.js 24.19.0 was verified locally, and CI uses Node.js 24. Allure 3 does not
require Java. See [installing Allure 3](https://allurereport.org/docs/v3/install/).
The Python `allure` extra installs the pytest plugin; install the CLI separately.
If you already have `allure` version 3.22.1, replace `npx --yes allure@3.22.1` with
`allure` in the commands below.

## Run and view the report

```shell
uv run pytest -c pyproject.toml examples/allure -m 'not integration' -v --alluredir=artifacts/allure-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-results --single-file --output artifacts/allure-report --report-language en
npx --yes allure@3.22.1 open artifacts/allure-report --host 127.0.0.1 --port 5050
```

Pytest result: **4 passed, 1 skipped**. Allure opens the report in a browser; stop
the server with `Ctrl+C`. Choose another `--port` if it is occupied. With
`--single-file`, you can also open `artifacts/allure-report/index.html` directly
in a browser or share that HTML file with someone else.

`--clean-alluredir` clears the previous run's results in the selected directory.
Use separate directories to keep reports from different runs. Generated
`artifacts/allure-*` files are excluded from Git.

| Test in [test_allure_reporter.py](test_allure_reporter.py) | What to inspect in the report |
| --- | --- |
| `test_sync_success` | A successful expectation and JSON summary without message content. |
| `test_async_parallel_waiters` | Separate summaries for two parallel expectations of the same event. |
| `test_expected_timeout` | Timeout diagnostics in a passing test: `pytest.raises` catches the exception. |
| `test_redacted_payload` | Matched and mismatched JSON bodies in `message-content.json`; the redactor removes `access_token`. |
| `test_intentional_timeout_failure` | Skipped by default; explicitly enabled to produce a real failed result. |

Open the test and its **Event observation summary [FakeTransport: operations]**
step. All JSON attachments use two-space indentation (`indent=2`). An expectation
summary includes `expectation-result.json`; new lifecycle records add
`event-timeline.json`, and mismatches add `mismatches.json`.

In `SUMMARY` mode, one completed wait creates one summary. Normal exit from
`watch()` does not repeat results. The next expectation gets a separate summary
with new results; new watcher failures are reported separately.

The `expectation-result.txt` attachment explains the outcome and reason, elapsed
time, matched and requested event counts, counters, and observation completeness.
When an export contains multiple expectations, the latest appears first. In the
redaction scenario, `message-content.json` contains `operation_id` and `status`
without the token, while the test's original payload remains unchanged.

## Kafka and AllureReporter

The integration example requires running Docker with Compose v2.18+, Python
extras `kafka,allure,pytest`, and the Allure CLI described in Setup. The following
development environment includes all dependencies:

```shell
uv sync --all-groups --all-extras
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml examples/allure/test_kafka.py -m integration -v --alluredir=artifacts/allure-kafka-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-kafka-results --single-file --output artifacts/allure-kafka-report --report-language en
npx --yes allure@3.22.1 open artifacts/allure-kafka-report --host 127.0.0.1 --port 5051
```

Expected result: **2 passed**. The broker is available at `127.0.0.1:9092`;
`up --wait` waits for its health check. The report uses port 5051 so it can remain
open alongside the regular Allure example on port 5050. The standalone HTML file
is `artifacts/allure-kafka-report/index.html`.

| Test in [test_kafka.py](test_kafka.py) | Verifies |
| --- | --- |
| `test_kafka_correlated_success` | The watcher is ready before the action; an independent producer publishes a JSON event with `status="completed"` and an `operation-id` header. The scope finds the event, its partition and offset match the broker acknowledgement, and the report includes its redacted body. |
| `test_kafka_expected_timeout` | A real JSON event with `status="started"` arrives before waiting for `completed` times out. The report includes its redacted body in the successful `started` expectation and in the subsequent mismatch diagnostics. |

The second test remains `PASSED` because its timeout is expected and caught with
`pytest.raises(EventTimeoutError)`. Open
**Event observation summary [KafkaAdapter: <topic>]**, where `<topic>` is the
test's unique topic. `expectation-result.json` contains the `timeout` outcome;
`mismatches.json` shows the unmatched event. New lifecycle records appear in
`event-timeline.json`, and the first expectation's summary includes the READY
timeline.

Both tests decode JSON messages containing `operation_id`, `status`, and a
synthetic `access_token`. Their fixture explicitly enables `REDACTED_PAYLOAD`
with a redactor that removes `access_token`. Open `message-content.json` to
inspect the readable message body: the success scenario shows
`status="completed"`; the timeout scenario shows `status="started"`.
Each bounded message sample includes `expectation_id`, `event_sequence`,
`match_status` (`matched` or `mismatch`), `format`, and the redacted `payload`.
These fields link the content to its expectation and explain whether the
message matched or contributed a mismatch. JSON attachments use `indent=2`.

The accompanying `expectation-result.txt` explains that the second scenario read
an event that did not satisfy the expectation for `completed`. The text does not
disclose message content. The successful Kafka scenario creates one summary;
the timeout scenario creates two, one each for `started` and `completed`.
Normal close does not duplicate them.

The redactor receives an independent copy: the tests assert that their returned
payloads still contain the original token, while report content excludes it.
The explicit disclosure policy applies to matched and mismatched samples under
the same capture and export limits. Without this opt-in, the default
`METADATA_ONLY` policy excludes message content, keys, and headers. The adapter
name and topic appear in summary titles; replace them through
`DiagnosticPolicy.aliases` when needed.

The function-scoped fixture creates a unique topic for each test and deletes it
in `finally`, including on failure. `KafkaAdapterHarness` is a test producer;
call your application instead of `harness.publish(...)` in a black-box test.

Both tests are skipped without `KAFKA_BOOTSTRAP_SERVERS`. If an address is set,
connection errors cause a failure. For another host port, use the same port in
`KAFKA_PORT` when starting Compose and `KAFKA_BOOTSTRAP_SERVERS` when running
pytest. See [local Kafka](../../docs/local-kafka.md#custom-port).

After the run, stop Kafka while preserving broker data:

```shell
docker compose down
```

Viewing an already generated HTML report does not require a running broker.

## RabbitMQ and AllureReporter

The RabbitMQ integration examples require the `rabbitmq,allure,pytest` extras
and the Allure CLI described in Setup. The full development environment
includes them:

```shell
uv sync --all-groups --all-extras
docker compose --profile rabbitmq up --detach --wait --wait-timeout 120 rabbitmq
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest -c pyproject.toml examples/allure/test_rabbitmq.py -m integration -v --alluredir=artifacts/allure-rabbitmq-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-rabbitmq-results --single-file --output artifacts/allure-rabbitmq-report --report-language en
npx --yes allure@3.22.1 open artifacts/allure-rabbitmq-report --host 127.0.0.1 --port 5053
```

Expected result: **2 passed**. The AMQP URL is
`amqp://observer:observer@127.0.0.1:5672/`; the optional management interface is
available at [127.0.0.1:15672](http://127.0.0.1:15672) with the same local
`observer` credentials. The report opens on port 5053. You can also open or
share `artifacts/allure-rabbitmq-report/index.html` without a running broker.

| Test in [test_rabbitmq.py](test_rabbitmq.py) | What to inspect |
| --- | --- |
| `test_rabbitmq_correlated_success` | A JSON `completed` event published after READY, correlated by the `operation-id` header, and included in the report after redaction. |
| `test_rabbitmq_expected_timeout` | A real `started` event is matched first, then retained as a mismatch while waiting for `completed`; the expected timeout leaves the test passing. |

Open **Event observation summary [RabbitMQAdapter: <exchange>]**, where
`<exchange>` is unique to the test. `expectation-result.json` and
`expectation-result.txt` describe the outcome, `mismatches.json` describes the
unsatisfied `completed` expectation, and `message-content.json` contains
readable bodies. Each bounded content sample includes `expectation_id`,
`event_sequence`, `match_status` (`matched` or `mismatch`), `format`, and
`payload`. All JSON attachments use `indent=2`.

The fixture explicitly uses `SUMMARY` and `REDACTED_PAYLOAD` with a redactor
that removes the synthetic `access_token`. Matched and mismatched bodies show
`operation_id` and `status` without the token; assertions verify that the
original returned test values still contain it. The redactor receives an
independent copy. Capture and export budgets still apply. Without explicit
disclosure, the default `METADATA_ONLY` policy omits message bodies, keys, and
headers; the text explanation does not include message content under either
policy. The adapter and exchange appear in summary titles and can be replaced
through `DiagnosticPolicy.aliases`.

The success scenario creates one summary, and the timeout scenario creates two,
one for `started` and one for the expected timeout. Normal close does not
duplicate them. Rechecking `started` uses the retained Journal, rather than
requesting broker replay.

The source is an exchange, and every watcher creates its own exclusive
auto-delete queue. It does not compete with application queues. The fixture
creates and deletes only its unique topic exchange, including on failure;
watcher closure or failure releases the observer-owned queue. The application
action belongs after READY; replace `harness.publish(...)` with that action
when adapting the example. Messages are acknowledged at Journal admission,
independently of matcher outcomes. RabbitMQ observation supports latest only
and does not reconnect.

Without `RABBITMQ_URL`, both tests explicitly skip. A specified but unreachable
broker causes failure. For an external broker, grant exchange binding/read
access and declaration, consumption, and deletion of observer-owned queues;
the harness also requires temporary exchange creation/deletion and publishing.
See [the adapter guide](../../docs/rabbitmq.md) and
[local RabbitMQ](../../docs/local-rabbitmq.md).

Stop only RabbitMQ while preserving its data and any running Kafka service:

```shell
docker compose stop rabbitmq
```

## A report with an intentional failure

Use separate directories to demonstrate a real `FAILED` result:

```shell
uv run pytest -c pyproject.toml examples/allure -m 'not integration' -v --allure-demo-failure --alluredir=artifacts/allure-demo-results --clean-alluredir
```

Expected result: **4 passed, 1 failed**, with pytest exit code **1**. The test
publishes `rejected`, then waits for `completed` without catching the resulting
`EventTimeoutError`. This demonstration failure is opt-in and is not enabled in CI.

Generate and view the collected results in separate commands afterward:

```shell
npx --yes allure@3.22.1 awesome artifacts/allure-demo-results --single-file --output artifacts/allure-demo-report --report-language en
npx --yes allure@3.22.1 open artifacts/allure-demo-report --host 127.0.0.1 --port 5050
```

The failed test includes its traceback and diagnostic JSON attachments. The
portable report is `artifacts/allure-demo-report/index.html`.

## Use this in your tests

Ordinary function-scoped fixtures in [conftest.py](conftest.py) create factories
with `reporters=[AllureReporter()]` and
`DiagnosticPolicy(mode=ReportingMode.SUMMARY)`. The test opens `watch()` before
the service action. For your broker, replace `FakeTransport` with its adapter
and `transport.publish(...)` with a call to your application.

`SUMMARY` includes attachments for successful expectations; the default
`ERRORS_ONLY` emits them only on error. Recording results requires the
`allure-pytest` plugin and `--alluredir`. Pytest determines the test status:
a caught timeout remains `PASSED`.

The reporter completes each summary step before the next `await`. User-created
`allure.step(...)` blocks in the async example also end before `await`. Payload
content is disclosed in the redaction example and the Kafka and RabbitMQ scenarios through
an explicit `REDACTED_PAYLOAD` policy and redactor. See
[diagnostics and safe reports](../../docs/diagnostics.md) for export policies
and limits.

## CI

The `rabbitmq` job starts the optional local RabbitMQ profile, runs contracts
and pytest examples, then generates the English report from `test_rabbitmq.py`.
It retains `allure-rabbitmq-example-report` for seven days, including reports
from failed test runs, and stops the broker after the job.

The `FakeTransport` scenarios are included in the main Python matrix. The
`allure-example` job runs them with the real Allure plugin, generates HTML, and
retains the `allure-example-report` artifact for seven days. Download and extract
that artifact from the GitHub Actions run, then open `index.html` in a browser.

The `kafka` job runs `test_kafka.py` against the broker from the same Compose file,
generates a separate report, and retains `allure-kafka-example-report` for seven
days. Report generation and upload also run when the Kafka Allure test fails.
