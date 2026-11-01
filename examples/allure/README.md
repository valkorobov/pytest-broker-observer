# Pytest examples with AllureReporter

The `FakeTransport` scenarios in `test_allure_reporter.py` demonstrate sync/async
expectations, timeout diagnostics, JSON payload redaction, and an intentional
failure. The two integration tests in `test_kafka.py` use real Kafka through
Docker Compose. Run all commands from the repository root.

## Setup

Install the development and example dependencies:

```shell
uv sync --all-groups --all-extras
```

For your own project, the `allure,pytest-asyncio` extras are sufficient:

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
content is disclosed in the redaction example and the Kafka scenarios through
an explicit `REDACTED_PAYLOAD` policy and redactor. See
[diagnostics and safe reports](../../docs/diagnostics.md) for export policies
and limits.

## CI

The `FakeTransport` scenarios are included in the main Python matrix. The
`allure-example` job runs them with the real Allure plugin, generates HTML, and
retains the `allure-example-report` artifact for seven days. Download and extract
that artifact from the GitHub Actions run, then open `index.html` in a browser.

The `kafka` job runs `test_kafka.py` against the broker from the same Compose file,
generates a separate report, and retains `allure-kafka-example-report` for seven
days. Report generation and upload also run when the Kafka Allure test fails.
