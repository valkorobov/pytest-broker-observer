# Pytest-примеры с AllureReporter

В `test_allure_reporter.py` сценарии с `FakeTransport` показывают sync/async
ожидания, диагностику timeout, очистку JSON payload и намеренное падение.
В `test_kafka.py` два интеграционных теста используют настоящую Kafka из
Docker Compose. Все команды выполняются из корня репозитория.

## Подготовка

Установите зависимости для разработки и примеров:

```shell
uv sync --all-groups --all-extras
```

Для своего проекта достаточно extras `allure,pytest-asyncio`:

```shell
python -m pip install -e '.[allure,pytest-asyncio]'
```

HTML строится с помощью Allure 3.22.1. Для команд с `npx` ниже нужен Node.js
с npm; локально проверен Node.js 24.19.0, в CI используется Node.js 24.
Java для Allure 3 не требуется. См. [установку Allure 3](https://allurereport.org/docs/v3/install/).
Python extra `allure` устанавливает pytest-плагин, CLI устанавливается отдельно.
Если у вас уже установлен `allure` версии 3.22.1, замените
`npx --yes allure@3.22.1` на `allure` в командах ниже.

## Обычный прогон и просмотр отчёта

```shell
uv run pytest -c pyproject.toml examples/allure -m 'not integration' -v --alluredir=artifacts/allure-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-results --single-file --output artifacts/allure-report --report-language ru
npx --yes allure@3.22.1 open artifacts/allure-report --host 127.0.0.1 --port 5050
```

Результат pytest: **4 passed, 1 skipped**. Allure откроет отчёт в браузере;
сервер останавливается через `Ctrl+C`. Если порт занят, укажите другой `--port`.
Благодаря `--single-file` можно также открыть `artifacts/allure-report/index.html`
непосредственно в браузере или передать этот HTML другому человеку.

`--clean-alluredir` очищает результаты предыдущего прогона в указанной папке.
Для разных прогонов используйте разные папки, если хотите сохранить оба отчёта.
Сгенерированные `artifacts/allure-*` исключены из Git.

| Тест в [test_allure_reporter.py](test_allure_reporter.py) | Что смотреть в отчёте |
| --- | --- |
| `test_sync_success` | Успешное ожидание и JSON summary без содержимого сообщения. |
| `test_async_parallel_waiters` | Отдельные summary для двух параллельных ожиданий одного события. |
| `test_expected_timeout` | Диагностика timeout при зелёном тесте: исключение перехвачено через `pytest.raises`. |
| `test_redacted_payload` | Несовпавшее JSON-сообщение в `message-content.json`; поле `access_token` удалено redactor. |
| `test_intentional_timeout_failure` | По умолчанию пропущен; включается отдельно для настоящего красного результата. |

Откройте тест и его step **Event observation summary [FakeTransport: operations]**.
Все JSON-вложения форматируются с отступом в два пробела (`indent=2`). Summary ожидания содержит
`expectation-result.json` и `event-timeline.json`; при несовпадениях добавляется
`mismatches.json`.
В redaction-сценарии `message-content.json` содержит `operation_id` и `status`,
но не токен; оригинальный payload теста сохраняется без изменений.

## Kafka и AllureReporter

Для интеграционного примера нужны запущенный Docker с Compose v2.18+,
Python extras `kafka,allure,pytest` и CLI Allure из раздела подготовки.
Development-окружение ниже уже включает все зависимости:

```shell
uv sync --all-groups --all-extras
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml examples/allure/test_kafka.py -m integration -v --alluredir=artifacts/allure-kafka-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-kafka-results --single-file --output artifacts/allure-kafka-report --report-language ru
npx --yes allure@3.22.1 open artifacts/allure-kafka-report --host 127.0.0.1 --port 5051
```

Ожидается **2 passed**. Брокер доступен по `127.0.0.1:9092`; `up --wait`
дожидается health check. Для отчёта выбран порт 5051, чтобы его можно было
смотреть одновременно с обычным Allure-примером на 5050. Самостоятельный HTML:
`artifacts/allure-kafka-report/index.html`.

| Тест в [test_kafka.py](test_kafka.py) | Что проверяет |
| --- | --- |
| `test_kafka_correlated_success` | Watcher готов до действия; независимый producer публикует `completed` с `operation-id`. Scope находит событие, partition и offset совпадают с подтверждением брокера. |
| `test_kafka_expected_timeout` | Сначала принята настоящая запись `started`, затем ожидание `completed` завершается timeout. В отчёте остаётся диагностика несовпадения. |

Второй тест остаётся `PASSED`, поскольку timeout ожидается и перехватывается
через `pytest.raises(EventTimeoutError)`. Откройте **Event observation summary [KafkaAdapter: <topic>]**,
где `<topic>` — уникальный topic этого теста:
`expectation-result.json` содержит исход `timeout`, `mismatches.json` показывает
несовпавшее событие, `event-timeline.json` — lifecycle наблюдения.
В обоих тестах используется `SUMMARY` с политикой `METADATA_ONLY`: содержимое
сообщений, key и headers не раскрываются в JSON-экспорте.
Имя адаптера и topic видны в заголовках summary; при необходимости их можно
заменить через `DiagnosticPolicy.aliases`.

Function-scoped fixture создаёт уникальный topic для каждого теста и удаляет
его в `finally`, включая случай ошибки. `KafkaAdapterHarness` — тестовый producer;
в black-box тесте вместо `harness.publish(...)` вызовите ваше приложение.

Без `KAFKA_BOOTSTRAP_SERVERS` оба теста пропускаются. Если адрес задан, ошибка
подключения считается failure. При другом host port задайте одинаковый порт
в `KAFKA_PORT` при запуске Compose и в `KAFKA_BOOTSTRAP_SERVERS` при запуске pytest;
подробнее — [локальная Kafka](../../docs/local-kafka.md#другой-порт).

После прогона Kafka можно остановить, сохранив данные брокера:

```shell
docker compose down
```

Просмотр уже собранного HTML не требует работающего брокера.

## Отчёт с намеренной ошибкой

Для демонстрации настоящего `FAILED` используйте отдельные папки:

```shell
uv run pytest -c pyproject.toml examples/allure -m 'not integration' -v --allure-demo-failure --alluredir=artifacts/allure-demo-results --clean-alluredir
```

Ожидается **4 passed, 1 failed**, код выхода pytest — **1**. Тест публикует
`rejected`, а затем без перехвата ожидает `completed` и получает
`EventTimeoutError`. Это демонстрационное падение, поэтому флаг не включён в CI.

После этого отдельно запустите генерацию и просмотр уже собранных результатов:

```shell
npx --yes allure@3.22.1 awesome artifacts/allure-demo-results --single-file --output artifacts/allure-demo-report --report-language ru
npx --yes allure@3.22.1 open artifacts/allure-demo-report --host 127.0.0.1 --port 5050
```

В красном тесте доступны traceback и диагностические JSON-вложения.
Готовый переносимый файл — `artifacts/allure-demo-report/index.html`.

## Как подключить к своим тестам

В [conftest.py](conftest.py) обычные function-scoped fixtures создают factories
с `reporters=[AllureReporter()]` и `DiagnosticPolicy(mode=ReportingMode.SUMMARY)`.
Тест открывает `watch()` до действия сервиса. Для своего брокера замените
`FakeTransport` на адаптер, а `transport.publish(...)` — на вызов приложения.

`SUMMARY` включает вложения при успешных ожиданиях; значение по умолчанию
`ERRORS_ONLY` отправляет их только при ошибке. Для записи результатов нужен
pytest-плагин `allure-pytest` и флаг `--alluredir`. Статус самого теста определяет
pytest: перехваченный timeout остаётся `PASSED`.

Репортёр завершает каждый summary step до следующего `await`. Пользовательские
`allure.step(...)` в async-примере также не пересекают `await`. Payload раскрывается
только в отдельном redaction-примере с явной политикой `REDACTED_PAYLOAD`.
Подробнее о лимитах и политике экспорта:
[диагностика и безопасные отчёты](../../docs/diagnostics.md).

## CI

Сценарии с `FakeTransport` включены в основную матрицу Python. Job `allure-example`
запускает их с настоящим Allure-плагином, строит HTML и сохраняет артефакт
`allure-example-report` на 7 дней. Скачайте и распакуйте артефакт из запуска
GitHub Actions, затем откройте `index.html` в браузере.

Job `kafka` запускает `test_kafka.py` на брокере из того же Compose-файла,
строит отдельный отчёт и сохраняет `allure-kafka-example-report` на 7 дней.
Генерация и сохранение отчёта выполняются и при падении Kafka Allure-теста.
