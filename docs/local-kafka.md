# Локальная Kafka через Docker Compose

В корне проекта находится [compose.yaml](../compose.yaml) с одним брокером
Apache Kafka 3.9.2 в режиме KRaft. Отдельный ZooKeeper не нужен. Образ закреплён
по SHA-256 manifest; CI использует этот же Compose-файл.

Требуется запущенный Docker и Compose v2.18+ с `up --wait --wait-timeout`.
Все команды выполняются из корня репозитория.

## Запуск и readiness

```shell
uv sync --all-groups --all-extras
docker compose up --detach --wait --wait-timeout 120
docker compose ps
```

`up --wait` возвращается после успешного health check: Kafka CLI получает список
topics через внутренний listener. Брокер доступен тестам на хосте по адресу
`127.0.0.1:9092`. Порт публикуется только на loopback-интерфейсе хоста.
Внутри Compose-сети адрес брокера — `kafka:29092`; controller-порт на хост
не публикуется. Локальный профиль использует PLAINTEXT.

Health check проверяет готовность брокера. Вход в `watch()` дополнительно
подтверждает готовность конкретного reader по контракту библиотеки.

## Интеграционные тесты и pytest-пример

Интеграционные проверки библиотеки вместе с Kafka-примером без Allure:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml tests/integration examples/pytest/test_kafka.py -v
```

Только интеграционные тесты библиотеки:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest tests/integration -v
```

Только пример использования с pytest:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml examples/pytest/test_kafka.py -m integration -v
```

Harness создаёт уникальные topics и удаляет только свои ресурсы при teardown.
Автоматическое создание topics брокером отключено: тестовые sources создаёт harness.
Без `KAFKA_BOOTSTRAP_SERVERS` Kafka-сценарии явно пропускаются. Указанный,
но недоступный брокер вызывает ошибку. Настройки подключения передаются
библиотеке явно; чтение этой переменной — ответственность тестовых fixtures.

## Kafka с отчётом Allure

Два интеграционных примера в [examples/allure/test_kafka.py](../examples/allure/test_kafka.py)
используют `AllureReporter`: успешную корреляцию события и ожидаемый timeout
с несовпавшим сообщением. На уже запущенном брокере:

```shell
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml examples/allure/test_kafka.py -m integration -v --alluredir=artifacts/allure-kafka-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-kafka-results --single-file --output artifacts/allure-kafka-report --report-language ru
npx --yes allure@3.22.1 open artifacts/allure-kafka-report --host 127.0.0.1 --port 5051
```

Ожидается **2 passed**. Нужен Node.js с npm для CLI Allure; development-окружение
из команды `uv sync` выше включает Python-зависимости. Полная подготовка,
описание вложений и просмотр HTML без работающей Kafka — в
[руководстве Kafka и AllureReporter](../examples/allure/README.md#kafka-и-allurereporter).
В CI job `kafka` сохраняет этот отчёт как `allure-kafka-example-report` на 7 дней.

## Другой порт

Если 9092 занят, выберите свободный порт и используйте его в обеих командах:

```shell
export KAFKA_PORT=19092
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 uv run pytest -c pyproject.toml tests/integration examples/pytest/test_kafka.py -v
docker compose down
unset KAFKA_PORT
```

`KAFKA_PORT` изменяет и published port, и advertised listener Kafka. Это
позволяет клиенту подключиться к адресу из metadata после bootstrap.
Внутренний listener и health check сохраняют адрес `kafka:29092`.
Тесты не получают адрес автоматически из Compose: при изменении порта
обновите также `KAFKA_BOOTSTRAP_SERVERS`.

## Хранение данных и остановка

```shell
docker compose down
```

Команда останавливает и удаляет контейнер и сеть. Данные брокера остаются в
именованном volume `kafka-data` этого Compose-проекта. Постоянный `CLUSTER_ID`
и каталог `/var/lib/kafka/data` позволяют снова запустить тот же брокер
с сохранёнными данными через `docker compose up --detach --wait --wait-timeout 120`.

Для полного сброса локального брокера, включая все его topics и записи:

```shell
docker compose down --volumes
```

Для одновременных независимых стендов задайте разное имя проекта через
`docker compose -p my-kafka ...` и отдельный `KAFKA_PORT` для каждого стенда.
То же имя `-p` используйте для запуска, просмотра логов и остановки.

## Проверка проблем запуска

```shell
docker compose ps
docker compose logs --no-color --tail 200 kafka
```

При конфликте порта выберите другой `KAFKA_PORT`. После изменения listeners
повторный `up` пересоздаёт контейнер с новой конфигурацией; volume сохраняется.
Если изменяете cluster ID или storage-настройки для нового тестового стенда,
используйте новый Compose-проект или явно сбросьте его данные.

Профиль предназначен для локальных тестов с одним брокером. Проверки нескольких
брокеров, ACL/TLS и fault-proxy остаются отдельными профилями из
[статуса реализации](status.md).

## Подтверждённая проверка

На macOS arm64 с CPython 3.14.7 и Docker Compose 5.5.1 прошли все 23 сценария:
интеграционные тесты библиотеки и Kafka pytest-пример. Проверены запуск на
нестандартном host port, health check, чтение сохранённой записи после
`down`/`up` и удаление только временного Compose-проекта и его volumes.
Отдельно прошли два Kafka-сценария с AllureReporter: успешная корреляция и
ожидаемый timeout; JSON-вложения проверены, самостоятельный HTML-отчёт собран.
Это локальная проверка профиля; успешный запуск GitHub Actions подтверждается
отдельно результатами CI.
