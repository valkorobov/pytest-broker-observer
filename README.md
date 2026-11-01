# pytest-broker-observer

Библиотека для black-box проверок событий из брокера в обычных и асинхронных
Python-тестах. Observer подключается **до тестового действия**, сохраняет
сообщения в ограниченном Journal и позволяет ожидать нужный результат без
собственного consumer loop.

Дистрибутив называется `pytest-broker-observer`; импорт — `event_observer`,
согласно [SPEC.md](SPEC.md). Текущая версия — локальный MVP-кандидат 0.1.0.
Подтверждённые проверки и оставшиеся release gates описаны в
[статусе реализации](docs/status.md) и [матрице совместимости](docs/compatibility.md).

## Установка из этого проекта

Требуется Python 3.11–3.14. Для Kafka, Protobuf и обычного pytest:

```shell
python -m pip install -e '.[kafka,protobuf,pytest]'
```

Для разработки со всеми optional dependencies и инструментами:

```shell
uv sync --all-groups --all-extras
```

Core не имеет runtime dependencies и работает вне pytest. Extras подключают
реализованные возможности: `kafka`, `protobuf`, `pytest`, `pytest-asyncio`,
`allure`, `contract-tests` и `all`. Pytest plugin пассивен: не создаёт fixtures,
подключений или фоновых потоков при autoload. Синхронным тестам не нужен
`pytest-asyncio`.

## Синхронный пример без брокера

Этот пример запускается после локальной установки и показывает обычный
блокирующий API:

```python
from event_observer import SyncEventObserver
from event_observer.testing import FakeTransport

transport = FakeTransport()
events = SyncEventObserver(transport=transport)

with events.watch(source="events") as watcher:
    transport.publish(b"operation-completed")
    result = watcher.wait_for(
        lambda payload: payload == b"operation-completed",
        timeout=1.0,
    )
    assert result == b"operation-completed"
```

`FakeTransport.publish()` — вспомогательный инструмент тестов. Production API
библиотеки наблюдает события; публикацию выполняет ваша система или отдельный
тестовый producer.

## Асинхронный пример

```python
import asyncio

from event_observer import EventObserver
from event_observer.testing import FakeTransport


async def main():
    transport = FakeTransport()
    async with EventObserver(transport=transport).watch(source="events") as watcher:
        transport.publish(b"operation-completed")
        observed = await watcher.wait_for_event(
            lambda payload: payload == b"operation-completed",
            timeout=1.0,
        )
        assert observed.payload == b"operation-completed"
        assert observed.raw.source == "events"
        print(observed.sequence)


asyncio.run(main())
```

Оба facade используют одно ядро. В sync режиме отдельный event loop непрерывно
читает сообщения, пока тест выполняет блокирующее действие сервиса.

## Kafka, Protobuf и корреляция

Готовые запускаемые pytest-примеры с обычными и yield fixtures, параметризацией,
sync/async ожиданиями и Protobuf находятся в [examples/pytest](examples/pytest/README.md).
Для запуска без брокера из корня проекта:

```shell
uv run pytest -c pyproject.toml examples/pytest -m 'not integration' -v
```

Создавайте watcher до вызова тестируемой системы. Вход в контекст дожидается
readiness; по умолчанию чтение начинается с зафиксированных `latest` границ.

```python
from uuid import uuid4

from event_observer import ScopeStart, SyncEventObserver
from event_observer.matchers import field_equals
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig
from example_pb2 import OperationEvent, COMPLETED


def test_operation(service_client):
    operation_id = uuid4().hex
    events = SyncEventObserver(
        transport=KafkaAdapter(KafkaConfig(bootstrap_servers="localhost:9092")),
    )
    with events.watch(
        source="example.events",
        message_type=OperationEvent,
    ) as watcher:
        operation = watcher.scope(
            field_equals("operation_id", operation_id),
            start=ScopeStart.from_now(),
        )
        service_client.perform(operation_id=operation_id)
        completed = operation.wait_for(
            field_equals("status", COMPLETED),
            timeout=10.0,
        )
        assert completed.operation_id == operation_id
```

`service_client` — fixture вашего проекта. `example_pb2` генерируется вашим
Protobuf toolchain; открытая схема примера находится в
[examples/proto/example.proto](examples/proto/example.proto).
Observer не угадывает тип по wire format. Для mixed-type потока доступны явные
header/source/callable resolvers и `on_type(T, matcher)`.

## Pytest и Allure

В [examples/allure](examples/allure/README.md) есть запускаемые примеры с
`AllureReporter`: успешные sync/async ожидания, timeout и redaction JSON payload.
После `uv sync --all-groups --all-extras`:

```shell
uv run pytest -c pyproject.toml examples/allure -m 'not integration' -v --alluredir=artifacts/allure-results --clean-alluredir
npx --yes allure@3.22.1 awesome artifacts/allure-results --single-file --output artifacts/allure-report --report-language ru
npx --yes allure@3.22.1 open artifacts/allure-report --host 127.0.0.1 --port 5050
```

Для генерации и просмотра нужен Node.js с npm; проверен Node.js 24.
Обычный прогон даёт **4 passed, 1 skipped**. Отдельный `--allure-demo-failure`
включает настоящую демонстрационную ошибку; команды и ожидаемый код выхода
описаны в [руководстве](examples/allure/README.md). Отчёт можно открыть и как
самостоятельный `artifacts/allure-report/index.html`.

Для настоящего брокера есть [Kafka-примеры с AllureReporter](examples/allure/README.md#kafka-и-allurereporter):
запуск Kafka через Compose, успешное ожидание с корреляцией и диагностика timeout.
Руководство содержит полный прогон и просмотр отдельного HTML-отчёта.

## Что входит в MVP

- Async и sync API: `wait_for`, `wait_for_event`, `wait_for_count`, вложенные scopes.
- Kafka через `confluent-kafka`: независимые sessions, ручное назначение partitions,
  `latest`, `earliest`, явные offsets и timestamp start.
- Raw bytes и null payload, обычный Protobuf, явное разрешение смешанных типов.
- Ограниченный Journal с `FAIL`, `DROP_OLDEST`, `DROP_NEWEST` и учётом неполноты.
- Структурированные matchers, predicates и независимые копии mutable payload.
- Ограниченная диагностика, opt-in Allure и пассивное извлечение W3C trace context.
- Fake adapter и переиспользуемые contract suites для пользовательских adapters.

Каждое ожидание читает историю самостоятельно и не потребляет события.
`wait_for_count(..., N)` возвращает первые N различных Journal records с одним
общим deadline; брокерные дубликаты остаются отдельными записями.

По умолчанию ожидания требуют полного диапазона. Известная потеря при DROP
даёт `IncompleteObservationError`, а terminal background failure —
`ObserverFailedError`, в том числе при нормальном выходе из контекста.
`ScopeStart.from_now()` явно выбирает новый диапазон после старой потери.

Timeout определяет допустимое время admission события в Journal. Финальная
обработка уже принятых сообщений может закончиться позже. Пользовательские
callbacks должны быть короткими и без I/O; в sync режиме они выполняются в
runtime thread. Активный sync handle принадлежит создавшему его потоку и не
используется внутри работающего async loop.

Бюджет Journal учитывает память по опубликованной модели, но не ограничивает
RSS всего процесса. Kafka partitions не имеют общего порядка. Последовательные
`wait_for` не доказывают порядок событий; temporal assertions, другие брокеры,
Schema Registry и reconnect с восстановлением checkpoints отложены за пределы
этого MVP.

## Руководства

- [Локальная Kafka: Docker Compose, запуск тестов, порты и данные](docs/local-kafka.md)
- [Kafka: конфигурация, readiness и стартовые позиции](docs/kafka.md)
- [Matchers и правила Protobuf](docs/matchers.md)
- [Scopes, корреляция и полнота](docs/scopes.md)
- [Диагностика, redaction, Allure и trace context](docs/diagnostics.md)
- [Запуск pytest с AllureReporter и просмотр HTML-отчёта](examples/allure/README.md)
- [Совместимость](docs/compatibility.md) и [проверенный статус](docs/status.md)
- [Техническая спецификация](SPEC.md), [изменения](CHANGELOG.md),
  [разработка](CONTRIBUTING.md), [безопасность](SECURITY.md)

## Проверки и сборка

После подготовки окружения:

```shell
uv run pytest -m 'not integration'
uv run pytest -n 2 -m 'not integration'
uv run ruff check .
uv run ruff format --check .
uv run pyrefly check
uv run pyright
uv build --no-sources
uv run twine check dist/*
```

Для локальной Kafka требуется Docker Compose v2.18+. Запустите брокер и
дождитесь health check:

```shell
docker compose up --detach --wait --wait-timeout 120
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest -c pyproject.toml tests/integration examples/pytest/test_kafka.py -v
```

Compose поднимает Kafka 3.9.2 в режиме KRaft на `127.0.0.1:9092`.
Интеграционные тесты и pytest-пример создают и удаляют свои уникальные topics.
Остановить локальный брокер с сохранением данных можно командой `docker compose down`.
Выбор порта, сброс данных и запуск отдельных наборов описаны в
[руководстве локальной Kafka](docs/local-kafka.md).

При отсутствии этой переменной integration tests пропускаются. Если брокер
указан, ошибка подключения считается failure. Наличие команды или CI workflow
не означает, что соответствующая среда уже проверена: результаты фиксируются
в [статусе](docs/status.md).
