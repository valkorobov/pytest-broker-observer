# Примеры использования с pytest

Все команды ниже выполняются из корня репозитория. Для полного набора примеров
подготовьте development-окружение:

```shell
uv sync --all-groups --all-extras
uv run pytest -c pyproject.toml examples/pytest -m 'not integration' -v
```

Девять сценариев используют `FakeTransport` и выполняются без брокера. Он
имитирует события тестируемого сервиса: настоящую публикацию в приложении
выполняет ваш сервис или отдельный producer. Это примеры API наблюдения,
а не интеграционная проверка Kafka.

| Файл | Что показывает |
| --- | --- |
| [conftest.py](conftest.py) | Function-scoped factory fixtures, yield fixture активного watcher и имитация действия сервиса. |
| [test_sync.py](test_sync.py) | Обычные `def test_*`, корреляция по header, envelope с key/headers и `pytest.mark.parametrize`. |
| [test_async.py](test_async.py) | `pytest.mark.asyncio`, `async with`, параллельные waiters и чтение истории через `wait_for_count`. |
| [test_protobuf.py](test_protobuf.py) | `message_type`, `field_equals`, явный resolver смешанных типов и guard `on_type`. |
| [test_kafka.py](test_kafka.py) | Реальный брокер, уникальный topic, независимый producer и cleanup через yield fixture. |

## Обычные синхронные тесты

Для запуска только синхронных raw-примеров достаточно pytest и базовой
библиотеки; `pytest-asyncio`, Kafka и Protobuf не нужны:

```shell
python -m pip install -e '.[pytest]'
python -m pytest examples/pytest/test_sync.py -v
```

В `conftest.py` fixture `sync_events` создаёт ленивую factory. Сам тест открывает
наблюдение через `with sync_events.watch(...)`. Контекст возвращается после READY,
поэтому действие сервиса вызывается внутри `with`, после создания scope.

Fixture `watched_events` показывает другой вариант: `watch()` открывается до
теста, watcher передаётся через `yield`, а контекст закрывается при teardown,
включая случай неуспешного assertion. Параметризованный тест получает отдельные
transport, watcher и историю для каждого значения параметра.

`perform_operation` — обычный callback fixture, вызываемый из тела теста.
В своём проекте замените его вызовом вашего service client. Library matchers
и decoders при этом остаются короткими callbacks без I/O.

## Асинхронные тесты

Нужен extra `pytest-asyncio`:

```shell
python -m pip install -e '.[pytest-asyncio]'
python -m pytest -c pyproject.toml examples/pytest/test_async.py -v
```

Async factory тоже предоставляется обычной `pytest.fixture`: ресурсы открываются
в `async with` внутри теста. Параллельные ожидания создаются через
`asyncio.create_task` и завершаются через `asyncio.gather`. Они могут увидеть одну
и ту же запись; найденное событие не удаляется из Journal.

Общий `pytest.ini` в этой папке содержит только опции pytest, поэтому raw-примеры
работают и без async plugin. Команды async/full-прогона явно используют
`pyproject.toml`, где настроены strict mode и function loop scope. При переносе
примеров в собственный проект задайте соответствующие настройки pytest-asyncio
в его конфигурации.

## Protobuf без генерации дополнительной схемы

```shell
python -m pip install -e '.[pytest,protobuf]'
python -m pytest examples/pytest/test_protobuf.py -v
```

Для самостоятельного запуска используются готовые generated-классы
`StringValue` и `Int32Value` из пакета Protobuf. Для своей схемы передавайте
класс из вашего `*_pb2.py` так же, как `message_type=StringValue` в примере.
Открытая схема `OperationEvent` находится в [../proto/example.proto](../proto/example.proto).

В смешанном потоке тип выбирается по уникальному header `event-type`.
`on_type(Int32Value, ...)` пропускает неподходящий тип, не вызывая вложенный
matcher. Тип не угадывается по бинарному содержимому.

## Отдельный пример с настоящей Kafka

Укажите адрес отдельного тестового брокера с разрешениями на создание и
удаление topics. Пример создаёт уникальный topic и удаляет его в `finally`.

```shell
uv sync --all-groups --all-extras
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest -c pyproject.toml examples/pytest/test_kafka.py -m integration -v
```

Без переменной `KAFKA_BOOTSTRAP_SERVERS` этот пример пропускается. Заданный,
но недоступный брокер вызывает ошибку. Переменная окружения принадлежит
тестовому проекту: сам Observer не ищет настройки подключения автоматически.

`KafkaAdapterHarness` предоставляет независимый producer и дожидается broker
acknowledgement. В обычном black-box тесте вместо `harness.publish(...)` вызывают
реальное приложение, а в `SyncEventObserver` явно передают `KafkaAdapter` с
`KafkaConfig`. Настройки TLS/SASL и политика replay описаны в
[руководстве Kafka](../../docs/kafka.md).

Создавайте scope до действия. `ScopeStart.from_now()` фиксирует следующую
позицию admission в runtime, а не время публикации сообщения в брокере.
`wait_for_count(..., 2)` считает разные Journal records, даже если payload
одинаков. Последовательные `wait_for` сами по себе не проверяют порядок событий.
