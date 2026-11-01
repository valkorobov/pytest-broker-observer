# Проверка адаптера

Core не зависит от pytest. Публичные testing helpers подключаются явно:

```python
from event_observer.testing.contracts import (
    IsolationContract,
    LifecycleContract,
    ReadinessContract,
)


class TestMyAdapter(LifecycleContract, ReadinessContract, IsolationContract):
    pass
```

Проект адаптера предоставляет async fixture `adapter_harness` по Protocol
`event_observer.testing.harness.AdapterTestHarness`. Каждый contract method —
отдельный pytest node. Fixture создаёт независимый publisher и очищает только
свои источники в `finally`. Никакие подключения не создаются при collection.
Каждая session предоставляет один receive iterator на весь свой срок жизни.

`FakeTransport` поддерживает READY barrier, немедленную доставку, контролируемые
failure/end, ограниченный handoff и stalled close. Публиковать можно из потока
sync-теста: в runtime отправляется не более одного отложенного wakeup на session.
История самого fake используется для replay тестовых сценариев; он не является
production-брокером или гарантией ограниченной памяти test publisher.

Для проверки существующего Kafka-брокера предназначен `KafkaAdapterHarness`:

```sh
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 uv run pytest tests/integration -v
```

Базовые контракты проверяют READY, отсутствие скрытых commits/конкуренции за
доставку в пределах заявленных capabilities, пустой источник, history replay,
null/empty payload, headers/key, независимость readers и lifecycle. Отдельные
Kafka integration tests проверяют explicit offsets, timestamp fallback,
partition expansion и открытую транзакцию через latest boundary.

Harness создаёт уникальные topics и удаляет только зарегистрированные им имена.
Producer receipt означает delivery acknowledgement, а не помещение в локальную
очередь producer. Невозможность подключения к явно указанному брокеру — failure.
Без переменной окружения реальные интеграционные nodes имеют явный skip.

Native-client doubles проверяют редкие interleavings и ошибки без брокера.
Они не подменяют тесты Fetch/ACL/транзакций на настоящей Kafka. Отдельные fault
extension Protocols не содержат молча работающих no-op implementations.

Временные тесты ядра используют injected clock, версии журнала и barriers;
граница deadline проверяется до, точно на и после admission timestamp.
Отдельно проверены lost wakeup, error-before-commit, 100 waiters с отменами,
conservative loss compaction, process/thread/loop affinity и повторный sync
lifecycle. Callbacks работают в runtime, а не в потоке sync-теста.

Полные broker faults, Toxiproxy и benchmarks перечислены как оставшиеся gates в
[status.md](status.md); CI-конфигурация не является свидетельством их выполнения.
