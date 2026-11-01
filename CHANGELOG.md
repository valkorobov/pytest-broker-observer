# Изменения

## 0.1.0 — кандидат, не опубликован

Первый MVP по [SPEC.md](SPEC.md), с именем дистрибутива
`pytest-broker-observer` и пространством импортов `event_observer`.

Добавлены:

- Общее async ядро и sync facade с непрерывно работающим runtime loop.
- Ленивые factories, readiness, lifecycle, controlled cleanup и typed errors.
- Независимые Kafka sessions через `confluent-kafka`, явные стартовые границы,
  timestamp replay и обнаружение изменений topology.
- Ограниченный Journal с тремя overflow policies и conservative loss ledger.
- `wait_for`, `wait_for_event`, `wait_for_count`, scoped correlation и вложенность.
- Raw/Protobuf decoders, явные type resolvers, payload isolation и matchers
  с трёхзначной логикой.
- Ограниченная безопасная диагностика, opt-in Allure и пассивный W3C trace context.
- FakeTransport, AdapterTestHarness и переиспользуемые contract suites.
- Типизация с `py.typed`, optional extras, пассивный pytest plugin, wheel/sdist
  packaging, документация и CI workflow.
- Pre-commit проверки Ruff, форматирования и Pyrefly с версиями из `uv.lock`;
  тот же набор запускается в CI.
- Запускаемые pytest-примеры с fixtures, параметризацией, sync/async API,
  Protobuf и отдельным opt-in Kafka-сценарием; примеры включены в CI.

Temporal assertions, дополнительные брокеры, Schema Registry и checkpoint
reconnect отложены. Подтверждённые результаты и незакрытые release gates —
в [docs/status.md](docs/status.md); проверенные комбинации зависимостей —
в [docs/compatibility.md](docs/compatibility.md).
