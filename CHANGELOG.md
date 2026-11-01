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
- Docker Compose с локальной Kafka в режиме KRaft, readiness health check,
  настраиваемым портом и постоянным volume; общий профиль для локальных тестов и CI.
- Pytest-примеры с AllureReporter: sync/async успех, timeout, redaction и opt-in
  намеренное падение; просмотр самостоятельного HTML-отчёта и артефакт в CI.
- Интеграционные Kafka-примеры с AllureReporter: корреляция успешного события,
  диагностика ожидаемого timeout, локальный запуск через Compose и отдельный
  HTML-отчёт с артефактом в Kafka CI job.
- JSON-вложения отчёта с `indent=2` и учётом форматирования в лимитах; заголовки
  Allure summary с транспортом и топиком/стримом, с поддержкой aliases.

Temporal assertions, дополнительные брокеры, Schema Registry и checkpoint
reconnect отложены. Подтверждённые результаты и незакрытые release gates —
в [docs/status.md](docs/status.md); проверенные комбинации зависимостей —
в [docs/compatibility.md](docs/compatibility.md).
