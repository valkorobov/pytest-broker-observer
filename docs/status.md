# Статус MVP и условия публикации

Это работающий кандидат MVP 0.1.0 по SPEC.md, а не объявление опубликованного
или прошедшего все release gates релиза. Спецификация сохранена без изменений.

Реализованы общее async-ядро и sync facade, ограниченный Journal и loss ledger,
независимые expectations/scopes, raw и Protobuf decoders/resolvers,
трёхзначные matchers, Kafka adapter, безопасная диагностика и явный Allure
reporter, passive pytest plugin, reusable transport contracts, packaging и CI.

Дистрибутив называется `pytest-broker-observer` по заданию владельца;
пространство импортов `event_observer` соответствует спецификации.

## Воспроизводимые проверки

Локальный прогон 1 ноября 2026 года: CPython 3.14.7, macOS arm64.
Конкретные версии зависимостей: [compatibility.json](compatibility.json).

| Проверка | Результат |
| --- | --- |
| Unit/Core/parity/contracts/reliability/reporting | 294 passed |
| pytest-xdist, два процесса | 294 passed |
| Ruff lint/format и Pyright | Без ошибок и предупреждений |
| Wheel + sdist, Twine metadata check | Собраны, metadata check PASSED |
| Чистая установка wheel без extras | Stdlib-only import, `py.typed` и sync observation прошли |
| Чистая установка sdist без extras | Импорт и `py.typed` прошли |
| Обычный pytest без pytest-asyncio/Kafka/Protobuf/Allure | 1 passed |
| Запускаемые примеры с pytest | 9 passed без брокера; отдельный Kafka-пример — 1 passed; в pytest-only окружении 4 passed, 3 skipped |
| Реальная Kafka 3.9.2 / KRaft / arm64 | 22 passed: общие contracts, replay, topology, transactions, sync/async + Protobuf |
| Локальная Kafka через Compose | 23 passed вместе с pytest-примером; нестандартный host port и сохранение данных после `down`/`up` проверены |
| Pytest-примеры с настоящим AllureReporter | 4 passed, 1 skipped; opt-in демонстрация — 4 passed, 1 намеренный FAILED; JSON-вложения и redaction проверены |
| HTML Allure 3.22.1 / Node.js 24.19.0 | Обычный и демонстрационный отчёты собраны как standalone HTML |
| Kafka-примеры с AllureReporter / Compose | 2 passed на настоящем брокере; без адреса Kafka — 2 skipped |
| Читаемость отчёта Allure | JSON с `indent=2` и лимитами; заголовки с транспортом и топиком/стримом, aliases и изоляция watcher проверены |
| Core вместе с pytest- и Allure-примерами без брокера | 307 passed, 1 skipped, 25 deselected |

Первый real-broker прогон выявил отличие `get_watermark_offsets` при
`read_committed`: клиент вернул LSO вместо физического high watermark.
Исправление использует worker-owned AdminClient ListOffsets с READ_UNCOMMITTED
для boundary, сохраняя READ_COMMITTED у reader. Отдельный native regression и
повторный полный integration run подтверждают исправление; первоначальный
failure не скрыт автоматическим retry.

Эти результаты относятся к локальному кандидату. Они не подменяют полную CI
matrix, проверку dependency floors и оставшиеся release gates ниже.

```sh
uv sync --all-groups --all-extras
uv run pytest -m 'not integration'
uv run pytest -n 2 -m 'not integration'
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv build --no-sources
uv run twine check dist/*
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest tests/integration -v
```

Запуск Allure-примеров, отдельной демонстрационной ошибки и просмотр HTML:
[examples/allure](../examples/allure/README.md).

Без `KAFKA_BOOTSTRAP_SERVERS` реальные Kafka-тесты имеют явный skip.
Заданный, но недоступный брокер вызывает failure. Такие skip не являются
доказательством Kafka-совместимости. Unit-тесты native client используют
управляемую реализацию его интерфейса и обозначены отдельно от real broker.

## Остаток до публикации

- Выбор владельцем лицензии и проверка имени в PyPI.
- Проверенные dependency floors и полная Python/OS/client/broker matrix.
- Полные fault suites: ACL/auth в реальном окружении, advertised endpoints
  через Toxiproxy, broker restart, retention, topic recreation и compaction.
- Полный Testcontainers harness с pinned digest и обязательной привязкой
  каждой заявленной capability к отдельному contract node на реальном брокере.
- Benchmarks RSS/throughput/latency с фиксированным окружением и бюджетами.
- Остальные reporting contexts Allure: примеры проверены локально с настоящим
  pytest-плагином и HTML, отдельный CI job подготовлен; полная runner matrix
  ещё требует подтверждения.
- Protected release environment, подтверждённый OIDC Trusted Publisher,
  attestations, проверка tag/version/hash и публикация проверенных artifacts.

CI подготовлен, но локальные проверки не подменяют успешный запуск CI.
Файл LICENSE намеренно отсутствует до решения владельца; публичная публикация
из этой задачи не выполнялась.

Связь областей SPEC с тестами: [acceptance.md](acceptance.md). Расширение и запуск
transport contracts: [testing.md](testing.md).
