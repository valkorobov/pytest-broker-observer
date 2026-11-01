# Разработка

Исходный контракт — [SPEC.md](SPEC.md). Проверенный статус реализации и
ограничения находятся в [docs/status.md](docs/status.md). Команды ниже
выполняются из корня проекта.

## Подготовка

Используйте Python 3.11–3.14 и установите окружение:

```shell
uv sync --all-groups --all-extras
```

Внутри Git-репозитория установите хуки перед коммитами:

```shell
uv run --locked pre-commit install
```

Хуки проверяют Ruff lint, форматирование Ruff и типы через Pyrefly. Все три
используют версии инструментов из `uv.lock` и проверяют проект целиком.
Файлы автоматически не изменяются; ошибки форматирования исправляйте отдельно.

Базовый Core использует только стандартную библиотеку. Не добавляйте обязательные
импорты Kafka, Protobuf, pytest, Allure или OpenTelemetry в обычный
`import event_observer`. Runtime не должен читать credentials или открывать
подключения до входа в контекст watcher.

## Проверки

```shell
uv run --locked pre-commit run --all-files
uv run pytest -m 'not integration'
uv run pytest -n 2 -m 'not integration'
uv run ruff check .
uv run ruff format --check .
uv run --locked pyrefly check
uv run pyright
uv build --no-sources
uv run twine check dist/*
```

Для исправления форматирования используйте `uv run ruff format .`.
Для Kafka integration tests нужен отдельный тестовый брокер:

```shell
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest -m integration -v
```

Эти тесты создают и удаляют topics. Указанный, но недоступный брокер даёт failure;
отсутствующая переменная окружения означает явный skip. Docker не требуется
для обычных Core tests или работы с уже существующим брокером.

## Изменения Core и adapters

Сохраняйте общий код ожиданий для sync и async facade. Predicate получает
отделённый payload; scope не создаёт consumer; независимые waiters не потребляют
Journal records друг у друга. Known failure и неполнота не должны превращаться
в обычный timeout или успешный результат.

Для race regression используйте управляемый barrier или Clock, а не случайный
sleep. Добавляйте проверку наблюдаемого контракта: причина ошибки, полнота,
независимость readers, освобождение ресурсов или deadline eligibility.

Новый adapter реализует `event_observer.transports.base.TransportAdapter` и
создаёт loop-bound ресурсы внутри `open`. Укажите реальные capabilities и
подключите contracts через собственную fixture `adapter_harness`:

```python
from event_observer.testing.contracts import (
    IsolationContract,
    LifecycleContract,
    ReadinessContract,
)


class TestCustomAdapter(LifecycleContract, ReadinessContract, IsolationContract):
    pass
```

Publisher harness должен подтверждать принятие публикации брокером. Fault,
topology и position controls оформляются отдельными extension protocols.
Не скрывайте непроверенную заявленную capability через skip.

## Документация и выпуск

Документируйте фактическое поведение API, обновляйте changelog и статус проверок.
Изменение обязательного контракта требует явной корректировки SPEC/ADR, а не
молчаливого ослабления readiness, копирования или полноты.

Перед выпуском нужны воспроизводимые проверки candidate artifacts и release
gates из SPEC. Публичное имя, лицензия, publishing identity и Trusted Publishing
требуют отдельного подтверждения владельцем. Локальная сборка не публикует пакет.
