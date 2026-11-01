# Диагностика и безопасные отчёты

У watcher есть `watcher.diagnostics.snapshot()`. Snapshot содержит неизменяемые
записи lifecycle, завершённые ожидания и агрегированные счётчики приёма.
`scope.snapshot()` показывает сохранённые результаты ожиданий данного scope;
повторно вызывать пользовательский predicate для отчёта не требуется.

Сводка ожидания различает `evaluated`, `matched`, `scope_excluded` и
`not_applicable`. Для несовпадений сохраняются первые и последние samples,
номера событий и коды причин. `EventTimeoutError.diagnostics` помогает отличить
пустой поток от неподходящей correlation или недостаточного count.

```python
from event_observer.diagnostics import DiagnosticLimits, DiagnosticPolicy, ReportingMode

policy = DiagnosticPolicy(
    mode=ReportingMode.SUMMARY,
    limits=DiagnosticLimits(
        max_records=500,
        max_expectation_summaries=100,
        max_mismatch_samples=10,
        max_attachment_bytes=128 * 1024,
    ),
)
```

По умолчанию `ERRORS_ONLY` отправляет reporters отчёт при ошибке. `SUMMARY`
также включает успешные операции. `VERBOSE` сохраняет те же ограничения памяти.
Входящие события агрегируются в `snapshot.counters`, чтобы частый receive
не вытеснял lifecycle. Суммарный export budget watcher по умолчанию — 2 MiB;
один JSON attachment — до 256 KiB. Усечение сохраняет валидный JSON и явные
счётчики пропусков.

`watcher.diagnostics.export()` возвращает `DiagnosticExport`; его `.to_dict()`
и `.to_json()` создают отделённое представление. `.to_json()` и все JSON-вложения
отчёта форматируются с `indent=2`. Лимиты учитывают размер UTF-8 документа вместе
с отступами и переносами строк. Snapshot уже очищен согласно политике;
он не содержит protobuf objects, исключений или native clients.

## Раскрытие содержимого

В JSON-экспорте `METADATA_ONLY` исключает payload, key, headers, expected/actual значения,
пользовательские descriptions, source labels и trace IDs. `allowed_fields`
разрешает выбранные дополнительные labels; `aliases` заменяет их значения.
Credentials и transport config исключены из диагностического экспорта.

С разрешённым disclosure несовпавшие samples автоматически получают
ограниченные копии содержимого. Явный `disclose_payload()` также доступен
для отдельных артефактов:

```python
from event_observer.diagnostics import DiagnosticPolicy, DisclosurePolicy


def redact(payload):
    payload.pop("access_token", None)
    return payload


policy = DiagnosticPolicy(
    disclosure=DisclosurePolicy.REDACTED_PAYLOAD,
    redactor=redact,
)
```

Redactor получает независимую копию. Его исключение или невалидный JSON
приводят к `payload_omitted` и безопасному коду ошибки, без fallback к исходному
содержимому. `FULL_PAYLOAD` — отдельное явное разрешение раскрытия сообщения.
Обработка содержимого запускается при оценке несовпадений, за пределами receive
и без Journal locks. По умолчанию payload не инспектируется даже ради отчёта.
Суммарное сохранённое содержимое samples ограничено capture budget, равным
`max_export_bytes`; при исчерпании сохраняются metadata и признак усечения.

Оригинальные exceptions могут оставаться в Python chaining. Политика защищает
собственные exports библиотеки; она не управляет traceback pytest или внешним
пользовательским logger.

## Allure

Установите зависимость из локального проекта:

```shell
python -m pip install -e '.[allure]'
```

```python
from event_observer import SyncEventObserver
from event_observer.diagnostics import DiagnosticPolicy, ReportingMode
from event_observer.integrations.allure import AllureReporter

events = SyncEventObserver(
    transport=adapter,
    reporters=[AllureReporter()],
    diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
)
```

Запускайте pytest с `--alluredir`, чтобы плагин записывал результаты и reporter
получал активный test context. `SUMMARY` в примере включает вложения при успехе;
без этой настройки действует `ERRORS_ONLY`. Полный набор запускаемых тестов,
генерация HTML и просмотр отчёта, включая намеренный `FAILED`, описаны в
[examples/allure](../examples/allure/README.md).

Для настоящего брокера есть [Kafka-примеры с AllureReporter](../examples/allure/README.md#kafka-и-allurereporter):
Compose, корреляция, ожидаемый timeout и отдельный отчёт с JSON-вложениями.

Reporter создаёт завершённые summary steps, не держит step открытым между
`await` и не создаёт step на каждое сообщение. Он проверяет активный test
context и не прикрепляет отчёт наблюдения к следующему тесту. Фактическая
длительность ожидания хранится в JSON. Ошибка reporter не меняет результат
ожидания. Каждый watcher получает отдельный Allure binding в контексте текущего
теста, поэтому factory с reporter можно хранить в session fixture. Binding
предыдущего watcher не записывает вложения в следующий тест.

Заголовок summary включает класс транспортного адаптера и топик/стрим watcher:
`Event observation summary [KafkaAdapter: orders.events]`.
Эти имена отображаются в заголовке независимо от disclosure JSON; `aliases`
применяется и к ним. Например, `aliases={"KafkaAdapter": "Kafka", "internal.orders": "orders"}`
даёт заголовок `Event observation summary [Kafka: orders]`. Метки ограничены
512 символами каждая, переводы строк заменяются пробелами. У каждого watcher
свой заголовок, в том числе при общей factory и параллельных ожиданиях.

`DiagnosticExport.attachments` содержит готовые JSON-артефакты:
`expectation-result.json`, `mismatches.json`, `event-timeline.json`, а при
соответствующей ошибке — `observer-summary.json` и `transport-state.json`.
Разрешённое содержимое Protobuf помещается в `protobuf-message.json`, остальных
JSON-compatible payload — в `message-content.json`. Allure прикрепляет только
эти уже очищенные документы. Лимиты учитывают encoded JSON и совокупный размер
всех вложений.

## Trace context

`ObservedEvent.trace_context` содержит `MISSING`, `VALID`, `INVALID` либо
`AMBIGUOUS`. Неверный `traceparent` не мешает декодировать сообщение.
Некорректный `tracestate` отбрасывается отдельно от корректного trace ID.
Библиотека учитывает versioning W3C, не создаёт spans и не устанавливает
извлечённый context в текущий OpenTelemetry context.

Trace IDs экспортируются только с `export_trace_ids=True`, а tracestate —
с отдельным `export_tracestate=True`. Baggage автоматически не переносится.

## Изоляция и оценка payload

`DefaultPayloadAccess` возвращает отделённые копии пользовательским callbacks,
результатам и redactor. Для Protobuf используется `CopyFrom`; его accounted
оценка равна `128 + 2 * ByteSize()`. Для остальных объектов суммируются
`sys.getsizeof` уникальных объектов, содержимое встроенных containers,
instance dictionaries и slots, включая унаследованные private slots.

Обход ограничен 64 уровнями и 100 000 просмотренными references. Большой
container обходится постепенно, без дополнительного списка всех его элементов.
Общий объект не учитывается дважды, но просмотр повторяющихся references также
ограничен. Превышение бюджета вызывает `PayloadIsolationError`.

Непрозрачные native allocations нельзя измерить таким обходом. Decoder,
возвращающий подобные объекты, должен предоставить собственный `PayloadAccess`
с `clone_for_user(value)` и `estimate_bytes(value)`. Модель accounted bytes
не является измерением RSS всего процесса.
