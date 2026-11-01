# Matchers

Матчер проверяет payload или полное `ObservedEvent`. В sync API ожидание
возвращает значение, в async API тот же вызов нужно выполнить с `await`.

```python
from event_observer.matchers import all_of, field_equals, on_type, source_equals
from example_pb2 import OperationEvent, COMPLETED

completed = all_of(
    on_type(OperationEvent, field_equals("status", COMPLETED)),
    source_equals("example.events"),
)
event = watcher.wait_for(completed, timeout=5.0)
```

`example_pb2` — модуль вашего проекта, сгенерированный Protobuf toolchain.
`message_type=OperationEvent` при создании watcher автоматически задаёт decoder
и известный тип для проверки путей полей.

## Доступные проверки

| Функция | Проверка |
|---|---|
| `field_equals("operation_id", "op-42")` | Равенство поля; вложенные пути разделяются точками |
| `field_matches("child", matcher)` | Проверка значения вложенного поля |
| `has_field("child")` | Явное присутствие поля |
| `protobuf_partial({"operation_id": "op-42", "status": 2})` | Сравнение перечисленных Protobuf полей |
| `predicate(fn)` или обычный `fn` | Синхронный payload predicate |
| `event_predicate(fn)` | Синхронный predicate полного события |
| `is_message_type(T)` | Проверка класса; другой класс даёт несовпадение |
| `on_type(T, matcher)` | Проверка только указанного типа; другой тип пропускается |
| `header_equals("correlation-id", b"op-42")` | Единственный заголовок с точным именем |
| `key_equals(b"key")` | Kafka key или другой нормализованный message key |
| `source_equals("example.events")` | Точный source |
| `trace_id_equals("a" * 32)` | Валидный W3C trace ID |
| `all_of(a, b)`, `any_of(a, b)`, `not_(a)` | Трёхзначная композиция |

Пути используют имена Protobuf schema, а не JSON aliases. Неизвестное имя поля
отклоняется при привязке matcher к известным типам. Для decoder без конечного
набора типов оберните структурированный matcher в `on_type(T, ...)`.

У scalar поля с implicit presence нельзя вызывать `has_field`: это
`UnsupportedFieldPresenceError`. Отсутствующее вложенное сообщение не
подставляется автоматически ради совпадения с default значениями.

В `protobuf_partial` вложенные messages сравниваются рекурсивно, repeated поля —
как полная упорядоченная последовательность, maps — по указанным ключам.
Выбранная oneof ветвь должна быть активной. `None` не означает отсутствие поля;
строковые имена enum не преобразуются в числовые значения.

## Предикаты и смешанные типы

```python
from event_observer.matchers import event_predicate, on_type

payload_check = lambda payload: payload.operation_id.startswith("op-")
safe_mixed_check = on_type(OperationEvent, payload_check)
metadata_check = event_predicate(lambda event: event.raw.metadata.get("kafka.partition") == 0)
```

Каждый пользовательский callback получает независимую копию payload. Изменения
в одном callback не меняют Journal или аргументы других ветвей.

Predicate должен возвращать настоящий `bool`. Async функции отклоняются;
coroutine, число или другой результат дают `InvalidMatcherResultError`.
Обычное исключение callback оборачивается в `MatcherEvaluationError` и завершает
только текущее ожидание. Исходное исключение сохраняется через chaining.

Callbacks должны быть короткими, детерминированными и без I/O. В sync режиме
они выполняются в runtime thread. Python callback, блокирующий этот поток,
нельзя принудительно остановить по таймеру.

## Композиция и metadata

Матчер возвращает `MATCH`, `MISMATCH` или `NOT_APPLICABLE`. Для `all_of`
несовпадение имеет приоритет, затем неприменимость, затем совпадение. Для
`any_of` приоритет обратный: совпадение, неприменимость, несовпадение.
`not_` сохраняет неприменимость. Пустые `all_of()` и `any_of()` запрещены.

`all_of` оценивает все ветви и может собрать несколько причин несовпадения.
Для защиты callback от чужого типа используйте `on_type`, например
`on_type(OperationEvent, payload_check)`. `any_of` останавливается при первом
совпадении.

`header_equals` различает отсутствующий, null и пустой заголовок. Повторяющееся
имя даёт `MatcherEvaluationError`; имена сравниваются с учётом регистра.
Для текста укажите encoding явно:

```python
from event_observer.matchers import header_equals

header_equals("correlation-id", "операция-42", encoding="utf-8")
```

Невалидные байты дают ошибку, replacement decoding не выполняется. Trace
correlation использует только валидированный контекст; отсутствующий или
повреждённый `traceparent` даёт несовпадение без ошибки Protobuf decoder.
