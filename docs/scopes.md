# Scopes и корреляция

Scope — фильтр и логическая начальная граница поверх общего Journal. Он не
создаёт отдельный consumer и не хранит собственную копию истории.

```python
from event_observer import ScopeStart
from event_observer.matchers import field_equals
from example_pb2 import OperationEvent, COMPLETED

with events.watch(source="example.events", message_type=OperationEvent) as watcher:
    operation = watcher.scope(
        field_equals("operation_id", operation_id),
        name="operation",
        start=ScopeStart.from_now(),
    )
    service.perform(operation_id=operation_id)
    completed = operation.wait_for(field_equals("status", COMPLETED), timeout=5.0)
```

Здесь `events` — `SyncEventObserver`, `service` — клиент тестируемой системы,
`OperationEvent` — generated Protobuf class вашего проекта. Reader продолжает
работать во время блокирующего вызова сервиса. Для `EventObserver` используйте
`async with` и `await operation.wait_for(...)`.

## Начальная граница

| Start | Область |
|---|---|
| `ScopeStart.all_retained()` | Default: логический диапазон родителя, включая сведения об уже утраченной истории |
| `ScopeStart.from_now()` | Следующая admission-позиция на момент исполнения команды в runtime |
| `ScopeStart.from_sequence(n)` | Явная неотрицательная позиция не позже текущего next sequence |

`from_now` относится к приёму в Journal. Сообщение, уже опубликованное в брокере,
но ещё находящееся в native buffers, может поступить после этой границы.
В sync режиме граница фиксируется до возврата `scope()` вызывающему потоку.

Вложенный scope наследует все фильтры родителя и выбирает максимум родительской
и собственной границ. Сначала проверяется parent filter; если он не совпал,
child predicate не вызывается. Определение scope неизменно: для другой
корреляции создайте новый scope.

```python
from event_observer.matchers import header_equals

operation_with_header = operation.scope(header_equals("tenant", b"example"))
```

Correlation можно задавать по payload, source, key, headers и валидному trace ID.
Для mixed-type потока используйте `on_type(T, matcher)` для типоспецифичных полей.
Scope фильтрует уже принятые события: нерелевантные сообщения продолжают занимать
общий Journal и могут привести к переполнению.

## Ожидания и полнота

Каждое ожидание заново сканирует логический диапазон scope собственным cursor.
Ожидания не потребляют события: одна запись может удовлетворить несколько
независимых проверок. `wait_for_count(matcher, N, timeout=...)` выбирает первые
N различных Journal records с одним общим deadline; бизнес-дубликаты считаются
отдельными записями.

`wait_for` возвращает копию payload, `wait_for_event` — detached envelope с
payload и metadata. Уже полученные результаты принадлежат вызывающему коду.
Последовательные ожидания находят ранее принятые сообщения, но не доказывают
порядок событий. Temporal assertions отложены за пределы MVP.

По умолчанию `require_complete=True`. Если DROP policy удалила запись из
требуемого диапазона, строгое ожидание даёт `IncompleteObservationError`, даже
если подходящее сообщение сохранилось. Создание нового default scope не
скрывает прежнюю потерю. Явный `from_now` может выбрать полный диапазон после неё.

`require_complete=False` разрешает искать по сохранившейся неполной истории.
Известная terminal transport/decode/Journal failure всё равно завершает
ожидания ошибкой и видна при нормальном выходе из контекста.

`scope.snapshot()` возвращает ограниченные summaries ожиданий, нижнюю границу
и полноту диапазона; для построения snapshot predicates повторно не вызываются.
Counters относятся к конкретным evaluations, а не к уникальным сообщениям
сразу всех ожиданий. Ограничение `max_scopes` задаётся через `RuntimeLimits`.
Закрытие watcher запрещает новые ожидания всех его scopes.
