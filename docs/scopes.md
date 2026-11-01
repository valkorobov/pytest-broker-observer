# Scopes and correlation

A scope is a filter and a logical starting boundary over the shared Journal.
It does not create a separate consumer or keep its own copy of the history.

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

Here, `events` is a `SyncEventObserver`, `service` is a client for the system
under test, and `OperationEvent` is your project's generated Protobuf class.
The reader continues running during the blocking service call. For
`EventObserver`, use `async with` and `await operation.wait_for(...)`.

## Starting boundary

| Start | Range |
|---|---|
| `ScopeStart.all_retained()` | Default: the parent's logical range, including information about history already lost |
| `ScopeStart.from_now()` | The next admission position when the command executes in the runtime |
| `ScopeStart.from_sequence(n)` | An explicit nonnegative position no later than the current next sequence |

`from_now` refers to admission to the Journal. A message already published to
the broker but still in native buffers can arrive after this boundary.
In sync mode, the boundary is fixed before `scope()` returns to the calling
thread.

A nested scope inherits all parent filters and takes the maximum of the
parent's and its own boundaries. The parent filter is checked first; if it does
not match, the child predicate is not called. A scope definition is immutable:
create a new scope for a different correlation.

```python
from event_observer.matchers import header_equals

operation_with_header = operation.scope(header_equals("tenant", b"example"))
```

Correlation can use payload, source, key, headers, and a valid trace ID.
For a mixed-type stream, use `on_type(T, matcher)` for type-specific fields.
A scope filters events already received: irrelevant messages still occupy the
shared Journal and can cause overflow.

## Expectations and completeness

Each expectation scans the scope's logical range again with its own cursor.
Expectations do not consume events: one record can satisfy several independent
checks. `wait_for_count(matcher, N, timeout=...)` selects the first N distinct
Journal records with one shared deadline; business duplicates count as
separate records.

`wait_for` returns a payload copy; `wait_for_event` returns a detached envelope
with payload and metadata. Results already returned belong to the calling
code. Sequential expectations find previously received messages but do not
prove event ordering. Temporal assertions are deferred beyond the MVP.

The default is `require_complete=True`. If a DROP policy removed a record from
the required range, a strict expectation raises `IncompleteObservationError`,
even if a matching message was retained. Creating a new default scope does not
hide previous loss. Explicit `from_now` can select a complete range after it.

`require_complete=False` allows searching the retained incomplete history.
A known terminal transport/decode/Journal failure still terminates expectations
with an error and is visible on normal context exit.

`scope.snapshot()` returns bounded expectation summaries, the lower boundary,
and range completeness; predicates are not called again to build the snapshot.
Counters refer to individual evaluations, rather than unique messages across
all expectations. The `max_scopes` limit is configured through `RuntimeLimits`.
Closing a watcher prohibits new expectations in all of its scopes.
