"""Ordinary def test_* functions: fixtures, parametrization, scopes, and raw metadata."""

from collections.abc import Callable
from uuid import uuid4

import pytest

from event_observer import ObservationMark, ScopeStart, SyncEventObserver, SyncEventWatcher
from event_observer.matchers import header_equals
from event_observer.testing import FakeTransport


def test_wait_for_operation(sync_events: SyncEventObserver, perform_operation: Callable[[str], None]):
    operation_id = uuid4().hex

    with sync_events.watch(source="operations") as watcher:
        operation = watcher.scope(
            header_equals("operation-id", operation_id.encode()), start=ScopeStart.from_now()
        )

        # Entering watch() has already confirmed READY; the action follows that barrier.
        perform_operation(operation_id)

        completed = operation.wait_for(lambda payload: payload == b"completed", timeout=1)
        assert completed == b"completed"


def test_observed_event_includes_key_and_headers(
    sync_events: SyncEventObserver, perform_operation: Callable[[str], None]
):
    operation_id = uuid4().hex

    with sync_events.watch(source="operations") as watcher:
        perform_operation(operation_id)

        completed = watcher.wait_for_event(lambda payload: payload == b"completed", timeout=1)

        assert completed.payload == b"completed"
        assert completed.raw.source == "operations"
        assert completed.raw.key == operation_id.encode()
        assert completed.raw.headers == (("operation-id", operation_id.encode()),)


def test_mark_excludes_history_and_can_be_used_after_the_event_arrives(
    sync_events: SyncEventObserver,
    transport: FakeTransport,
    perform_operation: Callable[[str], None],
):
    operation_id = uuid4().hex

    with sync_events.watch(source="operations") as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id.encode()))
        transport.publish(
            b"completed", source="operations", headers=(("operation-id", operation_id.encode()),)
        )
        # Waiting for the old event establishes admission without a timing-based sleep.
        previous = operation.wait_for_event(lambda payload: payload == b"completed", timeout=1)

        mark: ObservationMark = watcher.mark()
        perform_operation(operation_id)
        received = operation.wait_for_event(lambda payload: payload == b"completed", since=mark, timeout=1)

        # Reuse the mark later, after the new matching event is already in the Journal.
        completed = operation.wait_for(lambda payload: payload == b"completed", since=mark, timeout=1)
        assert completed == b"completed"
        assert previous.sequence < mark.sequence <= received.sequence


@pytest.mark.parametrize("expected_status", [b"started", b"completed"])
def test_each_parameter_has_independent_observation(
    expected_status: bytes,
    watched_events: SyncEventWatcher[bytes | None],
    perform_operation: Callable[[str], None],
):
    # The yield fixture has already opened the context; each parameter case gets a new watcher.
    perform_operation(uuid4().hex)

    observed = watched_events.wait_for(lambda payload: payload == expected_status, timeout=1)
    assert observed == expected_status
