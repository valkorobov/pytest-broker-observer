"""Consumer-side inference checks, analyzed against installed candidate packages.

The functions are deliberately not executed. Generic message-type arguments
exercise the generated-message overload without importing an optional runtime;
concrete runtime/gencode compatibility is a separate release matrix obligation.
"""

from dataclasses import dataclass
from typing import TypeVar, assert_type

from event_observer import (
    EventObserver,
    EventScope,
    EventWatcher,
    ObservedEvent,
    RawEvent,
    SyncEventObserver,
    SyncEventScope,
    SyncEventWatcher,
)
from event_observer.testing import FakeTransport

MessageT = TypeVar("MessageT")


@dataclass
class Payload:
    status: str


class PayloadDecoder:
    def decode(self, event: RawEvent) -> Payload:
        return Payload("completed")


def matches(value: object) -> bool:
    return True


def synchronous() -> None:
    observer = SyncEventObserver(transport=FakeTransport())
    assert_type(observer.watch(source="events"), SyncEventWatcher[bytes | None])
    assert_type(observer.watch(source="events", decoder=PayloadDecoder()), SyncEventWatcher[Payload])
    with observer.watch(source="events") as raw:
        assert_type(raw, SyncEventWatcher[bytes | None])
        assert_type(raw.wait_for(matches, timeout=1), bytes | None)
        assert_type(raw.wait_for_event(matches, timeout=1), ObservedEvent[bytes | None])
        assert_type(raw.wait_for_count(matches, 2, timeout=1), list[bytes | None])
    with observer.watch(source="events", decoder=PayloadDecoder()) as watcher:
        assert_type(watcher, SyncEventWatcher[Payload])
        scope = watcher.scope(matches)
        assert_type(scope, SyncEventScope[Payload])
        assert_type(scope.wait_for(matches, timeout=1), Payload)
        assert_type(watcher.wait_for(matches, timeout=1), Payload)
        assert_type(watcher.wait_for_event(matches, timeout=1), ObservedEvent[Payload])
        assert_type(watcher.wait_for_count(matches, 2, timeout=1), list[Payload])


async def asynchronous() -> None:
    observer = EventObserver(transport=FakeTransport())
    assert_type(observer.watch(source="events"), EventWatcher[bytes | None])
    assert_type(observer.watch(source="events", decoder=PayloadDecoder()), EventWatcher[Payload])
    async with observer.watch(source="events") as raw:
        assert_type(raw, EventWatcher[bytes | None])
        assert_type(await raw.wait_for(matches, timeout=1), bytes | None)
        assert_type(await raw.wait_for_event(matches, timeout=1), ObservedEvent[bytes | None])
        assert_type(await raw.wait_for_count(matches, 2, timeout=1), list[bytes | None])
    async with observer.watch(source="events", decoder=PayloadDecoder()) as watcher:
        assert_type(watcher, EventWatcher[Payload])
        scope = watcher.scope(matches)
        assert_type(scope, EventScope[Payload])
        assert_type(await scope.wait_for(matches, timeout=1), Payload)
        assert_type(await watcher.wait_for(matches, timeout=1), Payload)
        assert_type(await watcher.wait_for_event(matches, timeout=1), ObservedEvent[Payload])
        assert_type(await watcher.wait_for_count(matches, 2, timeout=1), list[Payload])


def synchronous_messages(observer: SyncEventObserver, message_type: type[MessageT]) -> None:
    assert_type(observer.watch(source="events", message_type=message_type), SyncEventWatcher[MessageT])
    with observer.watch(source="events", message_type=message_type) as watcher:
        assert_type(watcher, SyncEventWatcher[MessageT])
        assert_type(watcher.scope(matches), SyncEventScope[MessageT])
        assert_type(watcher.wait_for(matches, timeout=1), MessageT)
        assert_type(watcher.wait_for_event(matches, timeout=1), ObservedEvent[MessageT])
        assert_type(watcher.wait_for_count(matches, 2, timeout=1), list[MessageT])


async def asynchronous_messages(observer: EventObserver, message_type: type[MessageT]) -> None:
    assert_type(observer.watch(source="events", message_type=message_type), EventWatcher[MessageT])
    async with observer.watch(source="events", message_type=message_type) as watcher:
        assert_type(watcher, EventWatcher[MessageT])
        assert_type(watcher.scope(matches), EventScope[MessageT])
        assert_type(await watcher.wait_for(matches, timeout=1), MessageT)
        assert_type(await watcher.wait_for_event(matches, timeout=1), ObservedEvent[MessageT])
        assert_type(await watcher.wait_for_count(matches, 2, timeout=1), list[MessageT])
