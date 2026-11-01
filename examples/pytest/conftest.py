"""Fixtures принадлежат тестовому проекту; plugin библиотеки их не создаёт."""

from collections.abc import Callable, Iterator

import pytest

from event_observer import EventObserver, SyncEventObserver, SyncEventWatcher
from event_observer.testing import FakeTransport


@pytest.fixture
def transport() -> FakeTransport:
    # Новый transport на каждый тест: история одного теста не попадает в другой.
    return FakeTransport()


@pytest.fixture
def sync_events(transport: FakeTransport) -> SyncEventObserver:
    # Factory ленива. Ресурсы открывает и закрывает контекст watch() в тесте.
    return SyncEventObserver(transport=transport)


@pytest.fixture
def async_events(transport: FakeTransport) -> EventObserver:
    return EventObserver(transport=transport)


@pytest.fixture
def watched_events(sync_events: SyncEventObserver) -> Iterator[SyncEventWatcher[bytes | None]]:
    # Yield fixture подтверждает READY до теста и закрывает watcher при teardown.
    with sync_events.watch(source="operations") as watcher:
        yield watcher


@pytest.fixture
def perform_operation(transport: FakeTransport) -> Callable[[str], None]:
    def perform(operation_id: str) -> None:
        # Имитация внешнего сервиса. В реальном проекте здесь вызывают service_client.
        headers = (("operation-id", operation_id.encode()),)
        for status in (b"started", b"completed"):
            transport.publish(status, source="operations", key=operation_id.encode(), headers=headers)

    return perform
