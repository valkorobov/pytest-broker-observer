"""Project fixtures compose the plugin's stateless observer factories."""

from collections.abc import Callable, Iterator

import pytest

from event_observer import EventObserver, SyncEventObserver, SyncEventWatcher
from event_observer.testing import FakeTransport


@pytest.fixture
def transport() -> FakeTransport:
    # A new transport per test keeps one test's history out of another.
    return FakeTransport()


@pytest.fixture
def sync_events(
    sync_observer_factory: type[SyncEventObserver], transport: FakeTransport
) -> SyncEventObserver:
    # The factory is lazy. The test's watch() context opens and closes resources.
    return sync_observer_factory(transport=transport)


@pytest.fixture
def async_events(async_observer_factory: type[EventObserver], transport: FakeTransport) -> EventObserver:
    return async_observer_factory(transport=transport)


@pytest.fixture
def watched_events(sync_events: SyncEventObserver) -> Iterator[SyncEventWatcher[bytes | None]]:
    # The yield fixture confirms READY before the test and closes the watcher during teardown.
    with sync_events.watch(source="operations") as watcher:
        yield watcher


@pytest.fixture
def perform_operation(transport: FakeTransport) -> Callable[[str], None]:
    def perform(operation_id: str) -> None:
        # Simulate an external service. A real project would call service_client here.
        headers = (("operation-id", operation_id.encode()),)
        for status in (b"started", b"completed"):
            transport.publish(status, source="operations", key=operation_id.encode(), headers=headers)

    return perform
