"""Асинхронные pytest-тесты: fixtures, корреляция и независимые ожидания."""

import asyncio
from uuid import uuid4

import pytest

from event_observer import EventObserver, ScopeStart
from event_observer.matchers import header_equals
from event_observer.testing import FakeTransport

pytest.importorskip("pytest_asyncio")


@pytest.mark.asyncio
async def test_wait_for_operation_in_async_test(async_events: EventObserver, transport: FakeTransport):
    operation_id = uuid4().hex.encode()

    async with async_events.watch(source="operations") as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id), start=ScopeStart.from_now())

        # Здесь FakeTransport изображает события, опубликованные тестируемым сервисом.
        transport.publish(b"completed", source="operations", headers=(("operation-id", b"another"),))
        transport.publish(b"started", source="operations", headers=(("operation-id", operation_id),))
        transport.publish(b"completed", source="operations", headers=(("operation-id", operation_id),))

        completed = await operation.wait_for_event(lambda payload: payload == b"completed", timeout=1)

        assert completed.payload == b"completed"
        assert completed.raw.source == "operations"
        assert completed.raw.headers == (("operation-id", operation_id),)


@pytest.mark.asyncio
async def test_parallel_waiters_can_observe_same_event(async_events: EventObserver, transport: FakeTransport):
    async with async_events.watch(source="operations") as watcher:
        payload_waiter = asyncio.create_task(
            watcher.wait_for(lambda payload: payload == b"completed", timeout=1)
        )
        event_waiter = asyncio.create_task(
            watcher.wait_for_event(lambda payload: payload == b"completed", timeout=1)
        )

        transport.publish(b"completed", source="operations")
        payload, observed = await asyncio.gather(payload_waiter, event_waiter)

        # Ожидания не конкурируют за сообщение и не удаляют его из журнала.
        assert payload == observed.payload == b"completed"


@pytest.mark.asyncio
async def test_count_reads_history_without_consuming_events(
    async_events: EventObserver, transport: FakeTransport
):
    async with async_events.watch(source="operations") as watcher:
        transport.publish(b"started", source="operations")
        transport.publish(b"completed", source="operations")
        transport.publish(b"completed", source="operations")

        # Одинаковый payload в двух записях считается двумя событиями; deadline общий.
        completed = await watcher.wait_for_count(lambda payload: payload == b"completed", 2, timeout=1)
        started = await watcher.wait_for(lambda payload: payload == b"started", timeout=1)

        assert completed == [b"completed", b"completed"]
        assert started == b"started"
