"""Real RabbitMQ: set RABBITMQ_URL and select the integration marker."""

import asyncio
from collections.abc import Iterator
from importlib.util import find_spec
import os
from uuid import uuid4

import pytest

from event_observer import EventObserver, SyncEventObserver, TransportSource
from event_observer.matchers import header_equals
from event_observer.testing import RabbitMQAdapterHarness
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("RABBITMQ_URL"),
        reason="Set RABBITMQ_URL to run the real RabbitMQ examples",
    ),
]
async_example = (
    pytest.mark.asyncio
    if find_spec("pytest_asyncio") is not None
    else pytest.mark.skip(reason="Install the pytest-asyncio extra to run the asynchronous example")
)


@pytest.fixture(scope="session")
def rabbitmq_config() -> RabbitMQConfig:
    return RabbitMQConfig(url=os.environ["RABBITMQ_URL"])


@pytest.fixture(scope="session")
def rabbitmq_transport(
    rabbitmq_transport_factory: type[RabbitMQAdapter], rabbitmq_config: RabbitMQConfig
) -> RabbitMQAdapter:
    return rabbitmq_transport_factory(rabbitmq_config)


@pytest.fixture
def rabbitmq_events(
    sync_observer_factory: type[SyncEventObserver], rabbitmq_transport: RabbitMQAdapter
) -> SyncEventObserver:
    return sync_observer_factory(transport=rabbitmq_transport)


@pytest.fixture
def async_rabbitmq_events(
    async_observer_factory: type[EventObserver], rabbitmq_transport: RabbitMQAdapter
) -> EventObserver:
    return async_observer_factory(transport=rabbitmq_transport)


@pytest.fixture
def rabbitmq_source(
    request: pytest.FixtureRequest, rabbitmq_config: RabbitMQConfig
) -> Iterator[tuple[RabbitMQAdapterHarness, TransportSource]]:
    harness = RabbitMQAdapterHarness(rabbitmq_config.url)
    try:
        # The source is a unique exchange, separate from application queues.
        source = asyncio.run(harness.create_source(case_id=request.node.name))
        yield harness, source
    finally:
        # Harness calls use independent connections, without retaining an event loop.
        asyncio.run(harness.cleanup())


def test_rabbitmq_event_after_test_action(
    rabbitmq_source: tuple[RabbitMQAdapterHarness, TransportSource], rabbitmq_events: SyncEventObserver
):
    harness, source = rabbitmq_source
    operation_id = uuid4().hex.encode()

    with rabbitmq_events.watch(source=source) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id))
        mark = operation.mark()

        # Publish only after READY has confirmed this watcher's private queue binding.
        receipt = asyncio.run(
            harness.publish(source, b"completed", headers=(("operation-id", operation_id),))
        )
        assert receipt.accepted

        completed = operation.wait_for_event(lambda payload: payload == b"completed", timeout=10, since=mark)
        assert completed.payload == b"completed"
        assert completed.raw.source == source.name
        assert completed.raw.key is None
        assert isinstance(completed.raw.metadata["rabbitmq.routing_key"], str)


@async_example
async def test_rabbitmq_async_event_after_test_action(
    rabbitmq_source: tuple[RabbitMQAdapterHarness, TransportSource],
    async_rabbitmq_events: EventObserver,
):
    harness, source = rabbitmq_source
    operation_id = uuid4().hex.encode()

    async with async_rabbitmq_events.watch(source=source) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id))
        mark = operation.mark()
        receipt = await harness.publish(source, b"completed", headers=(("operation-id", operation_id),))
        assert receipt.accepted

        completed = await operation.wait_for_event(
            lambda payload: payload == b"completed", timeout=10, since=mark
        )
        assert completed.payload == b"completed"
        assert completed.raw.source == source.name
        assert completed.raw.key is None
        assert isinstance(completed.raw.metadata["rabbitmq.routing_key"], str)
