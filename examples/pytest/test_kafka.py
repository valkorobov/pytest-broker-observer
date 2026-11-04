"""Real Kafka: set KAFKA_BOOTSTRAP_SERVERS; use the integration marker to select this example."""

import asyncio
from collections.abc import Iterator
from importlib.util import find_spec
import os
from uuid import uuid4

import pytest

from event_observer import EventObserver, SyncEventObserver, TransportSource
from event_observer.matchers import header_equals
from event_observer.testing import KafkaAdapterHarness
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        reason="Set KAFKA_BOOTSTRAP_SERVERS to run the real Kafka example",
    ),
]
async_example = (
    pytest.mark.asyncio
    if find_spec("pytest_asyncio") is not None
    else pytest.mark.skip(reason="Install the pytest-asyncio extra to run the asynchronous example")
)


@pytest.fixture(scope="session")
def kafka_config() -> KafkaConfig:
    # Environment names and secret resolution belong to the test project.
    return KafkaConfig(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"])


@pytest.fixture(scope="session")
def kafka_transport(kafka_transport_factory: type[KafkaAdapter], kafka_config: KafkaConfig) -> KafkaAdapter:
    return kafka_transport_factory(kafka_config)


@pytest.fixture
def kafka_events(
    sync_observer_factory: type[SyncEventObserver], kafka_transport: KafkaAdapter
) -> SyncEventObserver:
    return sync_observer_factory(transport=kafka_transport)


@pytest.fixture
def async_kafka_events(
    async_observer_factory: type[EventObserver], kafka_transport: KafkaAdapter
) -> EventObserver:
    return async_observer_factory(transport=kafka_transport)


@pytest.fixture
def kafka_source(
    request: pytest.FixtureRequest, kafka_config: KafkaConfig
) -> Iterator[tuple[KafkaAdapterHarness, TransportSource]]:
    harness = KafkaAdapterHarness(kafka_config.bootstrap_servers)
    try:
        # The harness creates a unique topic and deletes only its own resources.
        source = asyncio.run(harness.create_source(case_id=request.node.name))
        yield harness, source
    finally:
        asyncio.run(harness.cleanup())


def test_kafka_event_after_test_action(
    kafka_source: tuple[KafkaAdapterHarness, TransportSource], kafka_events: SyncEventObserver
):
    harness, source = kafka_source
    operation_id = uuid4().hex.encode()

    with kafka_events.watch(source=source) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id))
        mark = operation.mark()

        # An independent producer simulates the service action; the receipt waits for broker acknowledgement.
        receipt = asyncio.run(
            harness.publish(source, b"completed", key=operation_id, headers=(("operation-id", operation_id),))
        )
        assert receipt.accepted

        completed = operation.wait_for_event(lambda payload: payload == b"completed", timeout=10, since=mark)
        assert completed.raw.source == source.name
        assert completed.raw.key == operation_id
        assert isinstance(completed.raw.metadata["kafka.offset"], int)


@async_example
async def test_kafka_async_event_after_test_action(
    kafka_source: tuple[KafkaAdapterHarness, TransportSource], async_kafka_events: EventObserver
):
    harness, source = kafka_source
    operation_id = uuid4().hex.encode()

    async with async_kafka_events.watch(source=source) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id))
        mark = operation.mark()
        receipt = await harness.publish(
            source, b"completed", key=operation_id, headers=(("operation-id", operation_id),)
        )
        assert receipt.accepted

        completed = await operation.wait_for_event(
            lambda payload: payload == b"completed", timeout=10, since=mark
        )
        assert completed.payload == b"completed"
        assert completed.raw.source == source.name
        assert completed.raw.key == operation_id
        assert isinstance(completed.raw.metadata["kafka.offset"], int)
