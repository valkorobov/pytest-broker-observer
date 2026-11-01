"""Real Kafka: set KAFKA_BOOTSTRAP_SERVERS; use the integration marker to select this example."""

import asyncio
from collections.abc import Iterator
import os
from uuid import uuid4

import pytest

from event_observer import ScopeStart, SyncEventObserver, TransportSource
from event_observer.matchers import header_equals
from event_observer.testing import KafkaAdapterHarness


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        reason="Set KAFKA_BOOTSTRAP_SERVERS to run the real Kafka example",
    ),
]


@pytest.fixture
def kafka_source() -> Iterator[tuple[KafkaAdapterHarness, TransportSource]]:
    harness = KafkaAdapterHarness(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    try:
        # The harness creates a unique topic and deletes only its own resources.
        source = asyncio.run(harness.create_source(case_id="pytest-example"))
        yield harness, source
    finally:
        asyncio.run(harness.cleanup())


def test_kafka_event_after_test_action(kafka_source: tuple[KafkaAdapterHarness, TransportSource]):
    harness, source = kafka_source
    operation_id = uuid4().hex.encode()
    events = SyncEventObserver(transport=harness.adapter)

    with events.watch(source=source) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id), start=ScopeStart.from_now())

        # An independent producer simulates the service action; the receipt waits for broker acknowledgement.
        receipt = asyncio.run(
            harness.publish(source, b"completed", key=operation_id, headers=(("operation-id", operation_id),))
        )
        assert receipt.accepted

        completed = operation.wait_for_event(lambda payload: payload == b"completed", timeout=10)
        assert completed.raw.source == source.name
        assert completed.raw.key == operation_id
        assert isinstance(completed.raw.metadata["kafka.offset"], int)
