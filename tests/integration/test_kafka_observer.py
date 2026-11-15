"""End-to-end facade/codec/correlation checks against an explicit real broker."""

import asyncio
import os

import pytest
from google.protobuf.wrappers_pb2 import StringValue

from event_observer import EventObserver, ScopeStart, SyncEventObserver
from event_observer.matchers import field_equals
from event_observer.testing.kafka import KafkaAdapterHarness


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        reason="Set KAFKA_BOOTSTRAP_SERVERS for real Kafka observation",
    ),
]


@pytest.fixture
def real_source():
    harness = KafkaAdapterHarness(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    try:
        source = asyncio.run(harness.create_source(case_id="protobuf-runtime"))
        yield harness, source
    finally:
        asyncio.run(harness.cleanup())


def test_sync_kafka_protobuf_collection_during_blocking_application_action(real_source):
    harness, source = real_source
    with SyncEventObserver(transport=harness.adapter).watch(
        source=source, message_type=StringValue
    ) as watcher:
        operation = watcher.scope(field_equals("value", "operation-42"), start=ScopeStart.from_now())
        asyncio.run(harness.publish(source, StringValue(value="operation-42").SerializeToString()))
        result = operation.wait_for_event(lambda payload: True, timeout=5)
        assert result.payload.value == "operation-42"
        assert result.raw.source == source.name
        result.payload.value = "changed"
        assert operation.wait_for(field_equals("value", "operation-42"), timeout=5).value == "operation-42"


@pytest.mark.asyncio
async def test_async_kafka_protobuf_independent_waiters(real_source):
    harness, source = real_source
    async with EventObserver(transport=harness.adapter).watch(
        source=source, message_type=StringValue
    ) as watcher:
        tasks = [
            asyncio.create_task(watcher.wait_for(field_equals("value", "same-record"), timeout=5))
            for _ in range(2)
        ]
        await harness.publish(source, StringValue(value="same-record").SerializeToString())
        results = await asyncio.gather(*tasks)
        assert [result.value for result in results] == ["same-record", "same-record"]
        assert results[0] is not results[1]
