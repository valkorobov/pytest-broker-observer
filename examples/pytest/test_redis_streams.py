"""Real Redis Streams: set REDIS_URL and select the integration marker."""

import asyncio
from collections.abc import Iterator
from importlib.util import find_spec
import os
from uuid import uuid4

import pytest

from event_observer import EventObserver, StartPosition, SyncEventObserver, TransportSource
from event_observer.matchers import header_equals
from event_observer.testing import RedisStreamsAdapterHarness

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("REDIS_URL"),
        reason="Set REDIS_URL to run the real Redis Streams examples",
    ),
]
async_example = (
    pytest.mark.asyncio
    if find_spec("pytest_asyncio") is not None
    else pytest.mark.skip(reason="Install the pytest-asyncio extra to run the asynchronous example")
)


@pytest.fixture
def redis_stream_source(
    request: pytest.FixtureRequest,
) -> Iterator[tuple[RedisStreamsAdapterHarness, TransportSource]]:
    harness = RedisStreamsAdapterHarness(os.environ["REDIS_URL"])
    try:
        # Create a unique, already typed empty stream; the observer itself never creates it.
        source = asyncio.run(harness.create_source(case_id=request.node.name))
        yield harness, source
    finally:
        # Each harness call owns its client, so no client survives an asyncio.run() loop.
        asyncio.run(harness.cleanup())


def test_redis_stream_event_after_test_action(
    redis_stream_source: tuple[RedisStreamsAdapterHarness, TransportSource],
):
    harness, source = redis_stream_source
    operation_id = uuid4().hex.encode()
    events = SyncEventObserver(transport=harness.adapter)

    with events.watch(source=source) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id))
        mark = operation.mark()
        # Replace the independent XADD producer with the action of your application.
        receipt = asyncio.run(
            harness.publish(source, b"completed", headers=(("operation-id", operation_id),))
        )
        assert receipt.accepted

        completed = operation.wait_for_event(lambda payload: payload == b"completed", timeout=10, since=mark)
        assert completed.payload == b"completed"
        assert completed.raw.source == source.name
        assert completed.raw.key is None
        assert completed.raw.metadata["redis.stream_id"] == receipt.metadata["redis.stream_id"]


@async_example
async def test_redis_stream_async_after_explicit_id(
    redis_stream_source: tuple[RedisStreamsAdapterHarness, TransportSource],
):
    harness, source = redis_stream_source
    operation_id = uuid4().hex.encode()
    previous = await harness.publish(source, b"before-boundary", headers=(("operation-id", operation_id),))
    assert previous.accepted
    previous_id = previous.metadata["redis.stream_id"]
    assert isinstance(previous_id, str)
    events = EventObserver(transport=harness.adapter)

    async with events.watch(source=source, start=StartPosition.after_stream_id(previous_id)) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id))
        receipt = await harness.publish(source, b"completed", headers=(("operation-id", operation_id),))
        assert receipt.accepted

        # A native stream-ID start is exclusive, so before-boundary cannot be the first event.
        completed = await operation.wait_for_event(lambda payload: True, timeout=10)
        assert completed.payload == b"completed"
        assert completed.raw.source == source.name
        assert completed.raw.key is None
        assert completed.raw.metadata["redis.stream_id"] == receipt.metadata["redis.stream_id"]
