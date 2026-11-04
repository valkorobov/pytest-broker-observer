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
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig

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


@pytest.fixture(scope="session")
def redis_streams_config() -> RedisStreamsConfig:
    return RedisStreamsConfig(url=os.environ["REDIS_URL"])


@pytest.fixture(scope="session")
def redis_streams_transport(
    redis_streams_transport_factory: type[RedisStreamsAdapter], redis_streams_config: RedisStreamsConfig
) -> RedisStreamsAdapter:
    return redis_streams_transport_factory(redis_streams_config)


@pytest.fixture
def redis_events(
    sync_observer_factory: type[SyncEventObserver], redis_streams_transport: RedisStreamsAdapter
) -> SyncEventObserver:
    return sync_observer_factory(transport=redis_streams_transport)


@pytest.fixture
def async_redis_events(
    async_observer_factory: type[EventObserver], redis_streams_transport: RedisStreamsAdapter
) -> EventObserver:
    return async_observer_factory(transport=redis_streams_transport)


@pytest.fixture
def redis_stream_source(
    request: pytest.FixtureRequest, redis_streams_config: RedisStreamsConfig
) -> Iterator[tuple[RedisStreamsAdapterHarness, TransportSource]]:
    harness = RedisStreamsAdapterHarness(redis_streams_config.url)
    try:
        # Create a unique, already typed empty stream; the observer itself never creates it.
        source = asyncio.run(harness.create_source(case_id=request.node.name))
        yield harness, source
    finally:
        # Each harness call owns its client, so no client survives an asyncio.run() loop.
        asyncio.run(harness.cleanup())


def test_redis_stream_event_after_test_action(
    redis_stream_source: tuple[RedisStreamsAdapterHarness, TransportSource],
    redis_events: SyncEventObserver,
):
    harness, source = redis_stream_source
    operation_id = uuid4().hex.encode()

    with redis_events.watch(source=source) as watcher:
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
    async_redis_events: EventObserver,
):
    harness, source = redis_stream_source
    operation_id = uuid4().hex.encode()
    previous = await harness.publish(source, b"before-boundary", headers=(("operation-id", operation_id),))
    assert previous.accepted
    previous_id = previous.metadata["redis.stream_id"]
    assert isinstance(previous_id, str)

    async with async_redis_events.watch(
        source=source, start=StartPosition.after_stream_id(previous_id)
    ) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id))
        receipt = await harness.publish(source, b"completed", headers=(("operation-id", operation_id),))
        assert receipt.accepted

        # A native stream-ID start is exclusive, so before-boundary cannot be the first event.
        completed = await operation.wait_for_event(lambda payload: True, timeout=10)
        assert completed.payload == b"completed"
        assert completed.raw.source == source.name
        assert completed.raw.key is None
        assert completed.raw.metadata["redis.stream_id"] == receipt.metadata["redis.stream_id"]
