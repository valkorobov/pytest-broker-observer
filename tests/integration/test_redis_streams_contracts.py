"""Shared adapter contracts against an explicitly configured Redis server."""

import os

import pytest
import pytest_asyncio

from event_observer.testing.contracts import IsolationContract, LifecycleContract, ReadinessContract
from event_observer.testing.redis_streams import RedisStreamsAdapterHarness

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("REDIS_URL"), reason="Set REDIS_URL for real Redis Streams contracts"
    ),
]


@pytest_asyncio.fixture
async def adapter_harness():
    harness = RedisStreamsAdapterHarness(os.environ["REDIS_URL"])
    try:
        yield harness
    finally:
        await harness.cleanup()


class TestRedisStreamsAdapterContracts(LifecycleContract, ReadinessContract, IsolationContract):
    pass
