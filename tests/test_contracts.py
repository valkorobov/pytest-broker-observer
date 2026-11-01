"""Run the published contracts unchanged against the bundled fake harness."""

import pytest_asyncio

from event_observer.testing.contracts import IsolationContract, LifecycleContract, ReadinessContract
from event_observer.testing.fake import FakeAdapterHarness


@pytest_asyncio.fixture
async def adapter_harness():
    harness = FakeAdapterHarness()
    try:
        yield harness
    finally:
        await harness.cleanup()


class TestFakeAdapter(LifecycleContract, ReadinessContract, IsolationContract):
    pass
