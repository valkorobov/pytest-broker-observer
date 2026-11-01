"""Published adapter contracts against an explicitly configured real broker."""

import os

import pytest
import pytest_asyncio

from event_observer.testing.contracts import IsolationContract, LifecycleContract, ReadinessContract
from event_observer.testing.kafka import KafkaAdapterHarness


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        reason="Set KAFKA_BOOTSTRAP_SERVERS for real Kafka contracts",
    ),
]


@pytest_asyncio.fixture
async def adapter_harness():
    harness = KafkaAdapterHarness(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    try:
        yield harness
    finally:
        await harness.cleanup()


class TestKafkaAdapterContracts(LifecycleContract, ReadinessContract, IsolationContract):
    pass
