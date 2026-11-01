"""Shared adapter contracts against an explicitly configured RabbitMQ broker."""

import os

import pytest
import pytest_asyncio

from event_observer.testing.contracts import IsolationContract, LifecycleContract, ReadinessContract
from event_observer.testing.rabbitmq import RabbitMQAdapterHarness

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("RABBITMQ_URL"), reason="Set RABBITMQ_URL for real RabbitMQ contracts"
    ),
]


@pytest_asyncio.fixture
async def adapter_harness():
    harness = RabbitMQAdapterHarness(os.environ["RABBITMQ_URL"])
    try:
        yield harness
    finally:
        await harness.cleanup()


class TestRabbitMQAdapterContracts(LifecycleContract, ReadinessContract, IsolationContract):
    pass
