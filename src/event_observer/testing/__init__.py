"""Explicit testing tools; never imported by the production runtime.

Synthetic transports, real-broker harnesses, and extension protocols are
opted into by test code. Importing this namespace does not start infrastructure;
harness fixtures own any resources opened by their methods.
"""

from .fake import FakeAdapterHarness, FakeTransport
from .harness import AdapterTestHarness, FailureHarness, PositionHarness, PublishReceipt, TopologyHarness
from .kafka import KafkaAdapterHarness
from .rabbitmq import RabbitMQAdapterHarness
from .redis_streams import RedisStreamsAdapterHarness

__all__ = [
    "FakeTransport",
    "FakeAdapterHarness",
    "KafkaAdapterHarness",
    "RabbitMQAdapterHarness",
    "RedisStreamsAdapterHarness",
    "AdapterTestHarness",
    "PublishReceipt",
    "FailureHarness",
    "PositionHarness",
    "TopologyHarness",
]
