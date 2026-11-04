"""Provide stateless constructors without acquiring observation resources."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from ..aio import EventObserver
    from ..sync import SyncEventObserver
    from ..transports.kafka import KafkaAdapter
    from ..transports.rabbitmq import RabbitMQAdapter
    from ..transports.redis_streams import RedisStreamsAdapter


@pytest.fixture(scope="session")
def kafka_transport_factory() -> type[KafkaAdapter]:
    """Return the lazy Kafka adapter constructor, accepting KafkaConfig.

    :returns: KafkaAdapter; each call creates a separate adapter without native I/O.
    """
    from ..transports.kafka import KafkaAdapter

    return KafkaAdapter


@pytest.fixture(scope="session")
def rabbitmq_transport_factory() -> type[RabbitMQAdapter]:
    """Return the lazy RabbitMQ adapter constructor, accepting RabbitMQConfig.

    :returns: RabbitMQAdapter, including its native blocking-adapter provider.
    """
    from ..transports.rabbitmq import RabbitMQAdapter

    return RabbitMQAdapter


@pytest.fixture(scope="session")
def redis_streams_transport_factory() -> type[RedisStreamsAdapter]:
    """Return the lazy Redis Streams adapter constructor, accepting RedisStreamsConfig.

    :returns: RedisStreamsAdapter, including its native blocking-adapter provider.
    """
    from ..transports.redis_streams import RedisStreamsAdapter

    return RedisStreamsAdapter


@pytest.fixture(scope="session")
def sync_observer_factory() -> type[SyncEventObserver]:
    """Return the blocking observer constructor with its original keyword arguments.

    :returns: SyncEventObserver; resources open only upon entering a watch context.
    """
    from ..sync import SyncEventObserver

    return SyncEventObserver


@pytest.fixture(scope="session")
def async_observer_factory() -> type[EventObserver]:
    """Return the async observer constructor without requiring an async pytest plugin.

    :returns: EventObserver; its watch contexts use the caller's running async loop.
    """
    from ..aio import EventObserver

    return EventObserver
