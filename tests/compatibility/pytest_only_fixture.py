"""Installed-package checks deliberately requiring pytest without async plugins.

The compatibility helper copies this file outside the checkout before collecting
it. Its filename prevents accidental collection in the all-extras development
suite, where absence of optional dependencies would be the wrong expectation.
"""

import asyncio
import importlib.resources
import importlib.util
from pathlib import Path
import socket
import sys
import threading

import event_observer
import pytest

from event_observer.testing import FakeTransport
from event_observer.transports.kafka import KafkaConfig
from event_observer.transports.rabbitmq import RabbitMQConfig
from event_observer.transports.redis_streams import RedisStreamsConfig


def forbidden(*args, **kwargs):
    raise AssertionError("factory construction must not acquire resources")


def assert_installed_pytest_only():
    assert sys.prefix != sys.base_prefix
    assert Path(event_observer.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    assert importlib.resources.files(event_observer).joinpath("py.typed").is_file()
    for name in (
        "pytest_asyncio",
        "confluent_kafka",
        "aio_pika",
        "aiormq",
        "pika",
        "redis",
        "google",
        "allure",
        "opentelemetry",
        "testcontainers",
    ):
        assert name not in sys.modules
        assert importlib.util.find_spec(name) is None


def test_all_installed_factories_are_lazy(
    kafka_transport_factory,
    rabbitmq_transport_factory,
    redis_streams_transport_factory,
    sync_observer_factory,
    async_observer_factory,
    monkeypatch,
):
    assert_installed_pytest_only()
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(asyncio, "new_event_loop", forbidden)
    monkeypatch.setattr(asyncio, "run", forbidden)
    monkeypatch.setattr(asyncio, "Runner", forbidden)
    for factory, config in (
        (kafka_transport_factory, KafkaConfig(bootstrap_servers="localhost:9092")),
        (rabbitmq_transport_factory, RabbitMQConfig(url="amqp://localhost/")),
        (redis_streams_transport_factory, RedisStreamsConfig(url="redis://localhost/0")),
    ):
        adapter = factory(config)
        sync_observer_factory(transport=adapter).watch(source="events")
        async_observer_factory(transport=adapter).watch(source="events")
    assert_installed_pytest_only()


@pytest.fixture(scope="session")
def transport():
    return FakeTransport()


@pytest.fixture(scope="session")
def events(sync_observer_factory, transport):
    return sync_observer_factory(transport=transport)


def test_installed_sync_observer_works_without_async_plugin(events, transport, monkeypatch):
    assert_installed_pytest_only()
    monkeypatch.setattr(asyncio, "new_event_loop", forbidden)
    monkeypatch.setattr(asyncio, "run", forbidden)
    monkeypatch.setattr(asyncio, "Runner", forbidden)
    with events.watch(source="events") as watcher:
        mark = watcher.mark()
        transport.publish(b"hello")
        assert watcher.wait_for(lambda payload: payload == b"hello", since=mark, timeout=1) == b"hello"
    assert transport.open_count == transport.close_count == 1
