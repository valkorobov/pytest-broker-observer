"""Prove that Kafka traffic uses the proxy after broker discovery."""

import asyncio
from contextlib import aclosing, contextmanager
import json
import os
import re
import time
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

import pytest

from event_observer.config import TransportBufferLimits
from event_observer.errors import TransportConnectionError
from event_observer.models import OpenContext, StartPosition
from event_observer.testing.kafka import KafkaAdapterHarness
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not any(
            os.environ.get(key) for key in ("EVENT_OBSERVER_TOXIPROXY_URL", "EVENT_OBSERVER_TOXIPROXY_PROXY")
        ),
        reason="Run the isolated Kafka container profile for the Toxiproxy route proof",
    ),
]


@pytest.fixture
def owned_route():
    """Only manipulate the explicitly selected private loopback proxy route."""
    control = urlsplit(os.environ["EVENT_OBSERVER_TOXIPROXY_URL"])
    assert control.scheme == "http" and control.hostname == "127.0.0.1" and control.port
    assert not any((control.username, control.password, control.path, control.query, control.fragment))
    name = os.environ["EVENT_OBSERVER_TOXIPROXY_PROXY"]
    assert re.fullmatch(r"observer-kafka-[0-9a-f]{32}", name)
    servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    assert re.fullmatch(r"127\.0\.0\.1:[1-9][0-9]{0,4}", servers)
    target = f"http://127.0.0.1:{control.port}/proxies/{name}"
    opener = build_opener(ProxyHandler({}))

    def configure(enabled=None):
        body = None if enabled is None else json.dumps({"enabled": enabled}).encode()
        request = Request(
            target,
            data=body,
            headers={"Content-Type": "application/json"},
            method="GET" if enabled is None else "PATCH",
        )
        with opener.open(request, timeout=5) as response:
            route = json.loads(response.read(16384))
        assert route["name"] == name and route["upstream"] == "kafka:9092"
        assert route["listen"] in {"0.0.0.0:8666", "[::]:8666"} and route["toxics"] == []
        assert route["enabled"] is (True if enabled is None else enabled)

    configure()
    return servers, configure


@contextmanager
def _route_disabled(configure):
    """Restore the private route even if a test fails after disabling it."""
    try:
        configure(False)
        yield
    finally:
        configure(True)


def _context(mode):
    return OpenContext(f"isolated-kafka-proxy-{mode}", time.monotonic() + 15, TransportBufferLimits())


def _assert_warm_route(admin, servers, topic):
    """Inspect metadata obtained through the same client used in the cut test."""
    metadata = admin.list_topics(topic=topic, timeout=8)
    assert not metadata.topics[topic].error
    assert {f"{broker.host}:{broker.port}" for broker in metadata.brokers.values()} == {servers}


def _assert_warmed_client_unreachable(admin, topic):
    """Issue a fresh metadata RPC after the proxy has dropped existing connections."""
    from confluent_kafka import KafkaError, KafkaException

    with pytest.raises(KafkaException) as failed:
        admin.list_topics(topic=topic, timeout=2)
    assert failed.value.args[0].code() in {
        KafkaError._ALL_BROKERS_DOWN,
        KafkaError._TIMED_OUT,
        KafkaError._TRANSPORT,
    }


def _poll(session):
    deadline = time.monotonic() + 8
    while (remaining := deadline - time.monotonic()) > 0:
        record = session.poll(min(0.1, remaining))
        if record is not None:
            return record
    pytest.fail("Kafka did not deliver the routed record before the poll deadline")


@pytest.mark.parametrize("mode", ["async", "blocking"])
def test_advertised_kafka_route_cannot_bypass_disabled_proxy(mode, owned_route):
    from confluent_kafka.admin import AdminClient

    servers, configure = owned_route
    native_options = {"bootstrap.servers": servers, "log_level": 0}
    admin = AdminClient(native_options)
    harness = KafkaAdapterHarness(servers)
    adapter = KafkaAdapter(KafkaConfig(servers, client_options={"log_level": 0}))
    record = b"private-route-proof"

    async def observe_async(source):
        session = await adapter.open(source, StartPosition.latest(), context=_context(mode))
        try:
            assert (await harness.publish(source, record)).accepted
            async with aclosing(session.receive()) as stream:
                assert (await asyncio.wait_for(anext(stream), 8)).payload == record
            with _route_disabled(configure):
                await asyncio.to_thread(_assert_warmed_client_unreachable, admin, source.name)
                assert isinstance(await asyncio.wait_for(session.wait_failure(), 8), TransportConnectionError)
        finally:
            await session.close(deadline=time.monotonic() + 8)

    def observe_blocking(source):
        session = adapter.sync_adapter().open(source, StartPosition.latest(), context=_context(mode))
        try:
            assert asyncio.run(harness.publish(source, record)).accepted
            assert _poll(session).payload == record
            with _route_disabled(configure):
                _assert_warmed_client_unreachable(admin, source.name)
                with pytest.raises(TransportConnectionError):
                    _poll(session)
                assert isinstance(session.failure(), TransportConnectionError)
        finally:
            session.close(deadline=time.monotonic() + 8)

    try:
        source = asyncio.run(harness.create_source(case_id=f"proxy-route-{mode}"))
        _assert_warm_route(admin, servers, source.name)
        if mode == "async":
            asyncio.run(observe_async(source))
        else:
            observe_blocking(source)
        _assert_warm_route(AdminClient(native_options), servers, source.name)
    finally:
        asyncio.run(harness.cleanup())
