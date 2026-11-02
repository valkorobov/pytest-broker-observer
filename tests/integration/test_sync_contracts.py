"""Blocking contracts and actor regressions against explicitly configured brokers.

The wrapper below runs only independent publisher/admin harness operations in
short test-side loops. It does not adapt an async reader into a sync transport:
all observation uses each adapter's genuinely blocking native provider.
"""

import asyncio
import base64
import json
import os
import time
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import pytest

from event_observer import (
    JournalLimits,
    JournalPolicy,
    SyncEventObserver,
    TransportBufferLimits,
)
from event_observer.errors import TransportConnectionError
from event_observer.models import OpenContext, StartPosition
from event_observer.testing.contracts.sync import (
    SyncIsolationContract,
    SyncLifecycleContract,
    SyncReadinessContract,
)
from event_observer.testing.kafka import KafkaAdapterHarness
from event_observer.testing.rabbitmq import RabbitMQAdapterHarness
from event_observer.testing.redis_streams import RedisStreamsAdapterHarness
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig

pytestmark = pytest.mark.integration


class IndependentPublicationHarness:
    """Keep async test administration separate from blocking reader operations."""

    def __init__(self, harness):
        self.publisher = harness
        self.adapter = harness.adapter.sync_adapter()

    def create_source(self, *, case_id):
        return asyncio.run(self.publisher.create_source(case_id=case_id))

    def publish(self, source, payload, *, key=None, headers=()):
        return asyncio.run(self.publisher.publish(source, payload, key=key, headers=headers))

    def cleanup(self):
        asyncio.run(self.publisher.cleanup())


@pytest.fixture(params=("kafka", "rabbitmq", "redis"))
def sync_adapter_harness(request):
    settings = {
        "kafka": ("KAFKA_BOOTSTRAP_SERVERS", KafkaAdapterHarness),
        "rabbitmq": ("RABBITMQ_URL", RabbitMQAdapterHarness),
        "redis": ("REDIS_URL", RedisStreamsAdapterHarness),
    }
    variable, factory = settings[request.param]
    address = os.environ.get(variable)
    if not address:
        pytest.skip(f"Set {variable} for real blocking {request.param} contracts")
    harness = IndependentPublicationHarness(factory(address))
    try:
        yield harness
    finally:
        harness.cleanup()


class TestNativeBlockingAdapterContracts(SyncLifecycleContract, SyncReadinessContract, SyncIsolationContract):
    pass


@pytest.fixture
def rabbitmq_publication_harness():
    address = os.environ.get("RABBITMQ_URL")
    if not address:
        pytest.skip("Set RABBITMQ_URL for real blocking RabbitMQ regressions")
    harness = IndependentPublicationHarness(RabbitMQAdapterHarness(address))
    try:
        yield harness
    finally:
        harness.cleanup()


def test_sync_actor_prefetch_one_collects_an_independently_confirmed_burst_before_waiting(
    rabbitmq_publication_harness,
):
    harness = rabbitmq_publication_harness
    source = harness.create_source(case_id="blocking-prefetch-one")
    adapter = RabbitMQAdapter(RabbitMQConfig(url=os.environ["RABBITMQ_URL"], prefetch_count=1))
    payloads = [b""] + [f"record-{index}".encode() for index in range(1, 30)]
    journal = JournalPolicy(JournalLimits(max_events=64, max_bytes=64 * 1024, max_event_bytes=4096))
    limits = TransportBufferLimits(max_events=1, max_bytes=4096)
    with SyncEventObserver(transport=adapter, journal=journal, buffers=limits).watch(
        source=source
    ) as watcher:
        mark = watcher.mark()
        for payload in payloads:
            assert harness.publish(source, payload).accepted
        assert watcher.wait_for_count(lambda _: True, len(payloads), since=mark, timeout=10) == payloads


def test_blocking_rabbitmq_detects_administrative_connection_closure_without_business_message(
    rabbitmq_publication_harness,
):
    management_url = os.environ.get("RABBITMQ_MANAGEMENT_URL")
    if not management_url:
        pytest.skip("Set RABBITMQ_MANAGEMENT_URL to close only the reader-owned RabbitMQ connection")

    harness = rabbitmq_publication_harness
    source = harness.create_source(case_id="blocking-idle-cancellation")
    session = harness.adapter.open(
        source,
        StartPosition.latest(),
        context=OpenContext("idle-cancellation", time.monotonic() + 10, TransportBufferLimits()),
    )

    try:
        # Exclusive queues cannot be deleted independently even through HTTP.
        # Resolve and close this reader's connection through its unique consumer.
        _close_owned_rabbitmq_connection(management_url, os.environ["RABBITMQ_URL"], session)
        deadline = time.monotonic() + 5
        while session.failure() is None and time.monotonic() < deadline:
            try:
                assert session.poll(0.05) is None
            except TransportConnectionError:
                break
        assert isinstance(session.failure(), TransportConnectionError)
        assert session.failure().cause.reply_code == 320
    finally:
        session.close(deadline=time.monotonic() + 5)


def _close_owned_rabbitmq_connection(management_url, rabbitmq_url, session):
    """Close only the connection identified by this reader's queue and consumer.

    Management credentials may be supplied in its URL; otherwise the configured
    AMQP credentials are reused. Queue metadata can lag consume-ok by one metrics
    refresh interval, so wait within a finite budget while pumping the native
    reader. No source exchange, application queue, or unrelated connection changes.
    """
    queue_name, consumer_tag = session._queue, session._consumer_tag
    assert isinstance(queue_name, str) and queue_name.startswith("amq.gen-")
    assert isinstance(consumer_tag, str) and consumer_tag.startswith("observer-")
    management, amqp = urlsplit(management_url), urlsplit(rabbitmq_url)
    assert management.scheme in {"http", "https"} and management.hostname
    username = management.username if management.username is not None else amqp.username
    password = management.password if management.password is not None else amqp.password
    assert username is not None and password is not None, "Configure management or AMQP credentials"
    credentials = f"{unquote(username)}:{unquote(password)}".encode("utf-8")
    vhost = unquote(amqp.path[1:]) if amqp.path not in {"", "/"} else "/"
    # Strip userinfo from the HTTP target; authentication lives only in its header.
    authority = management.netloc.rsplit("@", 1)[-1]
    deadline = time.monotonic() + 15

    def request(method, path, expected_status):
        """Make one bounded authenticated request and retain a useful failure body."""
        remaining = deadline - time.monotonic()
        assert remaining > 0, "RabbitMQ management fault injection exceeded its deadline"
        target = urlunsplit((management.scheme, authority, management.path.rstrip("/") + path, "", ""))
        operation = Request(
            target,
            method=method,
            headers={"Authorization": "Basic " + base64.b64encode(credentials).decode("ascii")},
        )
        try:
            with urlopen(operation, timeout=min(5, remaining)) as response:
                assert response.status == expected_status
                return json.load(response) if expected_status == 200 else None
        except HTTPError as error:
            reason = error.read(4096).decode("utf-8", errors="replace")
            pytest.fail(f"RabbitMQ management {method} failed ({error.code}): {reason}")

    queue_path = f"/api/queues/{quote(vhost, safe='')}/{quote(queue_name, safe='')}"
    while time.monotonic() < deadline:
        queue = request("GET", queue_path, 200)
        assert queue["name"] == queue_name and queue["vhost"] == vhost and queue["exclusive"] is True
        owned_consumers = [
            consumer
            for consumer in queue.get("consumer_details", [])
            if consumer.get("consumer_tag") == consumer_tag
            and consumer.get("queue", {}).get("name") == queue_name
            and consumer.get("queue", {}).get("vhost") == vhost
        ]
        if owned_consumers:
            assert len(owned_consumers) == 1
            # Consumer and connection statistics can arrive in separate refreshes.
            connection_name = (owned_consumers[0].get("channel_details") or {}).get("connection_name")
            owner_name = (queue.get("owner_pid_details") or {}).get("name")
            if connection_name is not None:
                assert isinstance(connection_name, str) and connection_name
            if owner_name is not None:
                assert isinstance(owner_name, str) and owner_name
            if connection_name is not None and owner_name is not None:
                assert connection_name == owner_name
                request("DELETE", "/api/connections/" + quote(connection_name, safe=""), 204)
                return
        assert session.poll(0.05) is None
    pytest.fail("RabbitMQ management did not expose the owned reader consumer before the deadline")
