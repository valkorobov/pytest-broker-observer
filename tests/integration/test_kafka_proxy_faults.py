"""Inject Kafka connectivity faults through a verified private proxy and broker."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
import threading
import time
from urllib.request import ProxyHandler, Request, build_opener

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.errors import EventTimeoutError, TransportConnectionError
from event_observer.models import StartPosition
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig
from event_observer.testing.kafka import KafkaAdapterHarness

from tests.integration.containers.common import restart_owned_broker, wait_until_ready
from tests.integration.containers.kafka_faults import kafka_fault_reader
from tests.integration.test_kafka_proxy_routing import _route_disabled, owned_route as owned_route


pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        os.environ.get("EVENT_OBSERVER_FAULTS") != "kafka", reason="Run the isolated Kafka fault profile"
    ),
    pytest.mark.parametrize("mode", ["async", "blocking"]),
]


@pytest.fixture
def owned_topic(owned_route):
    servers, configure = owned_route
    harness = KafkaAdapterHarness(servers)
    try:
        source = asyncio.run(harness.create_source(case_id="kafka-proxy-faults"))
        yield servers, configure, harness, source
    finally:
        asyncio.run(harness.cleanup())


def _adapter(servers, *, metadata_timeout=2):
    return KafkaAdapter(
        KafkaConfig(
            servers,
            client_options={"log_level": 0},
            metadata_timeout=metadata_timeout,
            topology_check_interval=0.1,
        )
    )


def _kafka_threads():
    return {thread for thread in threading.enumerate() if thread.name.startswith("event-observer-kafka-")}


def test_kafka_startup_unavailable_rolls_back(mode, owned_topic):
    servers, configure, harness, source = owned_topic
    adapter = _adapter(servers, metadata_timeout=1)
    initial = _kafka_threads()
    with _route_disabled(configure):
        began = time.monotonic()
        with pytest.raises(TransportConnectionError):
            with kafka_fault_reader(mode, adapter, source, StartPosition.latest(), startup_timeout=5):
                pytest.fail("Kafka reached READY while the only advertised route was disabled")
        assert time.monotonic() - began < 5
        assert _kafka_threads() == initial
    with kafka_fault_reader(mode, adapter, source, StartPosition.latest()) as fresh:
        assert asyncio.run(harness.publish(source, b"recovered-after-startup")).accepted
        assert fresh.next().payload == b"recovered-after-startup"


@contextmanager
def _delay_responses(configure, fault):
    configure()  # Verify the exact loopback route before changing its toxics.
    control = os.environ["EVENT_OBSERVER_TOXIPROXY_URL"]
    route = os.environ["EVENT_OBSERVER_TOXIPROXY_PROXY"]
    endpoint = f"{control}/proxies/{route}/toxics"
    kind, attributes = {
        "latency": ("latency", {"latency": 2500, "jitter": 0}),
        "blackhole": ("timeout", {"timeout": 0}),
    }[fault]
    toxic = {
        "name": f"observer-reply-{fault}",
        "type": kind,
        "stream": "downstream",
        "toxicity": 1.0,
        "attributes": attributes,
    }
    opener = build_opener(ProxyHandler({}))
    request = Request(endpoint, data=json.dumps(toxic).encode(), headers={"Content-Type": "application/json"})
    with opener.open(request, timeout=5) as response:
        installed = json.loads(response.read(16384))
    try:
        assert installed == toxic
        yield
    finally:
        delete = Request(f"{endpoint}/{toxic['name']}", method="DELETE")
        with opener.open(delete, timeout=5) as response:
            assert response.status == 204
        configure()


@pytest.mark.parametrize("fault", ["latency", "blackhole"])
def test_kafka_downstream_fault_does_not_extend_active_wait(mode, owned_topic, fault):
    servers, configure, harness, source = owned_topic
    adapter = _adapter(servers, metadata_timeout=4)
    wait_budget = 1.5
    started, installed, finished = (threading.Event() for _ in range(3))

    def reject_retained_record(value):
        assert value == b"retained-before-network-fault"
        started.set()
        return False

    def hold_fault():
        assert started.wait(8), "No matcher reached the admitted Kafka record"
        with _delay_responses(configure, fault):
            installed.set()
            assert finished.wait(12), "The Kafka watcher did not close under the active fault"

    def assert_timeout(error, began):
        assert installed.is_set()
        summary = error.diagnostics.expectations[-1]
        assert summary.outcome == "timeout" and summary.timeout == wait_budget
        assert summary.evaluated == 1 and summary.matched == 0
        assert wait_budget <= time.monotonic() - began < wait_budget + 1

    async def observe_async(executor):
        task = None
        try:
            async with EventObserver(transport=adapter).watch(source=source) as watcher:
                assert (await harness.publish(source, b"retained-before-network-fault")).accepted
                assert await watcher.wait_for(lambda _: True, timeout=8) == b"retained-before-network-fault"
                task = executor.submit(hold_fault)
                began = time.monotonic()
                with pytest.raises(EventTimeoutError) as failed:
                    await watcher.wait_for(reject_retained_record, timeout=wait_budget)
                assert_timeout(failed.value, began)
        finally:
            # Removing the toxic closes the connection. Stop the observer first.
            finished.set()
            if task is not None:
                await asyncio.wrap_future(task)

    def observe_blocking(executor):
        task = None
        try:
            with SyncEventObserver(transport=adapter).watch(source=source) as watcher:
                assert asyncio.run(harness.publish(source, b"retained-before-network-fault")).accepted
                assert watcher.wait_for(lambda _: True, timeout=8) == b"retained-before-network-fault"
                task = executor.submit(hold_fault)
                began = time.monotonic()
                with pytest.raises(EventTimeoutError) as failed:
                    watcher.wait_for(reject_retained_record, timeout=wait_budget)
                assert_timeout(failed.value, began)
        finally:
            finished.set()
            if task is not None:
                task.result(timeout=8)

    with ThreadPoolExecutor(max_workers=1) as executor:
        if mode == "async":
            asyncio.run(observe_async(executor))
        else:
            observe_blocking(executor)


def test_kafka_restart_cannot_resume_failed_reader(mode, owned_topic):
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    servers, _, harness, source = owned_topic
    adapter = _adapter(servers)
    with kafka_fault_reader(mode, adapter, source, StartPosition.latest()) as reader:
        assert asyncio.run(harness.publish(source, b"broker-before-restart")).accepted
        assert reader.next().payload == b"broker-before-restart"
        restart_owned_broker("kafka")
        error = reader.assert_failed(TransportConnectionError)

        admin = AdminClient({"bootstrap.servers": servers, "log_level": 0})

        def recovered(timeout):
            try:
                topic = admin.list_topics(timeout=timeout).topics.get(source.name)
            except KafkaException:
                return None
            if topic and topic.error is None and topic.partitions:
                leaders = all(p.error is None and p.leader >= 0 for p in topic.partitions.values())
                return True if leaders else None
            return None

        wait_until_ready("Kafka topic leaders after broker restart", recovered, timeout=45)
        with kafka_fault_reader(mode, adapter, source, StartPosition.latest()) as fresh:
            assert asyncio.run(harness.publish(source, b"broker-after-restart")).accepted
            assert fresh.next().payload == b"broker-after-restart"
        assert reader.failure() is error
