"""Real-broker checks. Set KAFKA_BOOTSTRAP_SERVERS to an isolated test broker.

These tests create and delete uniquely named topics. An unavailable broker is a
test failure when explicitly configured; only missing configuration is skipped.
"""

import asyncio
import importlib
import os
import time
from uuid import uuid4

import pytest

from event_observer.config import TransportBufferLimits
from event_observer.errors import StartPositionUnavailableError, TopologyChangedError
from event_observer.models import EpochMilliseconds, OpenContext, StartPosition, TransportSource
from event_observer.testing.kafka import KafkaAdapterHarness
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        reason="Set KAFKA_BOOTSTRAP_SERVERS for real Kafka tests",
    ),
]


@pytest.fixture
def broker():
    native = importlib.import_module("confluent_kafka")
    admin_module = importlib.import_module("confluent_kafka.admin")
    servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    admin = admin_module.AdminClient({"bootstrap.servers": servers})
    harness = KafkaAdapterHarness(servers, partitions=2, timeout=20)
    try:
        source = asyncio.run(harness.create_source(case_id="broker-contracts"))
        yield native, admin_module, servers, admin, source.name
    finally:
        asyncio.run(harness.cleanup())


def make_context():
    return OpenContext(
        observer_id=f"integration-{uuid4().hex}",
        startup_deadline=time.monotonic() + 15,
        buffers=TransportBufferLimits(),
    )


def produce(native, servers, topic, payload, *, partition=0, headers=None, key=None):
    producer = native.Producer({"bootstrap.servers": servers, "message.timeout.ms": 10000})
    delivery = []
    producer.produce(
        topic,
        value=payload,
        partition=partition,
        headers=headers,
        key=key,
        on_delivery=lambda error, message: delivery.append((error, message)),
    )
    assert producer.flush(15) == 0
    [(error, message)] = delivery
    assert error is None
    return message.offset()


def test_empty_topic_ready_then_two_independent_readers_receive_immediate_publication(broker):
    native, _, servers, _, topic = broker

    async def scenario():
        adapter = KafkaAdapter(KafkaConfig(servers))
        one = await adapter.open(TransportSource(topic), StartPosition.latest(), context=make_context())
        two = await adapter.open(TransportSource(topic), StartPosition.latest(), context=make_context())
        try:
            assert dict(one.readiness.boundary.positions) == {0: 0, 1: 0}
            await asyncio.to_thread(produce, native, servers, topic, b"ready-publication")
            first, second = await asyncio.wait_for(
                asyncio.gather(anext(one.receive()), anext(two.receive())), 10
            )
            assert first.payload == second.payload == b"ready-publication"
        finally:
            await asyncio.gather(
                one.close(deadline=time.monotonic() + 5), two.close(deadline=time.monotonic() + 5)
            )

    asyncio.run(scenario())


def test_earliest_preserves_headers_tombstone_and_explicit_offsets(broker):
    native, _, servers, _, topic = broker
    produce(native, servers, topic, None, headers=[("duplicate", b"one"), ("duplicate", None)])
    produce(native, servers, topic, b"", partition=1)

    async def scenario():
        adapter = KafkaAdapter(KafkaConfig(servers))
        session = await adapter.open(
            TransportSource(topic), StartPosition.at_offset({0: 0, 1: 0}), context=make_context()
        )
        try:
            stream = session.receive()
            events = [await asyncio.wait_for(anext(stream), 10), await asyncio.wait_for(anext(stream), 10)]
            by_partition = {event.metadata["kafka.partition"]: event for event in events}
            assert by_partition[0].payload is None
            assert by_partition[0].headers == (("duplicate", b"one"), ("duplicate", None))
            assert by_partition[1].payload == b""
        finally:
            await session.close(deadline=time.monotonic() + 5)
        with pytest.raises(StartPositionUnavailableError):
            await adapter.open(
                TransportSource(topic), StartPosition.at_offset({0: 999999, 1: 0}), context=make_context()
            )

    asyncio.run(scenario())


def test_future_timestamp_falls_back_to_high_or_requires_existing(broker):
    native, _, servers, _, topic = broker
    produce(native, servers, topic, b"past")

    async def scenario():
        adapter = KafkaAdapter(KafkaConfig(servers))
        timestamp = EpochMilliseconds(int(time.time() * 1000) + 600000)
        session = await adapter.open(
            TransportSource(topic), StartPosition.at_timestamp(timestamp), context=make_context()
        )
        try:
            assert dict(session.readiness.boundary.positions) == {0: 1, 1: 0}
        finally:
            await session.close(deadline=time.monotonic() + 5)
        with pytest.raises(StartPositionUnavailableError):
            await adapter.open(
                TransportSource(topic),
                StartPosition.at_timestamp(timestamp, require_existing=True),
                context=make_context(),
            )

    asyncio.run(scenario())


def test_partition_expansion_is_a_terminal_topology_failure(broker):
    _, admin_module, servers, admin, topic = broker

    async def scenario():
        session = await KafkaAdapter(KafkaConfig(servers, topology_check_interval=0.1)).open(
            TransportSource(topic), StartPosition.latest(), context=make_context()
        )
        try:
            futures = admin.create_partitions([admin_module.NewPartitions(topic, new_total_count=3)])
            await asyncio.to_thread(lambda: [future.result(timeout=15) for future in futures.values()])
            assert isinstance(await asyncio.wait_for(session.wait_failure(), 15), TopologyChangedError)
        finally:
            await session.close(deadline=time.monotonic() + 5)

    asyncio.run(scenario())


def test_read_committed_latest_uses_numeric_high_boundary_across_old_transaction(broker):
    native, _, servers, _, topic = broker
    transactional = native.Producer(
        {"bootstrap.servers": servers, "transactional.id": f"observer-it-{uuid4().hex}"}
    )
    transactional.init_transactions(15)
    transactional.begin_transaction()
    transactional.produce(topic, value=b"old-open-transaction", partition=0)
    assert transactional.flush(15) == 0

    async def scenario():
        session = await KafkaAdapter(KafkaConfig(servers, isolation_level="read_committed")).open(
            TransportSource(topic), StartPosition.latest(), context=make_context()
        )
        try:
            assert session.readiness.boundary.positions[0] >= 1
            await asyncio.to_thread(transactional.commit_transaction, 15)
            await asyncio.to_thread(produce, native, servers, topic, b"new-after-boundary")
            event = await asyncio.wait_for(anext(session.receive()), 10)
            assert event.payload == b"new-after-boundary"
        finally:
            await session.close(deadline=time.monotonic() + 5)

    try:
        asyncio.run(scenario())
    except BaseException:
        try:
            transactional.abort_transaction(15)
        except native.KafkaException:
            pass
        raise
