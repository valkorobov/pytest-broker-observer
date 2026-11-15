"""Check Kafka offsets and continuity against real broker state transitions."""

import os
import time
from uuid import uuid4

import pytest

from event_observer.errors import (
    StartPositionUnavailableError,
    TopologyChangedError,
    TransportSubscriptionError,
)
from event_observer.models import StartPosition, TransportSource
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig

from tests.integration.containers.common import wait_until_ready
from tests.integration.containers.kafka_faults import delete_kafka_records, kafka_fault_reader
from tests.integration.test_kafka import broker as broker, produce


pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        os.environ.get("EVENT_OBSERVER_FAULTS") != "kafka" or not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        reason="Run the isolated Kafka fault profile",
    ),
    pytest.mark.parametrize("mode", ["async", "blocking"]),
]


def _reader(servers):
    return KafkaAdapter(KafkaConfig(servers, topology_check_interval=0.1))


def test_kafka_retained_low_rejects_trimmed_offset(mode, broker):
    native, _, servers, _, topic = broker
    old_offset = produce(native, servers, topic, b"retention-obsolete")
    surviving_offset = produce(native, servers, topic, b"retention-surviving")
    assert delete_kafka_records(topic, 0, surviving_offset) == surviving_offset > old_offset

    adapter = _reader(servers)
    source = TransportSource(topic)
    with pytest.raises(StartPositionUnavailableError):
        start = StartPosition.at_offset({0: old_offset, 1: 0})
        with kafka_fault_reader(mode, adapter, source, start):
            pytest.fail("Kafka accepted a position removed by DeleteRecords")
    with kafka_fault_reader(mode, adapter, source, StartPosition.earliest()) as reader:
        assert reader.session.readiness.boundary.positions[0] == surviving_offset
        event = reader.next()
        assert event.payload == b"retention-surviving"
        assert event.metadata["kafka.offset"] == surviving_offset


def test_kafka_recreated_topic_cannot_resume_failed_reader(mode, broker):
    native, admin_module, servers, admin, topic = broker
    adapter = _reader(servers)
    source = TransportSource(topic)
    with kafka_fault_reader(mode, adapter, source, StartPosition.latest()) as reader:
        produce(native, servers, topic, b"original-topic")
        assert reader.next().payload == b"original-topic"
        admin.delete_topics([topic], operation_timeout=10, request_timeout=12)[topic].result(timeout=12)
        error = reader.assert_failed(TransportSubscriptionError)

        wait_until_ready(
            "deletion of the owned Kafka topic",
            lambda timeout: True if topic not in admin.list_topics(timeout=timeout).topics else None,
            timeout=15,
        )
        replacement = admin_module.NewTopic(topic, num_partitions=2, replication_factor=1)
        admin.create_topics([replacement], operation_timeout=10, request_timeout=12)[topic].result(timeout=12)

        def replacement_leaders(timeout):
            current = admin.list_topics(timeout=timeout).topics.get(topic)
            if current is not None and current.error is None and set(current.partitions) == {0, 1}:
                if all(p.error is None and p.leader >= 0 for p in current.partitions.values()):
                    return current
            return None

        wait_until_ready("leaders of the recreated Kafka topic", replacement_leaders, timeout=15)
        assert produce(native, servers, topic, b"replacement-topic") == 0
        # A fresh reader sees the replacement while the old reader retains its first failure.
        with kafka_fault_reader(mode, adapter, source, StartPosition.earliest()) as fresh:
            assert fresh.next().payload == b"replacement-topic"
        assert reader.failure() is error


def test_kafka_partition_expansion_fails_closed(mode, broker):
    native, admin_module, servers, admin, topic = broker
    with kafka_fault_reader(mode, _reader(servers), TransportSource(topic), StartPosition.latest()) as reader:
        assert dict(reader.session.readiness.boundary.positions) == {0: 0, 1: 0}
        produce(native, servers, topic, b"before-partition-expansion")
        assert reader.next().payload == b"before-partition-expansion"
        expanded = admin_module.NewPartitions(topic, new_total_count=3)
        admin.create_partitions([expanded], operation_timeout=10, request_timeout=12)[topic].result(
            timeout=12
        )
        reader.assert_failed(TopologyChangedError)


def _enable_compaction(admin_module, admin, topic):
    settings = {"cleanup.policy": "compact", "segment.bytes": "1024", "min.cleanable.dirty.ratio": "0.01"}
    resource = admin_module.ConfigResource(
        admin_module.ResourceType.TOPIC,
        topic,
        incremental_configs=[
            admin_module.ConfigEntry(key, val, incremental_operation=admin_module.AlterConfigOpType.SET)
            for key, val in settings.items()
        ],
    )
    admin.incremental_alter_configs([resource], request_timeout=12)[resource].result(timeout=12)

    def applied(timeout):
        configs = admin.describe_configs([resource], request_timeout=timeout)[resource].result(
            timeout=timeout
        )
        return True if all(configs[key].value == val for key, val in settings.items()) else None

    wait_until_ready("owned topic compaction settings", applied, timeout=15)


def _compacted_offsets(native, servers, topic, tail_offset):
    consumer = native.Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": f"observer-compaction-probe-{uuid4().hex}",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "error",
        }
    )

    def inspect(timeout):
        consumer.assign([native.TopicPartition(topic, 0, 0)])
        offsets = []
        deadline = time.monotonic() + timeout
        while (remaining := deadline - time.monotonic()) > 0:
            message = consumer.poll(remaining)
            if message is None:
                return None
            if message.error() is not None:
                raise native.KafkaException(message.error())
            offsets.append(message.offset())
            if offsets[-1] == tail_offset:
                return offsets if offsets == [0, 2, 3] else None
        return None

    try:
        return wait_until_ready("actual removal of the obsolete Kafka record", inspect, timeout=55)
    finally:
        consumer.close()


def _publish_transaction(native, servers, topic):
    producer = native.Producer(
        {"bootstrap.servers": servers, "transactional.id": f"observer-kafka-fault-{uuid4().hex}"}
    )
    producer.init_transactions(15)
    producer.begin_transaction()
    delivery = []
    producer.produce(
        topic,
        partition=0,
        key=b"committed",
        value=b"committed",
        on_delivery=lambda error, message: delivery.append((error, message)),
    )
    producer.commit_transaction(15)
    [(error, message)] = delivery
    assert error is None
    return message.offset()


def test_kafka_compaction_and_transaction_markers_leave_readable_offset_gaps(mode, broker):
    native, admin_module, servers, admin, topic = broker
    _enable_compaction(admin_module, admin, topic)
    # Each record fits one 1 KiB segment, while consecutive records force segment rolls.
    first, obsolete, replacement, active = (bytes([index]) * 640 for index in range(4))
    assert produce(native, servers, topic, first, key=b"anchor") == 0
    assert produce(native, servers, topic, obsolete, key=b"updated") == 1
    assert produce(native, servers, topic, replacement, key=b"updated") == 2
    tail_offset = produce(native, servers, topic, active, key=b"active")
    assert tail_offset == 3
    assert _compacted_offsets(native, servers, topic, tail_offset) == [0, 2, 3]
    committed_offset = _publish_transaction(native, servers, topic)
    later_offset = produce(native, servers, topic, b"after-marker", key=b"after-marker")
    assert later_offset == committed_offset + 2

    source = TransportSource(topic, partitions=(0,))
    with kafka_fault_reader(mode, _reader(servers), source, StartPosition.earliest()) as reader:
        assert reader.session.readiness.boundary.positions[0] == 0
        events = [reader.next() for _ in range(5)]
        assert [event.metadata["kafka.offset"] for event in events] == [
            0,
            2,
            3,
            committed_offset,
            later_offset,
        ]
        assert [event.payload for event in events] == [
            first,
            replacement,
            active,
            b"committed",
            b"after-marker",
        ]
