"""Typed startup failures on a privately owned SASL and ACL enabled broker."""

from contextlib import contextmanager
import os
from uuid import uuid4

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.errors import TransportAuthenticationError, TransportAuthorizationError
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig

from tests.integration.containers.common import wait_until_ready


pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        not os.environ.get("EVENT_OBSERVER_KAFKA_ADMIN_PASSWORD"),
        reason="Run the isolated Kafka fault suite for owned SASL and ACL checks",
    ),
]


@contextmanager
def owned_topic():
    """Create one topic as the fixture administrator and delete it on exit."""
    from confluent_kafka.admin import AdminClient, NewTopic

    servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    admin = AdminClient(
        {
            "bootstrap.servers": servers,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "observer_admin",
            "sasl.password": os.environ["EVENT_OBSERVER_KAFKA_ADMIN_PASSWORD"],
            "log_level": 0,
        }
    )
    topic = f"observer-auth-test-{uuid4().hex}"
    try:
        request = admin.create_topics([NewTopic(topic, 1, 1)], request_timeout=10)
        request[topic].result(timeout=10)

        def with_leader(timeout):
            metadata = admin.list_topics(timeout=timeout)
            entry = metadata.topics.get(topic)
            if entry and not entry.error and len(entry.partitions) == 1:
                partition = entry.partitions[0]
                if not partition.error and partition.leader >= 0:
                    return entry
            return None

        wait_until_ready("Kafka SASL topic metadata", with_leader, timeout=8)
        yield servers, topic
    finally:
        admin.delete_topics([topic], request_timeout=10)[topic].result(timeout=10)


@pytest.mark.parametrize(
    ("correct_password", "expected"),
    [(False, TransportAuthenticationError), (True, TransportAuthorizationError)],
    ids=["invalid-password", "missing-topic-acl"],
)
@pytest.mark.parametrize("mode", ["async", "blocking"])
@pytest.mark.asyncio
async def test_kafka_startup_security_is_typed(mode, correct_password, expected):
    import asyncio

    def blocking(adapter, topic):
        with SyncEventObserver(transport=adapter).watch(source=topic):
            pytest.fail("Kafka watcher reached READY without valid authentication and topic access")

    with owned_topic() as (servers, topic):
        reader_password = os.environ["EVENT_OBSERVER_KAFKA_READER_PASSWORD"]
        adapter = KafkaAdapter(
            KafkaConfig(
                servers,
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_username="observer_reader",
                sasl_password=reader_password
                if correct_password
                else os.environ["EVENT_OBSERVER_FAULT_PASSWORD"],
                client_options={"log_level": 0},
            )
        )
        with pytest.raises(expected):
            if mode == "async":
                async with EventObserver(transport=adapter).watch(source=topic):
                    pytest.fail("Kafka watcher reached READY without valid authentication and topic access")
            else:
                await asyncio.to_thread(blocking, adapter, topic)
