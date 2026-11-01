"""Real RabbitMQ facade, routing, and application-queue isolation checks."""

import asyncio
import os
from uuid import uuid4

import pytest
from google.protobuf.wrappers_pb2 import StringValue

from event_observer import (
    EventObserver,
    JournalLimits,
    JournalPolicy,
    ScopeStart,
    SyncEventObserver,
    TransportBufferLimits,
)
from event_observer.matchers import field_equals, header_equals
from event_observer.testing import RabbitMQAdapterHarness
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("RABBITMQ_URL"), reason="Set RABBITMQ_URL for real RabbitMQ observation"
    ),
]


@pytest.fixture
def real_rabbit_source():
    harness = RabbitMQAdapterHarness(os.environ["RABBITMQ_URL"])
    try:
        source = asyncio.run(harness.create_source(case_id="rabbitmq-observer"))
        yield harness, source
    finally:
        asyncio.run(harness.cleanup())


def test_sync_rabbitmq_protobuf_correlation_across_independent_publisher_loops(real_rabbit_source):
    harness, source = real_rabbit_source
    operation_id = uuid4().hex.encode()
    with SyncEventObserver(transport=harness.adapter).watch(
        source=source, message_type=StringValue
    ) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id), start=ScopeStart.from_now())
        receipt = asyncio.run(
            harness.publish(
                source,
                StringValue(value="completed").SerializeToString(),
                headers=(("operation-id", operation_id),),
            )
        )
        assert receipt.accepted
        result = operation.wait_for_event(field_equals("value", "completed"), timeout=5)
        assert result.payload.value == "completed"
        assert result.raw.source == source.name
        assert result.raw.metadata["rabbitmq.routing_key"] == "events"
        result.payload.value = "changed"
        assert operation.wait_for(field_equals("value", "completed"), timeout=5).value == "completed"


@pytest.mark.asyncio
async def test_async_rabbitmq_binding_filters_routing_and_preserves_unique_headers(real_rabbit_source):
    harness, source = real_rabbit_source
    adapter = RabbitMQAdapter(RabbitMQConfig(url=os.environ["RABBITMQ_URL"], binding_keys=("orders.*",)))
    headers = (("operation-id", b"operation-42"), ("empty", b""), ("nullable", None))
    async with EventObserver(transport=adapter).watch(source=source) as watcher:
        assert (await harness.publish(source, b"unrelated", routing_key="billing.completed")).accepted
        assert (
            await harness.publish(source, b"completed", routing_key="orders.completed", headers=headers)
        ).accepted
        observed = await watcher.wait_for_event(lambda _: True, timeout=5)
        assert observed.payload == b"completed"
        assert observed.sequence == 0
        assert dict(observed.raw.headers) == dict(headers)
        assert observed.raw.metadata["rabbitmq.routing_key"] == "orders.completed"
        assert observed.raw.source == source.name


@pytest.mark.asyncio
async def test_observer_does_not_consume_or_delete_an_existing_application_queue(real_rabbit_source):
    import aio_pika

    harness, source = real_rabbit_source
    connection = await aio_pika.connect(os.environ["RABBITMQ_URL"], timeout=10)
    try:
        channel = await connection.channel()
        exchange = await channel.get_exchange(source.name, ensure=True)
        application = await channel.declare_queue(exclusive=True, auto_delete=True)
        await application.bind(exchange, routing_key="events", timeout=5)
        async with EventObserver(transport=harness.adapter).watch(source=source) as watcher:
            assert (await harness.publish(source, b"shared-publication")).accepted
            assert await watcher.wait_for(lambda _: True, timeout=5) == b"shared-publication"
        # The observer closed its own subscription; the application's copy remains.
        message = await application.get(timeout=5)
        assert message is not None
        assert message.body == b"shared-publication"
        await message.ack()
        assert (await harness.publish(source, b"after-observer-close")).accepted
        next_message = await application.get(timeout=5)
        assert next_message is not None
        assert next_message.body == b"after-observer-close"
        await next_message.ack()
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_prefetch_one_and_one_record_handoff_drain_confirmed_burst_without_false_overflow(
    real_rabbit_source,
):
    import aio_pika
    import aiormq

    _, source = real_rabbit_source
    adapter = RabbitMQAdapter(RabbitMQConfig(url=os.environ["RABBITMQ_URL"], prefetch_count=1))
    # Keep Journal's hard event limit within the deliberately tiny handoff.
    journal = JournalPolicy(JournalLimits(max_events=128, max_bytes=64 * 1024, max_event_bytes=4096))
    limits = TransportBufferLimits(max_events=1, max_bytes=4096)
    payloads = [b""] + [f"record-{index}".encode() for index in range(1, 50)]
    connection = await aio_pika.connect(os.environ["RABBITMQ_URL"], timeout=10)
    try:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange(source.name, ensure=True)
        async with EventObserver(transport=adapter, journal=journal, buffers=limits).watch(
            source=source
        ) as watcher:
            waiting = asyncio.create_task(watcher.wait_for_count(lambda _: True, len(payloads), timeout=10))
            try:
                for payload in payloads:
                    confirmation = await exchange.publish(
                        aio_pika.Message(body=payload),
                        routing_key="events",
                        mandatory=True,
                        timeout=5,
                    )
                    assert isinstance(confirmation, aiormq.spec.Basic.Ack)
                assert await waiting == payloads
                assert watcher.snapshot().next_sequence == len(payloads)
                assert watcher.snapshot().complete
            finally:
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
    finally:
        await connection.close()
