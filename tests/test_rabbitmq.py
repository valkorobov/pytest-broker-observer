"""Controlled native-client tests, independent of RabbitMQ and aio-pika installs."""

import asyncio
from contextlib import aclosing
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from event_observer import EventObserver
from event_observer.config import (
    DecodingPolicy,
    DecodeErrorPolicy,
    JournalLimits,
    JournalPolicy,
    OverflowPolicy,
    TransportBufferLimits,
)
from event_observer.errors import (
    ConfigurationError,
    EventTooLargeError,
    JournalOverflowError,
    MissingOptionalDependencyError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    ObserverFailedError,
    TransportAuthenticationError,
    TransportAuthorizationError,
    TransportBackpressureError,
    TransportInterruptedError,
    TransportReadinessError,
    TransportSubscriptionError,
    UnknownMessageTypeError,
    UnsupportedCapabilityError,
)
from event_observer.models import EpochMilliseconds, OpenContext, StartPosition, TransportSource
from event_observer.transports.base import DeliveryOutcome
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig


class Callbacks(set):
    def fire(self, sender, error=None):
        for callback in tuple(self):
            callback(sender, error)


class Message:
    def __init__(self, body=b"value", *, headers=None, routing_key="orders.done", tag=1):
        self.body = body
        self.headers = headers
        self.routing_key = routing_key
        self.delivery_tag = tag
        self.acks = []
        self.acked = asyncio.Event()
        self.ack_gate = None
        self.ack_error = None
        self.on_ack = None

    async def ack(self, *, multiple=False):
        if self.on_ack is not None:
            self.on_ack()
        if self.ack_gate is not None:
            await self.ack_gate.wait()
        if self.ack_error is not None:
            raise self.ack_error
        self.acks.append(multiple)
        self.acked.set()


class Queue:
    def __init__(self, channel, kwargs):
        self.channel = channel
        self.kwargs = kwargs
        self.bindings = []
        self.callback = None
        self.consume_args = None

    async def bind(self, exchange, *, routing_key, timeout):
        self.channel.native.events.append("bind")
        if self.channel.native.bind_error is not None:
            raise self.channel.native.bind_error
        self.bindings.append((exchange, routing_key))

    async def consume(self, callback, **kwargs):
        self.channel.native.events.append("consume")
        self.callback = callback
        self.consume_args = kwargs
        tag = kwargs["consumer_tag"]
        self.channel.underlay.consumers[tag] = callback
        self.channel.native.consume_started.set()
        if self.channel.native.consume_gate is not None:
            await self.channel.native.consume_gate.wait()
        if self.channel.native.cancel_during_start:
            self.channel.underlay.consumers.pop(tag)
        return tag

    async def cancel(self, tag):
        self.channel.native.events.append("cancel")
        if self.channel.native.cancel_gate is not None:
            await self.channel.native.cancel_gate.wait()
        self.channel.underlay.consumers.pop(tag, None)

    async def delete(self, *, if_unused, if_empty):
        self.channel.native.events.append("delete")
        assert if_unused is False and if_empty is False
        if self.channel.native.delete_gate is not None:
            await self.channel.native.delete_gate.wait()
        if self.channel.native.delete_error is not None:
            raise self.channel.native.delete_error
        self.channel.underlay.consumers.clear()


class Channel:
    def __init__(self, native):
        self.native = native
        self.close_callbacks = Callbacks()
        self.is_closed = False
        self.underlay = SimpleNamespace(consumers={})
        self.queues = []
        self.prefetch_count = None

    async def get_underlay_channel(self):
        return self.underlay

    async def get_exchange(self, name, *, ensure):
        self.native.events.append("exchange")
        assert ensure is True
        return name

    async def set_qos(self, *, prefetch_count, timeout):
        self.native.events.append("qos")
        self.prefetch_count = prefetch_count

    async def declare_queue(self, name, **kwargs):
        self.native.events.append("queue")
        assert name == ""
        queue = Queue(self, kwargs)
        self.queues.append(queue)
        return queue

    async def close(self):
        self.native.events.append("channel_close")
        self.is_closed = True
        self.underlay.consumers.clear()
        self.close_callbacks.fire(self)


class Connection:
    def __init__(self, native):
        self.native = native
        self.close_callbacks = Callbacks()
        self.is_closed = False
        self.channels = []
        self.close_calls = 0

    async def channel(self, *, publisher_confirms):
        assert publisher_confirms is False
        self.native.events.append("channel")
        channel = Channel(self.native)
        self.channels.append(channel)
        return channel

    async def close(self):
        self.close_calls += 1
        self.native.events.append("connection_close")
        if self.native.close_gate is not None:
            await self.native.close_gate.wait()
        if self.native.close_error is not None:
            raise self.native.close_error
        self.is_closed = True
        for channel in self.channels:
            channel.is_closed = True
            channel.underlay.consumers.clear()
        self.close_callbacks.fire(self)


class Native:
    def __init__(self):
        self.connections = []
        self.events = []
        self.bind_error = None
        self.connect_error = None
        self.close_error = None
        self.consume_started = asyncio.Event()
        self.consume_gate = None
        self.cancel_gate = None
        self.delete_gate = None
        self.delete_error = None
        self.close_gate = None
        self.cancel_during_start = False

    async def connect(self, url, *, timeout):
        self.events.append("connect")
        assert timeout > 0
        if self.connect_error is not None:
            raise self.connect_error
        connection = Connection(self)
        self.connections.append(connection)
        return connection

    @property
    def queue(self):
        return self.connections[-1].channels[-1].queues[-1]

    async def deliver(self, message):
        await self.queue.callback(message)


def context(*, max_events=10, max_bytes=65536):
    return OpenContext(
        "observer-unit",
        time.monotonic() + 2,
        TransportBufferLimits(max_events=max_events, max_bytes=max_bytes),
    )


def adapter(monkeypatch, native, **config):
    monkeypatch.setattr("event_observer.transports.rabbitmq._load_native", lambda: native)
    return RabbitMQAdapter(RabbitMQConfig("amqp://user:SECRET@broker/", **config))


async def open_session(adapter, *, ctx=None):
    return await adapter.open(TransportSource("orders"), StartPosition.latest(), context=ctx or context())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"url": ""},
        {"url": "https://broker"},
        {"url": "amqp://broker:not-a-port"},
        {"url": " amqp://broker"},
        {"binding_keys": "#"},
        {"binding_keys": ()},
        {"binding_keys": (1,)},
        {"binding_keys": ("a" * 256,)},
        {"prefetch_count": True},
        {"prefetch_count": 0},
        {"prefetch_count": 65536},
    ],
)
def test_config_rejects_invalid_inputs_without_disclosing_values(kwargs):
    values = {"url": "amqp://user:SECRET@broker/", **kwargs}
    with pytest.raises(ConfigurationError) as error:
        RabbitMQConfig(**values)
    assert "SECRET" not in str(error.value)
    assert "SECRET" not in repr(RabbitMQConfig("amqp://user:SECRET@broker/"))


def test_optional_client_is_not_imported_by_module_or_factory():
    script = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'aio_pika', 'aiormq', 'pamqp'}:
            raise AssertionError('Optional RabbitMQ client imported eagerly')
sys.meta_path.insert(0, Block())
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig
RabbitMQAdapter(RabbitMQConfig('amqp://localhost/'))
"""
    subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, text=True)


@pytest.mark.asyncio
async def test_missing_optional_client_has_actionable_error(monkeypatch):
    def missing():
        raise ImportError("not installed")

    monkeypatch.setattr("event_observer.transports.rabbitmq._load_native", missing)
    with pytest.raises(MissingOptionalDependencyError, match="rabbitmq"):
        await open_session(RabbitMQAdapter(RabbitMQConfig("amqp://broker/")))


@pytest.mark.parametrize(
    "start,source",
    [
        (StartPosition.earliest(), TransportSource("orders")),
        (StartPosition.at_offset({0: 0}), TransportSource("orders")),
        (StartPosition.at_timestamp(EpochMilliseconds(0)), TransportSource("orders")),
        (StartPosition.latest(), TransportSource("orders", partitions=(0,))),
    ],
)
@pytest.mark.asyncio
async def test_unsupported_selection_is_rejected_before_connect(monkeypatch, start, source):
    native = Native()
    with pytest.raises(UnsupportedCapabilityError):
        await adapter(monkeypatch, native).open(source, start, context=context())
    assert native.events == []


@pytest.mark.asyncio
async def test_readiness_waits_for_bindings_and_consume_ok(monkeypatch):
    native = Native()
    native.consume_gate = asyncio.Event()
    task = asyncio.create_task(
        open_session(adapter(monkeypatch, native, binding_keys=("orders.*", "audit.#")))
    )
    await asyncio.wait_for(native.consume_started.wait(), 1)
    assert not task.done()
    assert native.events == ["connect", "channel", "exchange", "qos", "queue", "bind", "bind", "consume"]
    native.consume_gate.set()
    session = await task
    assert native.queue.kwargs["exclusive"] is True
    assert native.queue.kwargs["auto_delete"] is True
    assert native.queue.kwargs["arguments"] == {"x-queue-type": "classic"}
    assert native.queue.consume_args["no_ack"] is False
    assert native.queue.bindings == [("orders", "orders.*"), ("orders", "audit.#")]
    assert session.readiness.boundary.positions == {}
    caps = session.readiness.resolved_capabilities
    assert caps.isolation_modes == {"dedicated_subscription"}
    assert caps.supports_settlement and caps.supports_headers
    assert not any((caps.supports_key, caps.supports_null_payload, caps.supports_duplicate_headers))
    assert not any((caps.supports_position_tracking, caps.supports_topology_detection))
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_deliveries_preserve_empty_body_and_flat_headers_without_auto_ack(monkeypatch):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    binary_header = bytearray(b"\x80")
    message = Message(b"", headers={"text": "value", "raw": b"\xff", "binary": binary_header, "null": None})
    await native.deliver(message)
    async with aclosing(session.receive()) as stream:
        raw = await asyncio.wait_for(anext(stream), 1)
        assert raw.payload == b"" and raw.key is None and raw.source == "orders"
        binary_header[0] = 0
        assert raw.headers == (("text", b"value"), ("raw", b"\xff"), ("binary", b"\x80"), ("null", None))
        assert raw.metadata == {"rabbitmq.routing_key": "orders.done", "rabbitmq.delivery_tag": 1}
        assert raw.delivery_id and raw.delivery_id != "1"
        assert message.acks == []
        with pytest.raises(TransportSubscriptionError):
            session.receive()
        await session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED)
        assert message.acks == [False]
        with pytest.raises(TransportSubscriptionError):
            await session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED)
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("outcome", list(DeliveryOutcome))
@pytest.mark.asyncio
async def test_settlement_accepts_only_safe_explicit_outcomes(monkeypatch, outcome):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    message = Message()
    await native.deliver(message)
    raw = await anext(session.receive())
    with pytest.raises(TransportSubscriptionError):
        await session.settle(raw.delivery_id, "rejected")
    assert not message.acks
    await session.settle(raw.delivery_id, outcome)
    assert message.acks == [False]
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_pending_settlement_still_occupies_prefetch_capacity(monkeypatch):
    native = Native()
    session = await open_session(adapter(monkeypatch, native, prefetch_count=100), ctx=context(max_events=1))
    assert native.connections[0].channels[0].prefetch_count == 1
    message = Message()
    message.ack_gate = asyncio.Event()
    await native.deliver(message)
    raw = await anext(session.receive())
    settle = asyncio.create_task(session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED))
    await asyncio.sleep(0)
    await native.deliver(Message(tag=2))
    assert isinstance(await asyncio.wait_for(session.wait_failure(), 1), TransportBackpressureError)
    assert len(session._pending) == 1
    message.ack_gate.set()
    await settle
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_byte_capacity_is_bounded_even_when_count_is_available(monkeypatch):
    native = Native()
    session = await open_session(adapter(monkeypatch, native), ctx=context(max_bytes=300))
    message = Message(b"x" * 1000)
    await native.deliver(message)
    assert isinstance(await session.wait_failure(), TransportBackpressureError)
    assert not session._pending and not message.acks
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("value", [{"secret": "SECRET"}, ["SECRET"], 42, True])
@pytest.mark.asyncio
async def test_unsupported_header_values_fail_closed(monkeypatch, value):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    message = Message(headers={"private": value})
    await native.deliver(message)
    error = await session.wait_failure()
    assert isinstance(error, TransportSubscriptionError)
    assert "SECRET" not in str(error)
    assert not message.acks
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("resource", ["connection", "channel", "consumer"])
@pytest.mark.asyncio
async def test_failures_are_visible_without_receive_and_survive_waiter_cancellation(monkeypatch, resource):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    waiter = asyncio.create_task(session.wait_failure())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    connection = native.connections[0]
    channel = connection.channels[0]
    if resource == "connection":
        connection.close_callbacks.fire(connection, RuntimeError("SECRET"))
    elif resource == "channel":
        channel.close_callbacks.fire(channel, RuntimeError("SECRET"))
    else:
        channel.underlay.consumers.clear()
    error = await asyncio.wait_for(session.wait_failure(), 1)
    assert isinstance(error, (TransportInterruptedError, TransportSubscriptionError))
    assert "SECRET" not in str(error)
    with pytest.raises(type(error)):
        await anext(session.receive())
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_startup_rollback_and_cancellation_close_partial_resources(monkeypatch):
    native = Native()
    native.consume_gate = asyncio.Event()
    task = asyncio.create_task(open_session(adapter(monkeypatch, native)))
    await asyncio.wait_for(native.consume_started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert native.connections[0].is_closed
    assert not native.connections[0].channels[0].underlay.consumers


@pytest.mark.asyncio
async def test_broker_cancel_before_consume_ok_cannot_publish_ready(monkeypatch):
    native = Native()
    native.cancel_during_start = True
    with pytest.raises(TransportReadinessError):
        await open_session(adapter(monkeypatch, native))
    assert native.connections[0].is_closed
    assert native.events[-3:] == ["cancel", "channel_close", "connection_close"]


@pytest.mark.parametrize(
    "native_name,expected,phase",
    [
        ("ProbableAuthenticationError", TransportAuthenticationError, "connect"),
        ("ChannelAccessRefused", TransportAuthorizationError, "bind"),
        ("ChannelNotFoundEntity", TransportSubscriptionError, "bind"),
    ],
)
@pytest.mark.asyncio
async def test_native_errors_are_typed_sanitized_and_chained(monkeypatch, native_name, expected, phase):
    native = Native()
    error = type(native_name, (Exception,), {})("SECRET native message")
    setattr(native, phase + "_error", error)
    with pytest.raises(expected) as caught:
        await open_session(adapter(monkeypatch, native))
    assert caught.value.__cause__ is error
    assert "SECRET" not in str(caught.value)
    assert all(connection.is_closed for connection in native.connections)


@pytest.mark.asyncio
async def test_close_deletes_owned_queue_before_channel_and_connection_and_never_acks(monkeypatch):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    message = Message()
    await native.deliver(message)
    await asyncio.gather(*(session.close(deadline=time.monotonic() + 1) for _ in range(3)))
    assert native.connections[0].close_calls == 1
    assert native.events[-3:] == ["delete", "channel_close", "connection_close"]
    assert not message.acks and not session._pending
    assert await session.wait_failure() is None
    with pytest.raises(StopAsyncIteration):
        await anext(session.receive())


@pytest.mark.asyncio
async def test_stalled_cancel_does_not_prevent_connection_cleanup(monkeypatch):
    native = Native()
    native.cancel_gate = asyncio.Event()
    session = await open_session(adapter(monkeypatch, native))
    native.queue.channel.underlay.consumers.clear()
    assert isinstance(await asyncio.wait_for(session.wait_failure(), 1), TransportSubscriptionError)
    await asyncio.wait_for(session.close(deadline=time.monotonic() + 0.1), 1)
    assert native.connections[0].is_closed
    assert native.events[-3:] == ["cancel", "channel_close", "connection_close"]


@pytest.mark.parametrize("stalled", [False, True])
@pytest.mark.asyncio
async def test_unconfirmed_queue_deletion_is_explicit_and_still_closes_connection(monkeypatch, stalled):
    native = Native()
    if stalled:
        native.delete_gate = asyncio.Event()
    else:
        native.delete_error = RuntimeError("SECRET")
    session = await open_session(adapter(monkeypatch, native))
    expected = ObserverCleanupTimeoutError if stalled else ObserverCleanupError
    with pytest.raises(expected, match="RabbitMQ queue deletion") as caught:
        await session.close(deadline=time.monotonic() + 0.1)
    error = caught.value
    assert "SECRET" not in str(error)
    assert error.cause is error.__cause__
    assert isinstance(error.cause, TimeoutError) if stalled else error.cause is native.delete_error
    assert native.connections[0].is_closed
    assert native.events[-3:] == ["delete", "channel_close", "connection_close"]
    assert isinstance(await session.wait_failure(), TransportInterruptedError)


@pytest.mark.parametrize("stalled", [False, True])
@pytest.mark.asyncio
async def test_cleanup_failure_is_honest_and_resolves_failure_signal(monkeypatch, stalled):
    native = Native()
    if stalled:
        native.close_gate = asyncio.Event()
    else:
        native.close_error = RuntimeError("SECRET")
    session = await open_session(adapter(monkeypatch, native))
    expected = ObserverCleanupTimeoutError if stalled else ObserverCleanupError
    with pytest.raises(expected) as error:
        await session.close(deadline=time.monotonic() + 0.1)
    assert "SECRET" not in str(error.value)
    assert not session._closed
    assert isinstance(await session.wait_failure(), TransportInterruptedError)


@pytest.mark.asyncio
async def test_ack_failure_is_terminal_and_token_is_not_forgotten(monkeypatch):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    message = Message()
    message.ack_error = RuntimeError("SECRET")
    await native.deliver(message)
    raw = await anext(session.receive())
    with pytest.raises(TransportInterruptedError) as error:
        await session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED)
    assert "SECRET" not in str(error.value)
    assert raw.delivery_id in session._pending
    assert await session.wait_failure() is error.value
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("failure", ["decode", "oversize", "overflow"])
@pytest.mark.asyncio
async def test_core_failure_never_acknowledges_rejected_delivery(monkeypatch, failure):
    native = Native()
    transport = adapter(monkeypatch, native)
    policy = JournalPolicy(limits=JournalLimits(max_events=1, max_event_bytes=1024, max_bytes=65536))

    class Decoder:
        def decode(self, raw):
            if failure == "decode":
                raise ValueError("SECRET")
            return raw.payload

    message = Message(b"x" * 2048 if failure == "oversize" else b"rejected", tag=2)
    with pytest.raises(ObserverFailedError) as error:
        async with EventObserver(transport=transport, journal=policy).watch(
            source="orders", decoder=Decoder()
        ) as watcher:
            if failure == "overflow":
                first = Message(b"first")
                await native.deliver(first)
                assert await watcher.wait_for(lambda value: value == b"first", timeout=1) == b"first"
                assert first.acks == [False]
            await native.deliver(message)
            await watcher.wait_for(lambda value: value == b"rejected", timeout=1)
    if failure == "oversize":
        assert isinstance(error.value.cause, EventTooLargeError)
    elif failure == "overflow":
        assert isinstance(error.value.cause, JournalOverflowError)
    assert not message.acks


@pytest.mark.parametrize("discard", [False, True])
@pytest.mark.asyncio
async def test_core_acknowledges_only_after_admission_or_explicit_discard(monkeypatch, discard):
    native = Native()
    transport = adapter(monkeypatch, native)
    policy = JournalPolicy(limits=JournalLimits(max_events=1), overflow=OverflowPolicy.DROP_NEWEST)
    async with EventObserver(transport=transport, journal=policy).watch(source="orders") as watcher:
        first = Message(b"first")

        def assert_admitted():
            assert watcher.snapshot().events[0].payload == b"first"

        first.on_ack = assert_admitted
        await native.deliver(first)
        await watcher.wait_for(lambda value: value == b"first", timeout=1)
        assert first.acks == [False]
        if discard:
            second = Message(b"discarded", tag=2)
            second.on_ack = lambda: None
            await native.deliver(second)
            await asyncio.wait_for(second.acked.wait(), 1)
            assert second.acks == [False]
            assert not watcher.snapshot().complete


@pytest.mark.asyncio
async def test_unknown_type_skip_is_settled_without_storing_payload(monkeypatch):
    native = Native()

    class Decoder:
        def decode(self, raw):
            raise UnknownMessageTypeError("Unknown synthetic type")

    async with EventObserver(
        transport=adapter(monkeypatch, native),
    ).watch(
        source="orders",
        decoder=Decoder(),
        decoding=DecodingPolicy(error_policy=DecodeErrorPolicy.SKIP_UNKNOWN_TYPE),
    ) as watcher:
        message = Message()
        await native.deliver(message)
        await asyncio.wait_for(message.acked.wait(), 1)
        assert message.acks == [False]
        assert not watcher.snapshot().events and watcher.snapshot().filtered == 1
