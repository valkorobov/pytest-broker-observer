"""Blocking RabbitMQ native boundaries, ownership, and admission-time settlement."""

from collections import deque
import socket
import threading
import time
from types import MethodType, SimpleNamespace

import pytest

from event_observer import StartPosition, TransportBufferLimits
from event_observer.errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    ProcessAffinityError,
    ThreadAffinityError,
    TransportAuthenticationError,
    TransportAuthorizationError,
    TransportBackpressureError,
    TransportConnectionError,
    TransportInterruptedError,
    TransportReadinessError,
    TransportSubscriptionError,
    UnsupportedCapabilityError,
)
from event_observer.models import OpenContext, TransportSource
from event_observer.transports.base import DeliveryOutcome
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig
from event_observer.transports import sync_rabbitmq as module

URL = "amqp://observer:SECRET@127.0.0.1:5672/"
SOURCE = TransportSource("application.exchange")


class NativeImpl:
    def __init__(self, connection):
        self.connection = connection
        self.timers = {}
        self.next_timer = 0

    def _adapter_call_later(self, delay, callback):
        self.next_timer += 1
        self.timers[self.next_timer] = callback
        self.connection.native.timer_delays.append(delay)
        return self.next_timer

    def _adapter_remove_timeout(self, timer):
        self.timers.pop(timer, None)

    def _terminate_stream(self, error):
        self.connection.native.aborts.append(error)
        self.connection.is_closed = True
        self.connection.channel_object.is_closed = True


class NativeConnection:
    def __init__(self, native, parameters):
        self.native = native
        self.parameters = parameters
        self.is_closed = False
        self._impl = NativeImpl(self)
        self.channel_object = NativeChannel(self)

    def channel(self):
        self.native.step("channel")
        return self.channel_object

    def process_data_events(self, *, time_limit):
        self.native.step("pump", time_limit=time_limit)
        if self.native.pump_error is not None:
            raise self.native.pump_error
        while self.native.deliveries:
            body, headers, tag, route = self.native.deliveries.popleft()
            self.channel_object.message_callback(
                self.channel_object,
                SimpleNamespace(delivery_tag=tag, routing_key=route),
                SimpleNamespace(headers=headers),
                body,
            )
        if self.native.cancel_next:
            self.native.cancel_next = False
            self.channel_object.cancel_callback(
                SimpleNamespace(method=SimpleNamespace(consumer_tag=self.channel_object.consumer_tag))
            )

    def close(self):
        self.native.step("close")
        self.is_closed = True
        self.channel_object.is_closed = True


class NativeChannel:
    def __init__(self, connection):
        self.connection = connection
        self.is_closed = False
        self._closing_reason = None
        self.consumer_tag = None
        self.message_callback = None
        self.cancel_callback = None
        self.acks = []

    def exchange_declare(self, **kwargs):
        self.connection.native.step("exchange", **kwargs)

    def basic_qos(self, **kwargs):
        self.connection.native.step("qos", **kwargs)

    def queue_declare(self, **kwargs):
        self.connection.native.step("queue", **kwargs)
        return SimpleNamespace(method=SimpleNamespace(queue=self.connection.native.queue_name))

    def queue_bind(self, **kwargs):
        self.connection.native.step("bind:" + kwargs["routing_key"], **kwargs)

    def queue_delete(self, **kwargs):
        self.connection.native.step("delete", **kwargs)
        self.cancel_callback(SimpleNamespace(method=SimpleNamespace(consumer_tag=self.consumer_tag)))

    def add_on_cancel_callback(self, callback):
        self.cancel_callback = callback

    def basic_consume(self, **kwargs):
        self.connection.native.step("consume", **kwargs)
        self.message_callback = kwargs["on_message_callback"]
        self.consumer_tag = kwargs["consumer_tag"]
        self.connection.native.cancel_next = self.connection.native.cancel_before_ready
        return "unexpected-tag" if self.connection.native.bad_tag else self.consumer_tag

    def basic_ack(self, **kwargs):
        self.connection.native.step("ack", **kwargs)
        if self.connection.native.on_ack is not None:
            self.connection.native.on_ack()
        self.acks.append(kwargs)


class Native:
    def __init__(self, pika):
        self.URLParameters = pika.URLParameters
        self.exceptions = pika.exceptions
        self.connections = []
        self.calls = []
        self.timer_delays = []
        self.aborts = []
        self.deliveries = deque()
        self.queue_name = "amq.gen-observer-only"
        self.bad_tag = False
        self.cancel_before_ready = False
        self.fail_at = None
        self.error = RuntimeError("PRIVATE_NATIVE_ERROR")
        self.expire_at = None
        self.pump_error = None
        self.cancel_next = False
        self.on_ack = None

    def BlockingConnection(self, parameters):
        self.step("connect", parameters=parameters)
        connection = NativeConnection(self, parameters)
        self.connections.append(connection)
        return connection

    def step(self, name, **kwargs):
        self.calls.append((name, kwargs, threading.get_ident()))
        if name == self.fail_at:
            raise self.error
        if name == self.expire_at:
            connection = self.connections[-1]
            timer = next(reversed(connection._impl.timers.values()))
            timer()
            raise self.aborts[-1]

    def deliver(self, body=b"payload", *, headers=None, tag=1, route="events"):
        self.deliveries.append((body, headers, tag, route))


@pytest.fixture
def native(monkeypatch):
    client = Native(pytest.importorskip("pika"))
    monkeypatch.setattr(module, "_load_native", lambda: client)
    return client


def context(*, max_events=8, max_bytes=4096):
    return OpenContext("watcher", time.monotonic() + 3, TransportBufferLimits(max_events, max_bytes))


def open_session(native, *, config=None, limits=None):
    return module.SyncRabbitMQAdapter(config or RabbitMQConfig(URL)).open(
        SOURCE, StartPosition.latest(), context=limits or context()
    )


def close_session(session):
    session.close(deadline=time.monotonic() + 1)


def test_provider_is_lazy_and_keeps_canonical_capabilities(monkeypatch):
    def forbidden():
        raise AssertionError("Constructing an adapter must not import Pika")

    monkeypatch.setattr(module, "_load_native", forbidden)
    driver = RabbitMQAdapter(RabbitMQConfig(URL)).sync_adapter()
    assert isinstance(driver, module.SyncRabbitMQAdapter)
    assert driver.transport_name == "RabbitMQAdapter"
    assert driver.capabilities == RabbitMQAdapter(driver.config).capabilities
    assert driver.capabilities.supports_settlement
    assert not driver.capabilities.supports_key
    assert driver.capabilities.isolation_modes == frozenset({"dedicated_subscription"})
    assert "SECRET" not in repr(driver.config)


def test_invalid_adapter_configuration_is_typed():
    with pytest.raises(ConfigurationError):
        module.SyncRabbitMQAdapter(object())


def test_missing_pika_is_typed_and_does_not_include_connection_url(monkeypatch):
    def missing():
        raise ModuleNotFoundError("pika")

    monkeypatch.setattr(module, "_load_native", missing)
    with pytest.raises(MissingOptionalDependencyError) as caught:
        module.SyncRabbitMQAdapter(RabbitMQConfig(URL)).open(
            SOURCE, StartPosition.latest(), context=context()
        )
    assert "rabbitmq" in str(caught.value)
    assert "SECRET" not in str(caught.value)


@pytest.mark.parametrize(
    "start,source",
    [(StartPosition.earliest(), SOURCE), (StartPosition.latest(), TransportSource("x", partitions=(0,)))],
)
def test_unsupported_native_start_is_rejected_before_import(monkeypatch, start, source):
    def forbidden():
        raise AssertionError("Unsupported mode must be rejected before native import")

    monkeypatch.setattr(module, "_load_native", forbidden)
    with pytest.raises(UnsupportedCapabilityError):
        module.SyncRabbitMQAdapter(RabbitMQConfig(URL)).open(source, start, context=context())


def test_ready_requires_every_binding_and_consume_ok_and_uses_only_private_queue(native):
    session = open_session(
        native,
        config=RabbitMQConfig(URL, binding_keys=("created", "completed"), prefetch_count=100),
        limits=context(max_events=2),
    )
    try:
        names = [name for name, _, _ in native.calls]
        assert names == [
            "connect",
            "channel",
            "exchange",
            "qos",
            "queue",
            "bind:created",
            "bind:completed",
            "consume",
            "pump",
        ]
        operations = {name: arguments for name, arguments, _ in native.calls}
        assert operations["exchange"] == {"exchange": SOURCE.name, "passive": True}
        assert operations["qos"] == {"prefetch_count": 2}
        assert operations["queue"] == {
            "queue": "",
            "durable": False,
            "exclusive": True,
            "auto_delete": True,
            "arguments": {"x-queue-type": "classic"},
        }
        assert operations["consume"]["queue"] == native.queue_name
        assert operations["consume"]["auto_ack"] is False
        assert operations["consume"]["exclusive"] is True
        assert all(thread == threading.get_ident() for _, _, thread in native.calls)
        parameters = native.connections[0].parameters
        assert parameters.connection_attempts == 1
        assert parameters.retry_delay == 0
        assert 0 < parameters.socket_timeout <= parameters.stack_timeout <= 3
        assert session.readiness.boundary.source == SOURCE.name
        assert session.readiness.source_metadata["rabbitmq.prefetch_count"] == 2
        assert len(native.timer_delays) == len(native.calls) - 1
    finally:
        close_session(session)


@pytest.mark.parametrize("stage", ["channel", "exchange", "qos", "queue", "bind:#", "consume", "pump"])
def test_each_startup_rpc_uses_native_deadline_and_aborts_its_owned_stream(native, stage):
    native.expire_at = stage
    with pytest.raises(TimeoutError):
        open_session(native)
    assert len(native.aborts) == 1
    assert native.connections[0].is_closed
    assert not any(name == "ack" for name, _, _ in native.calls)


def test_bad_consume_tag_never_becomes_ready_and_rolls_back(native):
    native.bad_tag = True
    with pytest.raises(TransportReadinessError):
        open_session(native)
    assert native.connections[0].is_closed
    assert "delete" not in [name for name, _, _ in native.calls]


@pytest.mark.parametrize(
    "exception,expected",
    [
        ("ProbableAuthenticationError", TransportAuthenticationError),
        ("ProbableAccessDeniedError", TransportAuthorizationError),
        ("AMQPConnectionError", TransportConnectionError),
    ],
)
def test_native_connection_errors_have_safe_types_and_original_causes(native, exception, expected):
    native.fail_at = "connect"
    native.error = getattr(native.exceptions, exception)("SECRET_NATIVE")
    with pytest.raises(expected) as caught:
        open_session(native)
    assert caught.value.cause is caught.value.__cause__ is native.error
    assert "SECRET" not in str(caught.value)


@pytest.mark.parametrize(
    "code,expected", [(403, TransportAuthorizationError), (404, TransportSubscriptionError)]
)
def test_native_exchange_errors_are_typed_and_partial_connection_closes(native, code, expected):
    native.fail_at = "exchange"
    native.error = native.exceptions.ChannelClosedByBroker(code, "SECRET_NATIVE")
    with pytest.raises(expected) as caught:
        open_session(native)
    assert caught.value.cause is caught.value.__cause__ is native.error
    assert native.connections[0].is_closed
    assert "SECRET" not in str(caught.value)


def test_binary_headers_and_empty_body_are_detached_without_pre_admission_ack(native):
    session = open_session(native)
    binary = bytearray(b"\x00\xff")
    native.deliver(b"", headers={"binary": binary, "text": "Ready", "null": None}, route="created")
    try:
        raw = session.poll(0)
        binary[:] = b"xx"
        assert raw.payload == b""
        assert raw.source == SOURCE.name
        assert raw.headers == (("binary", b"\x00\xff"), ("text", b"Ready"), ("null", None))
        assert raw.metadata["rabbitmq.routing_key"] == "created"
        assert raw.delivery_id
        assert native.connections[0].channel_object.acks == []
        session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED)
        assert native.connections[0].channel_object.acks == [{"delivery_tag": 1, "multiple": False}]
        with pytest.raises(TransportSubscriptionError):
            session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED)
    finally:
        close_session(session)


@pytest.mark.parametrize(
    "body,headers,tag,route",
    [
        (None, None, 1, "events"),
        (b"x", {"nested": {}}, 1, "events"),
        (b"x", {"integer": 1}, 1, "events"),
        (b"x", None, True, "events"),
        (b"x", None, 1, b"events"),
    ],
)
def test_malformed_delivery_is_terminal_and_never_acknowledged(native, body, headers, tag, route):
    session = open_session(native)
    native.deliver(body, headers=headers, tag=tag, route=route)
    try:
        with pytest.raises(TransportSubscriptionError):
            session.poll(0)
        assert isinstance(session.failure(), TransportSubscriptionError)
        assert not native.connections[0].channel_object.acks
    finally:
        close_session(session)


@pytest.mark.parametrize("limit", ["events", "bytes"])
def test_outstanding_deliveries_enforce_both_handoff_quotas(native, limit):
    session = open_session(
        native, limits=context(max_events=1) if limit == "events" else context(max_bytes=256)
    )
    native.deliver(b"x" if limit == "events" else b"x" * 512, tag=1)
    if limit == "events":
        native.deliver(b"second", tag=2)
    try:
        with pytest.raises(TransportBackpressureError):
            session.poll(0)
        assert not native.connections[0].channel_object.acks
    finally:
        close_session(session)


def test_ack_reentrant_delivery_does_not_false_overflow_prefetch_one(native):
    session = open_session(
        native, config=RabbitMQConfig(URL, prefetch_count=1), limits=context(max_events=1, max_bytes=512)
    )
    native.deliver(b"one", tag=1)
    try:
        first = session.poll(0)

        def next_delivery():
            channel = native.connections[0].channel_object
            channel.message_callback(
                channel,
                SimpleNamespace(delivery_tag=2, routing_key="events"),
                SimpleNamespace(headers=None),
                b"two",
            )

        native.on_ack = next_delivery
        session.settle(first.delivery_id, DeliveryOutcome.ADMITTED)
        assert session.failure() is None
        assert session.poll(0).payload == b"two"
    finally:
        close_session(session)


def test_ack_failure_is_terminal_retains_token_and_native_cause(native):
    session = open_session(native)
    native.deliver()
    try:
        raw = session.poll(0)
        native.fail_at = "ack"
        with pytest.raises(TransportInterruptedError) as caught:
            session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED)
        assert caught.value.cause is caught.value.__cause__ is native.error
        assert session.failure() is caught.value
        assert raw.delivery_id in session._pending
        assert not native.connections[0].channel_object.acks
    finally:
        native.fail_at = None
        close_session(session)


def test_broker_cancellation_has_priority_over_buffered_deliveries(native):
    session = open_session(native)
    native.deliver(b"one", tag=1)
    native.deliver(b"two", tag=2)
    try:
        assert session.poll(0).payload == b"one"
        native.cancel_next = True
        with pytest.raises(TransportSubscriptionError):
            session.poll(0)
        assert isinstance(session.failure(), TransportSubscriptionError)
    finally:
        close_session(session)


def test_idle_poll_pumps_native_heartbeats_with_finite_slices(native):
    session = open_session(native)
    try:
        assert session.poll(0) is None
        assert session.poll(1000) is None
        pumps = [arguments["time_limit"] for name, arguments, _ in native.calls if name == "pump"]
        assert pumps == [0, 0, module._IO_SLICE]
    finally:
        close_session(session)


@pytest.mark.parametrize("timeout", [-1, True, float("nan"), float("inf"), 10**1000])
def test_invalid_poll_timeout_is_typed_without_native_io(native, timeout):
    session = open_session(native)
    try:
        calls = len(native.calls)
        with pytest.raises(ConfigurationError):
            session.poll(timeout)
        assert len(native.calls) == calls
    finally:
        close_session(session)


def test_native_session_rejects_foreign_thread_and_process(native, monkeypatch):
    session = open_session(native)
    errors = []
    operations = (lambda: session.poll(0), session.failure, lambda: close_session(session))

    def foreign():
        for operation in operations:
            try:
                operation()
            except ThreadAffinityError as error:
                errors.append(error)

    try:
        worker = threading.Thread(target=foreign)
        worker.start()
        worker.join(1)
        assert not worker.is_alive() and len(errors) == len(operations)
        with monkeypatch.context() as patch:
            patch.setattr(module.os, "getpid", lambda: session._pid + 1)
            for operation in operations:
                with pytest.raises(ProcessAffinityError):
                    operation()
    finally:
        close_session(session)


def test_close_deletes_only_owned_queue_before_connection_and_never_acks(native):
    session = open_session(native)
    native.deliver()
    assert session.poll(0) is not None
    close_session(session)
    close_session(session)
    assert session.poll(0) is None
    assert session.failure() is None
    cleanup_calls = [(name, kwargs) for name, kwargs, _ in native.calls if name in {"delete", "close"}]
    assert cleanup_calls == [("delete", {"queue": native.queue_name}), ("close", {})]
    assert not native.connections[0].channel_object.acks


@pytest.mark.parametrize("expires_at", ["before_call", "delete", "close"])
def test_cleanup_deadline_aborts_only_owned_connection_and_is_explicit(native, expires_at):
    session = open_session(native)
    native.expire_at = expires_at
    with pytest.raises(ObserverCleanupTimeoutError) as caught:
        session.close(deadline=time.monotonic() + (-1 if expires_at == "before_call" else 1))
    assert isinstance(caught.value.cause, TimeoutError)
    assert len(native.aborts) == 1
    assert native.connections[0].is_closed
    assert not native.connections[0].channel_object.acks
    close_session(session)


@pytest.mark.parametrize("fails_at", ["delete", "close"])
def test_cleanup_failure_aborts_resources_and_preserves_native_cause(native, fails_at):
    session = open_session(native)
    native.fail_at = fails_at
    with pytest.raises(ObserverCleanupError) as caught:
        close_session(session)
    assert caught.value.cause is caught.value.__cause__ is native.error
    assert native.connections[0].is_closed
    assert native.aborts == [native.error]


def test_installed_native_field_table_decodes_async_publisher_binary_headers():
    pika = pytest.importorskip("pika")
    encode = pytest.importorskip("pamqp.encode")
    encoded = encode.field_table({"binary": bytearray(b"\x00\xff"), "text": "Ready", "null": None})
    headers, consumed = pika.data.decode_table(encoded, 0)
    assert consumed == len(encoded)
    assert module._normalize_headers(headers) == (("binary", b"\x00\xff"), ("null", None), ("text", b"Ready"))


def test_pinned_native_rpc_flush_processes_deadline_and_owned_socket_abort(monkeypatch):
    pika = pytest.importorskip("pika")
    blocking = pytest.importorskip("pika.adapters.blocking_connection")
    select = pytest.importorskip("pika.adapters.select_connection")

    def wakeup_pair():
        reader, writer = socket.socketpair()
        reader.setblocking(False)
        writer.setblocking(False)
        return reader, writer

    monkeypatch.setattr(pika.compat, "nonblocking_socketpair", wakeup_pair)
    loop = select.IOLoop()
    loop.activate_poller()
    owned, peer = socket.socketpair()
    connection = pika.BlockingConnection.__new__(pika.BlockingConnection)
    connection._cleanup_mutex = threading.Lock()
    connection._ready_events = deque()
    connection._closed_result = blocking._CallbackResult(connection._OnClosedArgs)
    impl = SimpleNamespace(
        is_closed=False,
        ioloop=loop,
        _transport=object(),
        _get_write_buffer_size=lambda: 0,
        _remove_heartbeat=lambda: None,
    )
    connection._impl = impl
    impl._adapter_call_later = loop.call_later
    impl._adapter_remove_timeout = loop.remove_timeout

    def disconnect():
        owned.close()
        impl.is_closed = True
        connection._closed_result.set_value_once(impl, impl._error)

    impl._adapter_disconnect_stream = disconnect
    impl._terminate_stream = MethodType(pika.connection.Connection._terminate_stream, impl)
    session = module.SyncRabbitMQSession(RabbitMQConfig(URL), SOURCE, context())
    session._connection = connection
    try:
        with pytest.raises(TimeoutError):
            session._bounded_call(lambda: connection._flush_output(lambda: False), time.monotonic() + 0.02)
        assert connection.is_closed
        assert owned.fileno() == -1
    finally:
        owned.close()
        peer.close()
        if not connection.is_closed:
            loop.close()


def test_broker_cancellation_during_consume_ok_never_publishes_readiness(native):
    native.cancel_before_ready = True
    with pytest.raises(TransportSubscriptionError):
        open_session(native)
    assert native.connections[0].is_closed


def test_driver_does_not_create_or_run_an_asyncio_loop(native, monkeypatch):
    import asyncio

    def forbidden(*args, **kwargs):
        raise AssertionError("The blocking RabbitMQ driver attempted to use asyncio")

    monkeypatch.setattr(asyncio, "new_event_loop", forbidden)
    monkeypatch.setattr(asyncio, "run", forbidden)
    monkeypatch.setattr(asyncio, "get_running_loop", forbidden)
    session = open_session(native)
    native.deliver()
    raw = session.poll(0)
    session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED)
    close_session(session)


def test_ack_flush_deadline_is_terminal_and_keeps_original_timeout_cause(native):
    session = open_session(native)
    native.deliver()
    raw = session.poll(0)
    native.expire_at = "ack"
    with pytest.raises(TransportInterruptedError) as caught:
        session.settle(raw.delivery_id, DeliveryOutcome.ADMITTED)
    assert isinstance(caught.value.cause, TimeoutError)
    assert caught.value.cause is caught.value.__cause__
    assert session.failure() is caught.value
    assert native.connections[0].is_closed
    assert not native.connections[0].channel_object.acks
    close_session(session)
