"""Publisher confirmations, owned cleanup, and loop isolation of the RabbitMQ harness."""

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from event_observer.errors import ConfigurationError, TransportError, UnsupportedCapabilityError
from event_observer.models import TransportSource
from event_observer.testing import RabbitMQAdapterHarness
import event_observer.testing.rabbitmq as rabbitmq_module

URL = "amqp://observer:SECRET_PASSWORD@localhost:5672/"


class Ack:
    pass


class Nack:
    pass


class NotFound(Exception):
    pass


@dataclass
class Message:
    body: bytes
    headers: dict


@pytest.fixture
def client(monkeypatch):
    state = SimpleNamespace(
        connections=[],
        exchanges=set(),
        deleted=[],
        publications=[],
        channel_options=[],
        confirmation=Ack(),
        fail_declare=False,
        fail_delete=False,
        fail_publish=False,
        stall_connect=False,
    )

    class Exchange:
        def __init__(self, name):
            self.name = name

        async def publish(self, message, **kwargs):
            state.publications.append((self.name, message, kwargs))
            if state.fail_publish:
                raise RuntimeError("SECRET_NATIVE_PUBLISH")
            return state.confirmation

        async def delete(self, **kwargs):
            if state.fail_delete:
                raise RuntimeError("SECRET_NATIVE_DELETE")
            if self.name not in state.exchanges:
                raise NotFound
            state.deleted.append(self.name)
            state.exchanges.remove(self.name)

    class Channel:
        async def declare_exchange(self, name, **kwargs):
            assert kwargs["type"] == "topic"
            assert kwargs["auto_delete"] is False
            state.exchanges.add(name)
            if state.fail_declare:
                raise RuntimeError("SECRET_NATIVE_DECLARE")
            return Exchange(name)

        async def get_exchange(self, name, *, ensure):
            assert ensure is False
            return Exchange(name)

    class Connection:
        def __init__(self):
            self.loop = asyncio.get_running_loop()
            self.closed = False

        async def channel(self, **kwargs):
            state.channel_options.append(kwargs)
            return Channel()

        async def close(self):
            self.closed = True

    async def connect(url, *, timeout):
        assert url == URL
        if state.stall_connect:
            await asyncio.Event().wait()
        connection = Connection()
        state.connections.append(connection)
        return connection

    native = SimpleNamespace(connect=connect, ExchangeType=SimpleNamespace(TOPIC="topic"), Message=Message)
    amqp = SimpleNamespace(
        spec=SimpleNamespace(Basic=SimpleNamespace(Ack=Ack)),
        exceptions=SimpleNamespace(ChannelNotFoundEntity=NotFound),
    )
    monkeypatch.setattr(rabbitmq_module, "_load_client", lambda: (native, amqp))
    return state


def test_harness_construction_and_empty_cleanup_are_lazy_and_hide_credentials(monkeypatch):
    def forbidden():
        raise AssertionError("The optional client must remain lazy")

    monkeypatch.setattr(rabbitmq_module, "_load_client", forbidden)
    harness = RabbitMQAdapterHarness(URL)
    assert "SECRET_PASSWORD" not in repr(harness)
    assert "SECRET_PASSWORD" not in repr(harness.adapter)
    asyncio.run(harness.cleanup())


def test_publisher_confirmed_receipt_works_across_three_independent_loops(client):
    harness = RabbitMQAdapterHarness(URL)
    source = asyncio.run(harness.create_source(case_id="publisher"))
    headers = (("operation-id", b"42"), ("empty", b""), ("nullable", None))
    receipt = asyncio.run(harness.publish(source, b"", headers=headers, routing_key="orders.created"))
    asyncio.run(harness.cleanup())
    assert receipt.accepted
    assert receipt.metadata["rabbitmq.routing_key"] == "orders.created"
    assert len(client.connections) == 3
    assert len({id(connection.loop) for connection in client.connections}) == 3
    assert all(connection.closed for connection in client.connections)
    assert client.channel_options[1]["publisher_confirms"] is True
    [(_, message, options)] = client.publications
    assert message.body == b""
    assert message.headers == dict(headers)
    assert isinstance(message.headers["operation-id"], bytearray)
    assert options["mandatory"] is False
    assert client.deleted == [source.name]


@pytest.mark.parametrize("confirmation", [None, True, False, Nack()])
@pytest.mark.asyncio
async def test_unconfirmed_or_boolean_publication_never_returns_an_accepted_receipt(client, confirmation):
    harness = RabbitMQAdapterHarness(URL)
    source = await harness.create_source(case_id="unconfirmed")
    client.confirmation = confirmation
    try:
        with pytest.raises(TransportError, match="not confirmed"):
            await harness.publish(source, b"payload")
        assert all(connection.closed for connection in client.connections)
    finally:
        await harness.cleanup()


@pytest.mark.parametrize(
    "payload,kwargs",
    [
        (None, {}),
        (b"payload", {"key": b"key"}),
        (b"payload", {"headers": (("duplicate", b"a"), ("duplicate", b"b"))}),
    ],
)
@pytest.mark.asyncio
async def test_unsupported_envelopes_are_rejected_before_a_publisher_connection(client, payload, kwargs):
    harness = RabbitMQAdapterHarness(URL)
    source = await harness.create_source(case_id="envelope")
    try:
        connections_before = len(client.connections)
        with pytest.raises(UnsupportedCapabilityError):
            await harness.publish(source, payload, **kwargs)
        assert len(client.connections) == connections_before
        assert not client.publications
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_harness_cannot_publish_to_or_delete_an_application_exchange(client):
    harness = RabbitMQAdapterHarness(URL)
    owned = await harness.create_source(case_id="ownership")
    client.exchanges.add("application.exchange")
    with pytest.raises(ConfigurationError):
        await harness.publish(TransportSource("application.exchange"), b"payload")
    await harness.cleanup()
    assert client.deleted == [owned.name]
    assert client.exchanges == {"application.exchange"}
    before = len(client.connections)
    await harness.cleanup()
    assert len(client.connections) == before


@pytest.mark.asyncio
async def test_partially_created_exchange_remains_owned_for_cleanup(client):
    harness = RabbitMQAdapterHarness(URL)
    client.fail_declare = True
    with pytest.raises(TransportError) as caught:
        await harness.create_source(case_id="partial")
    assert "SECRET" not in str(caught.value)
    assert client.exchanges
    assert all(connection.closed for connection in client.connections)
    client.fail_declare = False
    await harness.cleanup()
    assert not client.exchanges
    assert all(connection.closed for connection in client.connections)


@pytest.mark.asyncio
async def test_failed_publish_closes_its_connection_without_exposing_native_error(client):
    harness = RabbitMQAdapterHarness(URL)
    source = await harness.create_source(case_id="publish-failure")
    client.fail_publish = True
    try:
        with pytest.raises(TransportError) as caught:
            await harness.publish(source, b"payload")
        assert "SECRET" not in str(caught.value)
        assert isinstance(caught.value.__cause__, RuntimeError)
        assert all(connection.closed for connection in client.connections)
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_failed_delete_closes_connection_and_keeps_ownership_for_retry(client):
    harness = RabbitMQAdapterHarness(URL)
    source = await harness.create_source(case_id="cleanup-failure")
    client.fail_delete = True
    with pytest.raises(TransportError) as caught:
        await harness.cleanup()
    assert "SECRET" not in str(caught.value)
    assert source.name in client.exchanges
    assert all(connection.closed for connection in client.connections)
    client.fail_delete = False
    await harness.cleanup()
    assert not client.exchanges


@pytest.mark.asyncio
async def test_connection_timeout_is_bounded_and_partial_ownership_can_be_cleaned(client):
    harness = RabbitMQAdapterHarness(URL, timeout=0.01)
    client.stall_connect = True
    with pytest.raises(TransportError) as caught:
        await harness.create_source(case_id="timeout")
    assert isinstance(caught.value.__cause__, TimeoutError)
    client.stall_connect = False
    await harness.cleanup()
    assert all(connection.closed for connection in client.connections)


@pytest.mark.asyncio
async def test_binary_headers_round_trip_through_the_installed_native_field_table_codec(client):
    encode = pytest.importorskip("pamqp.encode")
    decode = pytest.importorskip("pamqp.decode")
    harness = RabbitMQAdapterHarness(URL)
    source = await harness.create_source(case_id="native-header-codec")
    headers = (("binary", b"\xff\x00\x80"), ("text", "Ready 😀".encode()), ("empty", b""), ("nullable", None))
    try:
        await harness.publish(source, b"payload", headers=headers)
        [(_, message, _)] = client.publications
        encoded = encode.field_table(message.headers)
        consumed, decoded = decode.field_table(encoded)
        assert consumed == len(encoded)
        assert isinstance(decoded["binary"], bytearray)
        restored = {name: bytes(value) if value is not None else None for name, value in decoded.items()}
        assert restored == dict(headers)
    finally:
        await harness.cleanup()


@pytest.mark.parametrize("name", ["x" * 129, "é" * 128, "\ud800"])
@pytest.mark.asyncio
async def test_unsafe_native_header_names_fail_before_publication_or_connection(client, name):
    harness = RabbitMQAdapterHarness(URL)
    source = await harness.create_source(case_id="header-name")
    try:
        before = len(client.connections)
        with pytest.raises(ConfigurationError):
            await harness.publish(source, b"payload", headers=((name, b"value"),))
        assert len(client.connections) == before
        assert not client.publications
    finally:
        await harness.cleanup()
