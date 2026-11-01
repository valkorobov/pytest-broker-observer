"""Owned stream initialization, native XADD receipts, and per-call client cleanup."""

import asyncio
from types import SimpleNamespace

import pytest

from event_observer.errors import ConfigurationError, TransportError, UnsupportedCapabilityError
from event_observer.models import TransportSource
from event_observer.testing import RedisStreamsAdapterHarness
import event_observer.testing.redis_streams as streams_module

URL = "redis://:SECRET_PASSWORD@localhost:6379/0"
_UNSET = object()


@pytest.fixture
def client(monkeypatch):
    state = SimpleNamespace(
        clients=[],
        streams={},
        deleted=[],
        writes=[],
        next_id=1,
        response=_UNSET,
        fail_xdel=False,
        fail_xadd=False,
        fail_delete=False,
        stall_xadd=False,
    )
    retry = object()

    class Client:
        def __init__(self, options):
            self.options = options
            self.loop = asyncio.get_running_loop()
            self.closed = False

        async def xadd(self, name, fields, *, nomkstream=False):
            state.writes.append((name, fields, nomkstream))
            if state.stall_xadd:
                await asyncio.Event().wait()
            if state.fail_xadd:
                raise RuntimeError("SECRET_NATIVE_XADD")
            if nomkstream and name not in state.streams:
                return None
            entry_id = f"{state.next_id}-0".encode()
            state.next_id += 1
            state.streams.setdefault(name, {})[entry_id] = dict(fields)
            return entry_id if state.response is _UNSET else state.response

        async def xdel(self, name, entry_id):
            if state.fail_xdel:
                raise RuntimeError("SECRET_NATIVE_XDEL")
            if isinstance(entry_id, str):
                entry_id = entry_id.encode()
            return int(state.streams[name].pop(entry_id, None) is not None)

        async def delete(self, name):
            if state.fail_delete:
                raise RuntimeError("SECRET_NATIVE_DELETE")
            state.deleted.append(name)
            return int(state.streams.pop(name, None) is not None)

        async def aclose(self):
            self.closed = True

    def from_url(url, **kwargs):
        assert url == URL
        assert kwargs["decode_responses"] is False
        assert kwargs["protocol"] == 2
        assert kwargs["retry"] is retry
        result = Client(kwargs)
        state.clients.append(result)
        return result

    monkeypatch.setattr(streams_module, "_load_client", lambda: (SimpleNamespace(from_url=from_url), retry))
    return state


def test_harness_construction_and_empty_cleanup_are_lazy_and_do_not_expose_the_url(monkeypatch):
    def forbidden():
        raise AssertionError("The optional client must remain lazy")

    monkeypatch.setattr(streams_module, "_load_client", forbidden)
    harness = RedisStreamsAdapterHarness(URL)
    assert "SECRET_PASSWORD" not in repr(harness)
    assert "SECRET_PASSWORD" not in repr(harness.adapter)
    asyncio.run(harness.cleanup())


def test_empty_typed_stream_and_receipt_work_across_three_independent_event_loops(client):
    harness = RedisStreamsAdapterHarness(URL)
    source = asyncio.run(harness.create_source(case_id="empty-stream"))
    assert source.name in client.streams
    assert client.streams[source.name] == {}
    headers = (("operation-id", b"42"), ("binary", b"\xff\x00"), ("empty", b""))
    receipt = asyncio.run(harness.publish(source, b"", headers=headers))
    assert receipt.accepted
    assert receipt.metadata["redis.stream_id"] == "2-0"
    assert client.streams[source.name][b"2-0"] == {
        b"payload": b"",
        b"header:operation-id": b"42",
        b"header:binary": b"\xff\x00",
        b"header:empty": b"",
    }
    assert client.writes[-1][2] is True
    asyncio.run(harness.cleanup())
    assert len(client.clients) == 3
    assert len({id(instance.loop) for instance in client.clients}) == 3
    assert all(instance.closed for instance in client.clients)
    assert client.deleted == [source.name]


@pytest.mark.parametrize(
    "payload,kwargs",
    [
        (None, {}),
        (b"payload", {"key": b"key"}),
        (b"payload", {"headers": (("duplicate", b"a"), ("duplicate", b"b"))}),
        (b"payload", {"headers": (("nullable", None),)}),
    ],
)
@pytest.mark.asyncio
async def test_unsupported_envelopes_are_rejected_before_a_publisher_client(client, payload, kwargs):
    harness = RedisStreamsAdapterHarness(URL)
    source = await harness.create_source(case_id="unsupported")
    try:
        before = len(client.clients)
        with pytest.raises(UnsupportedCapabilityError):
            await harness.publish(source, payload, **kwargs)
        assert len(client.clients) == before
        assert len(client.writes) == 1
    finally:
        await harness.cleanup()


@pytest.mark.parametrize(
    "response", [None, True, b"not-an-id", b"\xff-0", "0-0", "01-0", "18446744073709551616-0"]
)
@pytest.mark.asyncio
async def test_only_valid_xadd_entry_ids_produce_a_receipt(client, response):
    harness = RedisStreamsAdapterHarness(URL)
    source = await harness.create_source(case_id="receipt")
    client.response = response
    try:
        with pytest.raises(TransportError):
            await harness.publish(source, b"payload")
        assert all(instance.closed for instance in client.clients)
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_partial_stream_creation_is_owned_until_cleanup(client):
    harness = RedisStreamsAdapterHarness(URL)
    client.fail_xdel = True
    with pytest.raises(TransportError) as caught:
        await harness.create_source(case_id="partial")
    assert "SECRET" not in str(caught.value)
    assert client.streams
    assert all(instance.closed for instance in client.clients)
    client.fail_xdel = False
    await harness.cleanup()
    assert not client.streams


@pytest.mark.asyncio
async def test_harness_never_appends_to_or_deletes_a_foreign_stream(client):
    harness = RedisStreamsAdapterHarness(URL)
    own = await harness.create_source(case_id="ownership")
    client.streams["application.stream"] = {b"1-0": {b"payload": b"keep"}}
    with pytest.raises(ConfigurationError):
        await harness.publish(TransportSource("application.stream"), b"payload")
    await harness.cleanup()
    assert client.deleted == [own.name]
    assert client.streams["application.stream"] == {b"1-0": {b"payload": b"keep"}}
    before = len(client.clients)
    await harness.cleanup()
    assert len(client.clients) == before


@pytest.mark.asyncio
async def test_publish_does_not_recreate_an_owned_stream_that_disappeared(client):
    harness = RedisStreamsAdapterHarness(URL)
    source = await harness.create_source(case_id="disappeared")
    del client.streams[source.name]
    try:
        with pytest.raises(TransportError):
            await harness.publish(source, b"payload")
        assert source.name not in client.streams
        assert all(instance.closed for instance in client.clients)
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_native_write_failure_closes_its_client_and_keeps_a_safe_summary(client):
    harness = RedisStreamsAdapterHarness(URL)
    source = await harness.create_source(case_id="write-failure")
    client.fail_xadd = True
    try:
        with pytest.raises(TransportError) as caught:
            await harness.publish(source, b"payload")
        assert "SECRET" not in str(caught.value)
        assert isinstance(caught.value.__cause__, RuntimeError)
        assert all(instance.closed for instance in client.clients)
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_cleanup_failure_retains_ownership_for_retry_and_closes_its_client(client):
    harness = RedisStreamsAdapterHarness(URL)
    source = await harness.create_source(case_id="delete-failure")
    client.fail_delete = True
    with pytest.raises(TransportError):
        await harness.cleanup()
    assert source.name in client.streams
    assert all(instance.closed for instance in client.clients)
    client.fail_delete = False
    await harness.cleanup()
    assert not client.streams


@pytest.mark.asyncio
async def test_timeout_closes_the_partial_client_and_preserves_cleanup_ownership(client):
    harness = RedisStreamsAdapterHarness(URL, timeout=0.01)
    client.stall_xadd = True
    with pytest.raises(TransportError) as caught:
        await harness.create_source(case_id="timeout")
    assert isinstance(caught.value.__cause__, TimeoutError)
    assert all(instance.closed for instance in client.clients)
    client.stall_xadd = False
    await harness.cleanup()


@pytest.mark.asyncio
async def test_payload_and_headers_survive_the_installed_binary_redis_encoder(client):
    encoders = pytest.importorskip("redis._parsers.encoders")
    encoder = encoders.Encoder("utf-8", "strict", False)
    harness = RedisStreamsAdapterHarness(URL)
    source = await harness.create_source(case_id="native-encoding")
    try:
        await harness.publish(source, b"\xff\x00", headers=(("label", "Ready 😀".encode()),))
        _, fields, _ = client.writes[-1]
        assert {encoder.encode(name): encoder.encode(value) for name, value in fields.items()} == {
            b"payload": b"\xff\x00",
            b"header:label": "Ready 😀".encode(),
        }
    finally:
        await harness.cleanup()
