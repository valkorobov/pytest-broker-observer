"""Core-level codec, correlation and copy guarantees through both facades."""

import asyncio

import pytest
from google.protobuf.wrappers_pb2 import Int32Value, StringValue

from event_observer import (
    DecodeErrorPolicy,
    DecodingPolicy,
    EventObserver,
    JournalLimits,
    JournalPolicy,
    SyncEventObserver,
)
from event_observer.codecs.protobuf import ProtobufDecoder, ResolvingProtobufDecoder
from event_observer.codecs.resolvers import HeaderTypeResolver
from event_observer.errors import AmbiguousMessageTypeError, EventTooLargeError, ObserverFailedError
from event_observer.matchers import field_equals, on_type
from event_observer.testing import FakeTransport


@pytest.mark.parametrize("sync", [False, True])
def test_protobuf_facades_return_independent_messages(sync):
    adapter = FakeTransport()

    def publish():
        adapter.publish(StringValue(value="operation-42").SerializeToString())

    if sync:
        with SyncEventObserver(transport=adapter).watch(source="events", message_type=StringValue) as watcher:
            publish()
            event = watcher.wait_for_event(field_equals("value", "operation-42"), timeout=1)
            event.payload.value = "changed"
            assert watcher.wait_for(field_equals("value", "operation-42"), timeout=1).value == "operation-42"
    else:

        async def scenario():
            async with EventObserver(transport=adapter).watch(
                source="events", message_type=StringValue
            ) as watcher:
                publish()
                event = await watcher.wait_for_event(field_equals("value", "operation-42"), timeout=1)
                event.payload.value = "changed"
                assert (
                    await watcher.wait_for(field_equals("value", "operation-42"), timeout=1)
                ).value == "operation-42"

        asyncio.run(scenario())


@pytest.mark.asyncio
async def test_unknown_types_are_filtered_but_known_types_remain_complete():
    adapter = FakeTransport()
    decoder = ResolvingProtobufDecoder(
        resolver=HeaderTypeResolver(header="type", types={b"text": StringValue, b"int": Int32Value})
    )
    async with EventObserver(transport=adapter).watch(
        source="events",
        decoder=decoder,
        decoding=DecodingPolicy(DecodeErrorPolicy.SKIP_UNKNOWN_TYPE),
    ) as watcher:
        adapter.publish(b"unknown", headers=(("type", b"unknown"),))
        adapter.publish(StringValue(value="ignored").SerializeToString(), headers=(("type", b"text"),))
        adapter.publish(Int32Value(value=42).SerializeToString(), headers=(("type", b"int"),))
        result = await watcher.wait_for(on_type(Int32Value, field_equals("value", 42)), timeout=1)
        assert result.value == 42
        assert watcher.snapshot().complete
        assert watcher.snapshot().filtered == 1


@pytest.mark.asyncio
async def test_ambiguous_type_is_fatal_even_under_skip_policy():
    adapter = FakeTransport()
    decoder = ResolvingProtobufDecoder(
        resolver=HeaderTypeResolver(header="type", types={b"text": StringValue})
    )
    with pytest.raises(ObserverFailedError) as caught:
        async with EventObserver(transport=adapter).watch(
            source="events",
            decoder=decoder,
            decoding=DecodingPolicy(DecodeErrorPolicy.SKIP_UNKNOWN_TYPE),
        ) as watcher:
            adapter.publish(b"", headers=(("type", b"text"), ("type", b"text")))
            await watcher.wait_for(lambda p: True, timeout=1)
    assert isinstance(caught.value.cause, AmbiguousMessageTypeError)


@pytest.mark.asyncio
async def test_hard_event_size_is_checked_before_decoder():
    adapter = FakeTransport()
    called = []

    class Decoder:
        def decode(self, raw):
            called.append(raw)
            return ProtobufDecoder(StringValue).decode(raw)

    policy = JournalPolicy(JournalLimits(max_event_bytes=256, max_bytes=1024))
    with pytest.raises(ObserverFailedError) as caught:
        async with EventObserver(transport=adapter, journal=policy).watch(
            source="events", decoder=Decoder()
        ) as watcher:
            adapter.publish(b"x" * 300)
            await watcher.wait_for(lambda p: True, timeout=1)
    assert isinstance(caught.value.cause, EventTooLargeError)
    assert not called
