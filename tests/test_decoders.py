import pytest

from event_observer.decoders import (
    CallableTypeResolver,
    DefaultPayloadAccess,
    HeaderTypeResolver,
    ProtobufDecoder,
    RawDecoder,
    ResolvingProtobufDecoder,
    SourceTypeResolver,
)
from event_observer.errors import (
    AmbiguousMessageTypeError,
    InvalidPayloadError,
    NullPayloadError,
    PayloadIsolationError,
    TypeResolverError,
    UnknownMessageTypeError,
    UnsupportedMessageTypeError,
)
from event_observer.models import RawEvent

protobuf = pytest.importorskip("google.protobuf")
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory  # noqa: E402
from google.protobuf.wrappers_pb2 import Int32Value, StringValue  # noqa: E402


def raw(payload=b"", *, headers=(), source="example.events"):
    return RawEvent(payload=payload, source=source, headers=headers)


def test_raw_distinguishes_empty_and_null():
    decoder = RawDecoder()
    assert decoder.decode(raw(None)) is None
    assert decoder.decode(raw(b"")) == b""


def test_protobuf_empty_is_valid_and_null_is_error():
    decoder = ProtobufDecoder(StringValue)
    assert decoder.decode(raw()).value == ""
    with pytest.raises(NullPayloadError):
        decoder.decode(raw(None))


def test_protobuf_parse_is_a_fresh_value_and_preserves_cause():
    payload = StringValue(value="operation-42").SerializeToString()
    decoder = ProtobufDecoder(StringValue)
    first, second = decoder.decode(raw(payload)), decoder.decode(raw(payload))
    first.value = "changed"
    assert second.value == "operation-42"
    with pytest.raises(InvalidPayloadError) as caught:
        decoder.decode(raw(b"\xff"))
    assert caught.value.__cause__ is not None


def test_proto2_required_initialization_policy():
    file = descriptor_pb2.FileDescriptorProto(name="required_test.proto", syntax="proto2")
    message = file.message_type.add(name="RequiredEvent")
    message.field.add(name="id", number=1, label=2, type=9)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("RequiredEvent"))
    with pytest.raises(InvalidPayloadError):
        ProtobufDecoder(cls).decode(raw())
    assert not ProtobufDecoder(cls, validate_required_fields=False).decode(raw()).IsInitialized()


def test_header_resolver_requires_unique_even_if_equal():
    resolver = HeaderTypeResolver(header="event_type", types={b"created": StringValue})
    with pytest.raises(AmbiguousMessageTypeError):
        resolver.resolve(raw(headers=(("event_type", b"created"), ("event_type", b"created"))))


@pytest.mark.parametrize(
    "headers, reason",
    [
        ((), "missing_discriminator"),
        ((("event_type", None),), "missing_discriminator"),
        ((("event_type", b"other"),), "unregistered_type"),
    ],
)
def test_unknown_type_has_stable_reason(headers, reason):
    resolver = HeaderTypeResolver(header="event_type", types={b"created": StringValue})
    with pytest.raises(UnknownMessageTypeError) as caught:
        resolver.resolve(raw(headers=headers))
    assert caught.value.reason == reason


def test_text_resolver_requires_explicit_encoding_and_valid_input():
    with pytest.raises(TypeResolverError):
        HeaderTypeResolver(header="event_type", types={"created": StringValue})
    resolver = HeaderTypeResolver(header="event_type", encoding="utf-8", types={"created": StringValue})
    assert resolver.resolve(raw(headers=(("event_type", b"created"),))) is StringValue
    with pytest.raises(TypeResolverError):
        resolver.resolve(raw(headers=(("event_type", b"\xff"),)))


def test_mixed_decoder_uses_explicit_schema_and_keeps_invalid_fatal():
    resolver = HeaderTypeResolver(header="event_type", types={b"text": StringValue, b"int": Int32Value})
    decoder = ResolvingProtobufDecoder(resolver=resolver)
    assert set(decoder.known_message_types) == {StringValue, Int32Value}
    result = decoder.decode(raw(Int32Value(value=42).SerializeToString(), headers=(("event_type", b"int"),)))
    assert isinstance(result, Int32Value) and result.value == 42
    with pytest.raises(InvalidPayloadError):
        decoder.decode(raw(b"\xff", headers=(("event_type", b"text"),)))


def test_source_and_callable_resolvers_validate_classes():
    assert SourceTypeResolver({"example.events": StringValue}).resolve(raw()) is StringValue
    with pytest.raises(UnsupportedMessageTypeError):
        CallableTypeResolver(lambda _: str).resolve(raw())

    def failure(_):
        raise RuntimeError("SECRET")

    with pytest.raises(TypeResolverError) as caught:
        CallableTypeResolver(failure).resolve(raw())
    assert str(caught.value.__cause__) == "SECRET"
    assert "SECRET" not in str(caught.value)


def test_async_type_resolver_is_rejected_and_coroutine_closed():
    import inspect

    async def async_resolver(_):
        return StringValue

    pending = async_resolver(raw())
    with pytest.raises(UnsupportedMessageTypeError):
        CallableTypeResolver(lambda _: pending).resolve(raw())
    assert inspect.getcoroutinestate(pending) == inspect.CORO_CLOSED


def test_payload_access_copies_mutable_python_and_protobuf():
    access = DefaultPayloadAccess()
    original = {"ids": ["42"]}
    clone = access.clone_for_user(original)
    clone["ids"].append("43")
    assert original == {"ids": ["42"]}
    message = StringValue(value="42")
    message_clone = access.clone_for_user(message)
    message_clone.value = "43"
    assert message.value == "42"
    assert access.estimate_bytes(message) == 128 + 2 * message.ByteSize()


def test_payload_access_failure_is_explicit():
    class Uncopyable:
        def __deepcopy__(self, memo):
            raise RuntimeError("cannot copy")

    with pytest.raises(PayloadIsolationError):
        DefaultPayloadAccess().clone_for_user(Uncopyable())


def test_payload_estimate_handles_cycles():
    value = []
    value.append(value)
    assert DefaultPayloadAccess().estimate_bytes(value) > 0


def test_payload_estimate_counts_dataclass_dict_and_inherited_private_slots():
    from dataclasses import dataclass

    @dataclass
    class Event:
        blob: bytes

    class Parent:
        __slots__ = "__blob"

        def __init__(self, blob):
            self.__blob = blob

    class Child(Parent):
        __slots__ = "event"

        def __init__(self, blob, event):
            super().__init__(blob)
            self.event = event

    first = b"a" * 200_000
    second = b"b" * 300_000
    payload = Child(first, Event(second))
    assert DefaultPayloadAccess().estimate_bytes(payload) >= 500_000


def test_payload_estimate_avoids_custom_attribute_access():
    class Event:
        def __init__(self):
            object.__setattr__(self, "blob", b"a" * 200_000)

        def __getattribute__(self, name):
            raise AssertionError("custom attribute callback invoked")

    assert DefaultPayloadAccess().estimate_bytes(Event()) >= 200_000


def test_payload_estimate_fails_bounded_traversal_for_huge_shared_container():
    with pytest.raises(PayloadIsolationError, match="traversal budget"):
        DefaultPayloadAccess().estimate_bytes([None] * 100_001)
