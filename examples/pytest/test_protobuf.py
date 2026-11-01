"""Generated Protobuf classes are available without compiling another schema."""

from uuid import uuid4

import pytest

pytest.importorskip("google.protobuf")
from google.protobuf.wrappers_pb2 import Int32Value, StringValue  # noqa: E402

from event_observer import ScopeStart, SyncEventObserver  # noqa: E402
from event_observer.codecs.protobuf import ResolvingProtobufDecoder  # noqa: E402
from event_observer.codecs.resolvers import HeaderTypeResolver  # noqa: E402
from event_observer.matchers import field_equals, header_equals, on_type  # noqa: E402
from event_observer.testing import FakeTransport  # noqa: E402


def test_message_type_decodes_protobuf(sync_events: SyncEventObserver, transport: FakeTransport):
    expected = StringValue(value="operation-42:completed")

    with sync_events.watch(source="operations", message_type=StringValue) as watcher:
        transport.publish(expected.SerializeToString(), source="operations")

        completed = watcher.wait_for(field_equals("value", "operation-42:completed"), timeout=1)

        # The result is a generated message rather than bytes or JSON.
        assert isinstance(completed, StringValue)
        assert completed.value == "operation-42:completed"


def test_explicit_resolver_handles_mixed_protobuf_types(
    sync_events: SyncEventObserver, transport: FakeTransport
):
    operation_id = uuid4().hex.encode()
    decoder = ResolvingProtobufDecoder(
        resolver=HeaderTypeResolver(
            header="event-type",
            types={b"status": StringValue, b"processed-count": Int32Value},
        )
    )

    with sync_events.watch(source="operations", decoder=decoder) as watcher:
        operation = watcher.scope(header_equals("operation-id", operation_id), start=ScopeStart.from_now())
        transport.publish(
            StringValue(value="completed").SerializeToString(),
            source="operations",
            headers=(("event-type", b"status"), ("operation-id", operation_id)),
        )
        transport.publish(
            Int32Value(value=3).SerializeToString(),
            source="operations",
            headers=(("event-type", b"processed-count"), ("operation-id", operation_id)),
        )

        # on_type protects the nested check from messages of a different type.
        count = operation.wait_for(on_type(Int32Value, field_equals("value", 3)), timeout=1)
        status = operation.wait_for(on_type(StringValue, field_equals("value", "completed")), timeout=1)

        assert isinstance(count, Int32Value)
        assert count.value == 3
        assert isinstance(status, StringValue)
        assert status.value == "completed"
