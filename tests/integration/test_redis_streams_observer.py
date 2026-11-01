"""Real Redis Streams replay, facade, group isolation, and continuity checks."""

import asyncio
import os
from uuid import uuid4

import pytest
from google.protobuf.wrappers_pb2 import StringValue

from event_observer import EventObserver, ScopeStart, StartPosition, SyncEventObserver
from event_observer.errors import ObserverFailedError, TransportError, TransportInterruptedError
from event_observer.matchers import field_equals, header_equals
from event_observer.testing import RedisStreamsAdapterHarness

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("REDIS_URL"), reason="Set REDIS_URL for real Redis Streams observation"
    ),
]


@pytest.fixture
def real_stream_source():
    harness = RedisStreamsAdapterHarness(os.environ["REDIS_URL"])
    try:
        source = asyncio.run(harness.create_source(case_id="stream-observer"))
        yield harness, source
    finally:
        asyncio.run(harness.cleanup())


def test_sync_stream_protobuf_correlation_with_independent_publisher_loops(real_stream_source):
    harness, source = real_stream_source
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
        observed = operation.wait_for_event(field_equals("value", "completed"), timeout=5)
        assert observed.raw.source == source.name
        assert observed.raw.metadata["redis.stream_id"] == receipt.metadata["redis.stream_id"]
        observed.payload.value = "changed"
        assert operation.wait_for(field_equals("value", "completed"), timeout=5).value == "completed"


@pytest.mark.asyncio
async def test_async_stream_independent_waiters_preserve_protobuf_and_binary_headers(real_stream_source):
    harness, source = real_stream_source
    headers = (("operation-id", b"operation-42"), ("binary", b"\xff\x00"), ("empty", b""))
    async with EventObserver(transport=harness.adapter).watch(
        source=source, message_type=StringValue
    ) as watcher:
        operation = watcher.scope(header_equals("operation-id", b"operation-42"))
        payload_wait = asyncio.create_task(operation.wait_for(field_equals("value", "completed"), timeout=5))
        event_wait = asyncio.create_task(operation.wait_for_event(lambda _: True, timeout=5))
        try:
            receipt = await harness.publish(
                source, StringValue(value="completed").SerializeToString(), headers=headers
            )
            payload, observed = await asyncio.gather(payload_wait, event_wait)
        finally:
            for task in (payload_wait, event_wait):
                task.cancel()
            await asyncio.gather(payload_wait, event_wait, return_exceptions=True)
        assert payload.value == observed.payload.value == "completed"
        assert payload is not observed.payload
        assert dict(observed.raw.headers) == dict(headers)
        assert observed.raw.metadata["redis.stream_id"] == receipt.metadata["redis.stream_id"]


@pytest.mark.asyncio
async def test_stream_observer_leaves_application_consumer_group_and_pending_entries_untouched(
    real_stream_source,
):
    import redis.asyncio as redis

    harness, source = real_stream_source
    client = redis.from_url(os.environ["REDIS_URL"], protocol=2, decode_responses=False)
    group = "application-" + uuid4().hex
    try:
        first = await harness.publish(source, b"pending-application-entry")
        await client.xgroup_create(source.name, group, id="0-0", mkstream=False)
        delivered = await client.xreadgroup(group, "application-consumer", {source.name: ">"}, count=1)
        assert delivered[0][1][0][0].decode() == first.metadata["redis.stream_id"]
        before = await client.xpending_range(source.name, group, "-", "+", 10)
        async with EventObserver(transport=harness.adapter).watch(
            source=source, start=StartPosition.earliest()
        ) as watcher:
            assert (
                await watcher.wait_for(lambda value: value == b"pending-application-entry", timeout=5)
                == b"pending-application-entry"
            )
            second = await harness.publish(source, b"next-application-entry")
            assert (
                await watcher.wait_for(lambda value: value == b"next-application-entry", timeout=5)
                == b"next-application-entry"
            )
        after = await client.xpending_range(source.name, group, "-", "+", 10)

        def identity(item):
            return item["message_id"], item["consumer"], item["times_delivered"]

        assert list(map(identity, after)) == list(map(identity, before))
        [group_info] = await client.xinfo_groups(source.name)
        assert group_info["pending"] == 1
        assert group_info["consumers"] == 1
        assert group_info["last-delivered-id"].decode() == first.metadata["redis.stream_id"]
        next_delivery = await client.xreadgroup(group, "application-consumer", {source.name: ">"}, count=1)
        assert next_delivery[0][1][0][0].decode() == second.metadata["redis.stream_id"]
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_latest_uses_last_generated_id_when_last_entry_was_deleted_and_accepts_native_id_gaps(
    real_stream_source,
):
    import redis.asyncio as redis

    harness, source = real_stream_source
    client = redis.from_url(os.environ["REDIS_URL"], protocol=2, decode_responses=False)
    try:
        seed = (await client.xinfo_stream(source.name))["last-generated-id"].decode()
        tail_id = f"{int(seed.split('-')[0]) + 1000}-17"
        assert await client.xadd(source.name, {"payload": b"deleted-tail"}, id=tail_id) == tail_id.encode()
        assert await client.xdel(source.name, tail_id) == 1
        assert await client.xlen(source.name) == 0
        async with EventObserver(transport=harness.adapter).watch(source=source) as watcher:
            assert watcher.readiness.source_metadata["redis.start_id"] == tail_id
            receipt = await harness.publish(source, b"new-live-entry")
            observed = await watcher.wait_for_event(lambda _: True, timeout=5)
            assert observed.sequence == 0
            assert observed.payload == b"new-live-entry"
            assert observed.raw.metadata["redis.stream_id"] == receipt.metadata["redis.stream_id"]
            assert watcher.snapshot().complete
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_explicit_stream_id_replay_is_exclusive_and_preserves_entry_ids(real_stream_source):
    harness, source = real_stream_source
    first = await harness.publish(source, b"one")
    second = await harness.publish(source, b"two")
    third = await harness.publish(source, b"three")
    async with EventObserver(transport=harness.adapter).watch(
        source=source,
        start=StartPosition.after_stream_id(first.metadata["redis.stream_id"]),
    ) as watcher:
        assert await watcher.wait_for_count(lambda _: True, 2, timeout=5) == [b"two", b"three"]
        observed = await watcher.wait_for_event(lambda _: True, timeout=5)
        assert observed.raw.metadata["redis.stream_id"] == second.metadata["redis.stream_id"]
        assert [event.raw.metadata["redis.stream_id"] for event in watcher.snapshot().events] == [
            second.metadata["redis.stream_id"],
            third.metadata["redis.stream_id"],
        ]


@pytest.mark.parametrize("mutation", ["xdel", "xtrim"])
@pytest.mark.asyncio
async def test_new_deletion_or_trimming_fails_before_handing_off_a_following_entry(
    real_stream_source, mutation
):
    import redis.asyncio as redis

    harness, source = real_stream_source
    old = await harness.publish(source, b"historical-entry")
    client = redis.from_url(os.environ["REDIS_URL"], protocol=2, decode_responses=False)
    try:
        with pytest.raises(ObserverFailedError):
            async with EventObserver(transport=harness.adapter).watch(source=source) as watcher:
                async with client.pipeline(transaction=True) as pipe:
                    if mutation == "xdel":
                        pipe.xdel(source.name, old.metadata["redis.stream_id"])
                    else:
                        pipe.xtrim(source.name, maxlen=0, approximate=False)
                    pipe.xadd(source.name, {"payload": b"must-not-be-admitted"})
                    await pipe.execute()
                with pytest.raises(ObserverFailedError) as caught:
                    await watcher.wait_for(lambda _: True, timeout=5)
                assert isinstance(caught.value.cause, TransportInterruptedError)
                assert watcher.snapshot().next_sequence == 0
    finally:
        await client.aclose()


@pytest.mark.parametrize("replacement", ["missing", "wrong-type"])
@pytest.mark.asyncio
async def test_missing_or_non_stream_source_is_rejected_at_startup(real_stream_source, replacement):
    import redis.asyncio as redis

    harness, source = real_stream_source
    client = redis.from_url(os.environ["REDIS_URL"], protocol=2, decode_responses=False)
    try:
        await client.delete(source.name)
        if replacement == "wrong-type":
            await client.set(source.name, b"not-a-stream")
        with pytest.raises(TransportError):
            async with EventObserver(transport=harness.adapter).watch(source=source):
                pytest.fail("An unavailable source must not reach READY")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_real_harness_cleanup_removes_its_stream_without_deleting_an_application_key():
    import redis.asyncio as redis

    harness = RedisStreamsAdapterHarness(os.environ["REDIS_URL"])
    client = redis.from_url(os.environ["REDIS_URL"], protocol=2, decode_responses=False)
    application = "application-key-" + uuid4().hex
    try:
        source = await harness.create_source(case_id="cleanup")
        await client.set(application, b"keep")
        await harness.cleanup()
        assert await client.exists(source.name) == 0
        assert await client.get(application) == b"keep"
    finally:
        await harness.cleanup()
        await client.delete(application)
        await client.aclose()


@pytest.mark.asyncio
async def test_killing_only_the_observer_connection_signals_failure_without_a_business_message(
    real_stream_source,
):
    import redis.asyncio as redis

    harness, source = real_stream_source
    control = redis.from_url(os.environ["REDIS_URL"], protocol=2, decode_responses=False)
    try:
        with pytest.raises(ObserverFailedError):
            async with EventObserver(transport=harness.adapter).watch(source=source) as watcher:
                observer_client_id = watcher.readiness.source_metadata["redis.client_id"]
                assert isinstance(observer_client_id, int) and not isinstance(observer_client_id, bool)
                assert observer_client_id != await control.client_id()
                # Kill precisely this owned connection, never a broad address/type filter.
                assert await control.execute_command("CLIENT", "KILL", "ID", observer_client_id) == 1
                assert watcher._session is not None
                error = await asyncio.wait_for(watcher._session.wait_failure(), timeout=5)
                assert isinstance(error, TransportInterruptedError)
                await asyncio.sleep(0)
                assert watcher.snapshot().next_sequence == 0
                assert watcher.terminal_error is not None
    finally:
        await control.aclose()
