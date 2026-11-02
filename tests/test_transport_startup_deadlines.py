"""Final native readiness must satisfy the injected absolute startup deadline."""

import asyncio
import threading
import time
from datetime import datetime, timezone

import pytest

from event_observer.config import TransportBufferLimits
from event_observer.models import OpenContext, StartPosition, TransportSource
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig

from tests.test_kafka import Native as KafkaNative
from tests.test_redis_streams import Native as AsyncRedisNative, Pipeline as AsyncRedisPipeline
from tests.test_sync_redis_streams import Native as SyncRedisNative, Pipeline as SyncRedisPipeline


class AdvancingClock:
    def __init__(self):
        self._lock = threading.Lock()
        self.offset = 0
        self.advanced = threading.Event()

    def monotonic(self):
        with self._lock:
            return time.monotonic() + self.offset

    def utcnow(self):
        return datetime.now(timezone.utc)

    def expire(self):
        with self._lock:
            self.offset = 10
            self.advanced.set()


def startup_context(clock):
    return OpenContext(
        "final-native-read",
        clock.monotonic() + 0.2,
        TransportBufferLimits(),
        clock=clock,
    )


def kafka_transport(monkeypatch, clock, *, asynchronous):
    native = KafkaNative()
    create_consumer = native.Consumer
    release = threading.Event()
    if not asynchronous:
        release.set()

    def consumer(config):
        client = create_consumer(config)
        native_poll = client.poll

        def poll(timeout):
            message = native_poll(timeout)
            if message is not None and len(client.assignments) == 1:
                assert release.wait(1), "The controlled final Fetch barrier was not released"
                clock.expire()
            return message

        client.poll = poll
        return client

    monkeypatch.setattr(native, "Consumer", consumer)
    monkeypatch.setattr("event_observer.transports.kafka._load_native", lambda: native)
    monkeypatch.setattr("event_observer.transports.kafka._load_admin", lambda: native)
    return KafkaAdapter(KafkaConfig("controlled")), native, release


def redis_transport(monkeypatch, clock, *, asynchronous):
    native = AsyncRedisNative() if asynchronous else SyncRedisNative()
    if asynchronous:
        execute = AsyncRedisPipeline.execute

        async def async_execute(self):
            result = await execute(self)
            if len(self.commands) == 4:
                clock.expire()
            return result

        monkeypatch.setattr(AsyncRedisPipeline, "execute", async_execute)
        module = "event_observer.transports.redis_streams"
    else:
        execute = SyncRedisPipeline.execute

        def sync_execute(self):
            result = execute(self)
            if len(self.commands) == 4:
                clock.expire()
            return result

        monkeypatch.setattr(SyncRedisPipeline, "execute", sync_execute)
        module = "event_observer.transports.sync_redis_streams"
    monkeypatch.setattr(f"{module}._load_native", lambda: native)
    monkeypatch.setattr(f"{module}._no_retry", lambda: "zero-retry")
    return RedisStreamsAdapter(RedisStreamsConfig("redis://controlled")), native, None


def transport(monkeypatch, backend, clock, *, asynchronous):
    factory = kafka_transport if backend == "kafka" else redis_transport
    return factory(monkeypatch, clock, asynchronous=asynchronous)


def assert_clean_native_resources(native, threads_before):
    assert len(native.clients) == 1
    assert native.clients[0].closed
    assert not [
        worker
        for worker in threading.enumerate()
        if worker not in threads_before and worker.name.startswith("event-observer-")
    ]


@pytest.mark.parametrize("backend", ["kafka", "redis"])
def test_blocking_final_native_read_cannot_return_ready_after_absolute_deadline(monkeypatch, backend):
    clock = AdvancingClock()
    adapter, native, _ = transport(monkeypatch, backend, clock, asynchronous=False)
    context = startup_context(clock)
    threads_before = set(threading.enumerate())
    with pytest.raises(TimeoutError, match="startup deadline expired"):
        adapter.sync_adapter().open(TransportSource("events"), StartPosition.latest(), context=context)
    assert clock.advanced.is_set()
    assert clock.monotonic() > context.startup_deadline
    assert_clean_native_resources(native, threads_before)


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["kafka", "redis"])
async def test_async_final_native_read_cannot_return_ready_after_absolute_deadline(monkeypatch, backend):
    clock = AdvancingClock()
    adapter, native, release = transport(monkeypatch, backend, clock, asynchronous=True)
    context = startup_context(clock)
    threads_before = set(threading.enumerate())
    tasks_before = asyncio.all_tasks()
    if release is not None:
        # The final Fetch can advance time only after open has installed its
        # relative owner-loop wait, exercising the absolute completion check.
        asyncio.get_running_loop().call_soon(release.set)
    with pytest.raises(TimeoutError, match="startup deadline expired"):
        await adapter.open(TransportSource("events"), StartPosition.latest(), context=context)
    assert clock.advanced.is_set()
    assert clock.monotonic() > context.startup_deadline
    assert_clean_native_resources(native, threads_before)
    assert not [
        task for task in asyncio.all_tasks() - tasks_before if task.get_name().startswith("event-observer-")
    ]
