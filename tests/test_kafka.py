"""Native-client contract tests; no broker or optional dependency required."""

import asyncio
import concurrent.futures
import threading
import time
from collections import deque
from types import SimpleNamespace

import pytest

from event_observer.config import TransportBufferLimits
from event_observer.errors import (
    ConfigurationError,
    ObserverCleanupError,
    StartPositionUnavailableError,
    TopologyChangedError,
    TransportAuthorizationError,
    TransportBackpressureError,
    TransportError,
    TransportReadinessError,
    TransportSubscriptionError,
)
from event_observer.models import EpochMilliseconds, OpenContext, StartPosition, TransportSource
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig


class NativeError:
    _PARTITION_EOF = -191
    TOPIC_AUTHORIZATION_FAILED = 29
    OFFSET_OUT_OF_RANGE = 1
    _AUTO_OFFSET_RESET = -140

    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class Partition:
    def __init__(self, topic, partition, offset=-1001):
        self.topic, self.partition, self.offset = topic, partition, offset
        self.error = None


class Message:
    def __init__(self, partition, offset, payload=b"value", error=None, headers=()):
        self._partition, self._offset, self._payload = partition, offset, payload
        self._error, self._headers = error, headers

    def error(self):
        return self._error

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._payload

    def key(self):
        return b"key"

    def headers(self):
        return self._headers

    def topic(self):
        return "events"

    def timestamp(self):
        return (1, 1234)


class Native:
    KafkaError = NativeError
    TopicPartition = Partition
    IsolationLevel = SimpleNamespace(READ_UNCOMMITTED="uncommitted")
    OffsetSpec = SimpleNamespace(latest=lambda: "latest")

    def __init__(self, partitions=(0,), messages=()):
        self.partitions = partitions
        self.messages = list(messages)
        self.clients = []
        self.after_probe_assignment = None
        self.suppress_fetch = False
        self.stats_fetch = False
        self.topic_error = None
        self.admin_topic_error = None
        self.timestamp_result = -1
        self.probe_error = None
        self.close_failure = False
        self.consumer_high_override = None
        self.admin_configs = []
        self.admin_threads = []

    def Consumer(self, config):
        client = Consumer(self, config)
        self.clients.append(client)
        return client

    def AdminClient(self, config):
        self.admin_configs.append(config)
        self.admin_threads.append(threading.get_ident())
        return Admin(self)

    def metadata(self, topic, error):
        partitions = {p: SimpleNamespace(error=None) for p in self.partitions}
        return SimpleNamespace(topics={topic: SimpleNamespace(error=error, partitions=partitions)})


class Admin:
    def __init__(self, native):
        self.native = native
        self.metadata_topics: set[str] = set()

    def list_topics(self, topic, timeout):
        assert timeout > 0
        self.native.admin_threads.append(threading.get_ident())
        self.metadata_topics.add(topic)
        return self.native.metadata(topic, self.native.admin_topic_error)

    def list_offsets(self, requests, *, isolation_level, request_timeout):
        assert isolation_level == self.native.IsolationLevel.READ_UNCOMMITTED
        assert request_timeout > 0
        assert {partition.topic for partition in requests} <= self.metadata_topics
        self.native.admin_threads.append(threading.get_ident())
        futures = {}
        for partition in requests:
            records = [
                message.offset()
                for message in self.native.messages
                if message.partition() == partition.partition
            ]
            future = concurrent.futures.Future()
            future.set_result(SimpleNamespace(offset=max(records) + 1 if records else 0))
            futures[partition] = future
        return futures


class Consumer:
    def __init__(self, native, config):
        self.native, self.config = native, config
        self.calls = [threading.get_ident()]
        self.assignments = []
        self.positions = {}
        self.eof = set()
        self.closed = False
        self.errors = deque()

    def touch(self):
        self.calls.append(threading.get_ident())

    def list_topics(self, topic, timeout):
        self.touch()
        return self.native.metadata(topic, self.native.topic_error)

    def get_watermark_offsets(self, partition, timeout, cached=False):
        self.touch()
        records = [m.offset() for m in self.native.messages if m.partition() == partition.partition]
        high = max(records) + 1 if records else 0
        if self.native.consumer_high_override is not None:
            high = self.native.consumer_high_override
        return (0, high)

    def offsets_for_times(self, partitions, timeout):
        self.touch()
        return [Partition(p.topic, p.partition, self.native.timestamp_result) for p in partitions]

    def assign(self, partitions):
        self.touch()
        self.assignments.append([(p.partition, p.offset) for p in partitions])
        self.positions = {p.partition: p.offset for p in partitions}
        self.eof.clear()
        if len(self.assignments) == 1 and self.native.after_probe_assignment:
            self.native.after_probe_assignment(self.native)

    def poll(self, timeout):
        self.touch()
        if len(self.assignments) == 1 and self.native.probe_error is not None:
            return Message(0, 0, error=self.native.probe_error)
        if self.errors:
            return Message(0, 0, error=self.errors.popleft())
        if self.native.stats_fetch:
            import json

            info = {
                str(p): {"fetch_state": "active", "hi_offset": 10, "ls_offset": 0} for p in self.positions
            }
            self.config["stats_cb"](json.dumps({"topics": {"events": {"partitions": info}}}))
        if self.native.suppress_fetch:
            time.sleep(min(timeout, 0.005))
            return None
        for message in self.native.messages:
            partition = message.partition()
            if partition in self.positions and message.offset() >= self.positions[partition]:
                self.positions[partition] = message.offset() + 1
                return message
        for partition in self.positions:
            if partition not in self.eof:
                self.eof.add(partition)
                return Message(
                    partition, self.positions[partition], error=NativeError(NativeError._PARTITION_EOF)
                )
        time.sleep(min(timeout, 0.005))
        return None

    def pause(self, assignment):
        self.touch()

    def resume(self, assignment):
        self.touch()

    def close(self):
        self.touch()
        self.closed = True
        if self.native.close_failure:
            raise RuntimeError("native cleanup with server-provided sensitive detail")


def context(timeout=1.0, max_bytes=1024 * 1024):
    return OpenContext(
        observer_id="unit-test",
        startup_deadline=time.monotonic() + timeout,
        buffers=TransportBufferLimits(max_events=4, max_bytes=max_bytes),
    )


def setup_native(monkeypatch, native):
    monkeypatch.setattr("event_observer.transports.kafka._load_native", lambda: native)
    monkeypatch.setattr("event_observer.transports.kafka._load_admin", lambda: native)


def run(coro):
    return asyncio.run(coro)


@pytest.mark.parametrize(
    "option", ["enable.auto.commit", "group.id", "auto.offset.reset", "ssl.endpoint.identification.algorithm"]
)
def test_observer_managed_options_cannot_be_overridden(option):
    with pytest.raises(ConfigurationError):
        KafkaConfig("localhost:9092", client_options={option: True})


@pytest.mark.parametrize(
    "kwargs",
    [
        {"bootstrap_servers": ""},
        {"bootstrap_servers": ()},
        {"bootstrap_servers": "kafka", "topology_check_interval": float("nan")},
        {"bootstrap_servers": "kafka", "security_protocol": "SSL", "ssl_key_location": "key"},
        {"bootstrap_servers": "kafka", "sasl_username": "name"},
        {"bootstrap_servers": "kafka", "security_protocol": "SASL_SSL"},
    ],
)
def test_configuration_validation(kwargs):
    with pytest.raises(ConfigurationError):
        KafkaConfig(**kwargs)


def test_secrets_are_excluded_from_repr_and_client_options_are_copied():
    options = {"client.id": "test"}
    config = KafkaConfig(
        "kafka",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="private-user",
        sasl_password="private-secret",
        client_options=options,
    )
    options["client.id"] = "changed"
    assert config.client_options["client.id"] == "test"
    assert "private-secret" not in repr(config) and "private-user" not in repr(config)


def test_latest_boundary_survives_publication_during_readiness(monkeypatch):
    native = Native(partitions=(0, 1), messages=[Message(0, 0, b"old")])
    native.after_probe_assignment = lambda n: n.messages.append(Message(1, 0, b"new"))
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.latest(), context=context()
        )
        assert dict(session.readiness.boundary.positions) == {0: 1, 1: 0}
        event = await asyncio.wait_for(anext(session.receive()), timeout=1)
        assert event.payload == b"new"
        await session.close(deadline=time.monotonic() + 1)
        client = native.clients[0]
        assert client.assignments[-1] == [(0, 1), (1, 0)]
        assert client.config["enable.auto.commit"] is False
        assert client.config["enable.auto.offset.store"] is False
        assert client.config["auto.offset.reset"] == "error"
        assert len(set(client.calls)) == 1
        assert client.calls[0] != threading.get_ident()
        assert client.closed

    run(scenario())


def test_replay_preserves_tombstones_headers_and_noncontiguous_offsets(monkeypatch):
    messages = [Message(0, 0, None, headers=(("type", b"one"), ("type", None))), Message(0, 4, b"")]
    native = Native(messages=messages)
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.earliest(), context=context()
        )
        stream = session.receive()
        one = await asyncio.wait_for(anext(stream), timeout=1)
        two = await asyncio.wait_for(anext(stream), timeout=1)
        assert one.payload is None and one.headers == (("type", b"one"), ("type", None))
        assert two.payload == b"" and two.metadata["kafka.offset"] == 4
        with pytest.raises(TransportSubscriptionError):
            session.receive()
        await session.close(deadline=time.monotonic() + 1)

    run(scenario())


@pytest.mark.parametrize("offsets", [{0: 2}, {0: 0, 1: 0}])
def test_invalid_explicit_offsets_fail_without_reset(monkeypatch, offsets):
    native = Native(messages=[Message(0, 0)])
    setup_native(monkeypatch, native)

    async def scenario():
        with pytest.raises(StartPositionUnavailableError):
            await KafkaAdapter(KafkaConfig("kafka")).open(
                TransportSource("events"), StartPosition.at_offset(offsets), context=context()
            )
        assert native.clients[0].closed

    run(scenario())


def test_offset_at_high_is_valid_and_eof_does_not_end_stream(monkeypatch):
    native = Native(messages=[Message(0, 0)])
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.at_offset({0: 1}), context=context()
        )
        stream = session.receive()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0.02)
        assert not pending.done()
        native.messages.append(Message(0, 1, b"later"))
        assert (await asyncio.wait_for(pending, 1)).payload == b"later"
        await session.close(deadline=time.monotonic() + 1)

    run(scenario())


@pytest.mark.parametrize("required", [True, False])
def test_timestamp_missing_fallback_is_explicit(monkeypatch, required):
    native = Native(messages=[Message(0, 0)])
    setup_native(monkeypatch, native)

    async def scenario():
        adapter = KafkaAdapter(KafkaConfig("kafka"))
        start = StartPosition.at_timestamp(EpochMilliseconds(9999), require_existing=required)
        if required:
            with pytest.raises(StartPositionUnavailableError):
                await adapter.open(TransportSource("events"), start, context=context())
        else:
            session = await adapter.open(TransportSource("events"), start, context=context())
            assert dict(session.readiness.boundary.positions) == {0: 1}
            await session.close(deadline=time.monotonic() + 1)

    run(scenario())


def test_no_fetch_confirmation_means_no_optimistic_readiness(monkeypatch):
    native = Native()
    native.suppress_fetch = True
    setup_native(monkeypatch, native)

    async def scenario():
        with pytest.raises((TransportReadinessError, asyncio.TimeoutError)):
            await KafkaAdapter(KafkaConfig("kafka")).open(
                TransportSource("events"), StartPosition.latest(), context=context(timeout=0.06)
            )
        assert native.clients[0].closed

    run(scenario())


def test_successful_fetch_statistics_allow_open_transaction_without_records(monkeypatch):
    native = Native()
    native.suppress_fetch = True
    native.stats_fetch = True
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.latest(), context=context()
        )
        assert session.readiness.boundary.positions == {0: 0}
        await session.close(deadline=time.monotonic() + 1)

    run(scenario())


@pytest.mark.parametrize("error_attribute", ["topic_error", "admin_topic_error"])
def test_metadata_authorization_failure_is_typed_and_startup_rolls_back(monkeypatch, error_attribute):
    native = Native()
    setattr(native, error_attribute, NativeError(NativeError.TOPIC_AUTHORIZATION_FAILED))
    setup_native(monkeypatch, native)

    async def scenario():
        with pytest.raises(TransportAuthorizationError):
            await KafkaAdapter(KafkaConfig("kafka")).open(
                TransportSource("events"), StartPosition.latest(), context=context()
            )
        assert native.clients[0].assignments == []
        assert native.clients[0].closed

    run(scenario())


def test_full_handoff_does_not_block_topology_failure_or_close(monkeypatch):
    native = Native(messages=[Message(0, 0), Message(0, 1)])
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka", topology_check_interval=0.01)).open(
            TransportSource("events"), StartPosition.earliest(), context=context()
        )
        await asyncio.sleep(0.02)
        assert len(session._queue) == 1
        native.partitions = (0, 1)
        assert isinstance(await asyncio.wait_for(session.wait_failure(), timeout=1), TopologyChangedError)
        await asyncio.gather(
            session.close(deadline=time.monotonic() + 1), session.close(deadline=time.monotonic() + 1)
        )
        assert native.clients[0].closed

    run(scenario())


def test_oversize_handoff_reports_failure_without_drop(monkeypatch):
    native = Native()
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.latest(), context=context(max_bytes=1024)
        )
        native.messages.append(Message(0, 0, b"x" * 2048))
        assert isinstance(
            await asyncio.wait_for(session.wait_failure(), timeout=1), TransportBackpressureError
        )
        await session.close(deadline=time.monotonic() + 1)

    run(scenario())


def test_adapter_sessions_are_independent_and_cancelling_monitor_is_safe(monkeypatch):
    native = Native()
    setup_native(monkeypatch, native)

    async def scenario():
        adapter = KafkaAdapter(KafkaConfig("kafka"))
        one = await adapter.open(TransportSource("events"), StartPosition.latest(), context=context())
        two = await adapter.open(TransportSource("events"), StartPosition.latest(), context=context())
        monitor = asyncio.create_task(one.wait_failure())
        await asyncio.sleep(0)
        monitor.cancel()
        with pytest.raises(asyncio.CancelledError):
            await monitor
        assert not one._failure.done()
        await one.close(deadline=time.monotonic() + 1)
        assert await one.wait_failure() is None
        native.messages.append(Message(0, 0, b"independent"))
        assert (await asyncio.wait_for(anext(two.receive()), timeout=1)).payload == b"independent"
        await two.close(deadline=time.monotonic() + 1)
        assert native.clients[0].config["group.id"] != native.clients[1].config["group.id"]

    run(scenario())


def test_read_authorization_failure_is_not_hidden_by_metadata_success(monkeypatch):
    native = Native()
    native.probe_error = NativeError(NativeError.TOPIC_AUTHORIZATION_FAILED)
    setup_native(monkeypatch, native)

    async def scenario():
        with pytest.raises(TransportAuthorizationError):
            await KafkaAdapter(KafkaConfig("kafka")).open(
                TransportSource("events"), StartPosition.latest(), context=context()
            )
        assert native.clients[0].closed

    run(scenario())


def test_close_failure_is_explicit_and_does_not_expose_native_details(monkeypatch):
    native = Native()
    native.close_failure = True
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.latest(), context=context()
        )
        with pytest.raises(ObserverCleanupError) as raised:
            await session.close(deadline=time.monotonic() + 1)
        assert "sensitive" not in str(raised.value)
        assert not session._thread.is_alive()

    run(scenario())


def test_startup_failure_preserves_primary_cause_when_rollback_close_also_fails(monkeypatch):
    native = Native()
    native.probe_error = NativeError(NativeError.TOPIC_AUTHORIZATION_FAILED)
    native.close_failure = True
    setup_native(monkeypatch, native)

    async def scenario():
        with pytest.raises(TransportAuthorizationError) as raised:
            await KafkaAdapter(KafkaConfig("kafka")).open(
                TransportSource("events"), StartPosition.latest(), context=context()
            )
        assert isinstance(raised.value.cleanup_error, ObserverCleanupError)
        assert native.clients[0].closed

    run(scenario())


def test_startup_cancellation_rolls_back_native_worker(monkeypatch):
    native = Native()
    native.suppress_fetch = True
    setup_native(monkeypatch, native)

    async def scenario():
        opening = asyncio.create_task(
            KafkaAdapter(KafkaConfig("kafka")).open(
                TransportSource("events"), StartPosition.latest(), context=context()
            )
        )
        for _ in range(100):
            if native.clients:
                break
            await asyncio.sleep(0.001)
        assert native.clients
        opening.cancel()
        with pytest.raises(asyncio.CancelledError):
            await opening
        assert native.clients[0].closed
        assert len(set(native.clients[0].calls)) == 1

    run(scenario())


def test_burst_cannot_accumulate_unbounded_handoff_or_block_close(monkeypatch):
    native = Native(messages=[Message(0, offset) for offset in range(500)])
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.earliest(), context=context()
        )
        await asyncio.sleep(0.02)
        assert len(session._queue) == 1
        began = time.monotonic()
        await session.close(deadline=time.monotonic() + 1)
        assert time.monotonic() - began < 0.5
        assert not session._thread.is_alive()

    run(scenario())


def test_sync_core_continues_reading_kafka_during_blocking_application_action(monkeypatch):
    from event_observer import SyncEventObserver

    native = Native()
    setup_native(monkeypatch, native)
    with SyncEventObserver(transport=KafkaAdapter(KafkaConfig("kafka"))).watch(source="events") as watcher:
        native.messages.append(Message(0, 0, b"during-action"))
        time.sleep(0.02)
        assert watcher.wait_for(lambda value: value == b"during-action", timeout=1) == b"during-action"
    assert native.clients[0].closed


def test_async_core_waiters_share_kafka_history_without_competing(monkeypatch):
    from event_observer import EventObserver

    native = Native()
    setup_native(monkeypatch, native)

    async def scenario():
        async with EventObserver(transport=KafkaAdapter(KafkaConfig("kafka"))).watch(
            source="events"
        ) as watcher:
            one = asyncio.create_task(watcher.wait_for(lambda value: value == b"shared", timeout=1))
            two = asyncio.create_task(watcher.wait_for_event(lambda value: value == b"shared", timeout=1))
            native.messages.append(Message(0, 0, b"shared"))
            payload, event = await asyncio.gather(one, two)
            assert payload == event.payload == b"shared"
            assert event.raw.metadata["kafka.offset"] == 0
        assert native.clients[0].closed

    run(scenario())


def test_native_session_uses_the_injected_clock_domain(monkeypatch):
    class OffsetClock:
        def monotonic(self):
            return time.monotonic() + 10000

    native = Native()
    setup_native(monkeypatch, native)
    clock = OffsetClock()

    async def scenario():
        opened = OpenContext(
            observer_id="clock-test",
            startup_deadline=clock.monotonic() + 1,
            buffers=TransportBufferLimits(),
            clock=clock,
        )
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.latest(), context=opened
        )
        assert session.readiness.ready_monotonic > time.monotonic() + 9990
        assert session.readiness.boundary.captured_monotonic > time.monotonic() + 9990
        await session.close(deadline=clock.monotonic() + 1)
        assert not session._thread.is_alive()

    run(scenario())


def test_read_committed_boundary_uses_physical_high_instead_of_consumer_lso(monkeypatch):
    native = Native(messages=[Message(0, 0, b"old-open-transaction")])
    native.consumer_high_override = 0
    setup_native(monkeypatch, native)

    async def scenario():
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.latest(), context=context()
        )
        assert session.readiness.boundary.positions == {0: 1}
        assert native.clients[0].config["isolation.level"] == "read_committed"
        assert "group.id" not in native.admin_configs[0]
        assert "enable.auto.commit" not in native.admin_configs[0]
        assert "isolation.level" not in native.admin_configs[0]
        assert native.admin_configs[0]["allow.auto.create.topics"] is False
        assert set(native.admin_threads) == set(native.clients[0].calls)
        native.messages.append(Message(0, 1, b"new-after-boundary"))
        assert (await asyncio.wait_for(anext(session.receive()), 1)).payload == b"new-after-boundary"
        await session.close(deadline=time.monotonic() + 1)

    run(scenario())


@pytest.mark.parametrize("phase", ["startup", "background"])
@pytest.mark.parametrize("typed", [False, True])
def test_worker_native_exception_retains_cause_and_chain(monkeypatch, phase, typed):
    """Preserve worker failures for debugging without copying native details into summaries.

    :param monkeypatch: Fixture replacing native clients with controlled doubles.
    :param phase: Whether the failure occurs before or after READY.
    :param typed: Whether the native error supplies a recognizable authorization code.
    :returns: ``None`` after both cause references and the safe summary are checked.
    """
    native = Native()
    detail = "sensitive-native-detail"
    cause = (
        RuntimeError(NativeError(NativeError.TOPIC_AUTHORIZATION_FAILED), detail)
        if typed
        else RuntimeError(detail)
    )
    setup_native(monkeypatch, native)

    def fail_native(*args, **kwargs):
        """Raise the exact native failure from the consumer constructor or poll call.

        :param args: Unused native call positional arguments.
        :param kwargs: Unused native call keyword arguments.
        :raises RuntimeError: The original failure retained by the normalized error.
        """
        raise cause

    async def scenario():
        """Exercise the worker boundary and join every worker before returning.

        :returns: ``None`` after the failure and cleanup have completed.
        """
        adapter = KafkaAdapter(KafkaConfig("kafka"))
        if phase == "startup":
            monkeypatch.setattr(native, "Consumer", fail_native)
            with pytest.raises(TransportError) as raised:
                await adapter.open(TransportSource("events"), StartPosition.latest(), context=context())
            error = raised.value
        else:
            session = await adapter.open(TransportSource("events"), StartPosition.latest(), context=context())
            try:
                monkeypatch.setattr(native.clients[0], "poll", fail_native)
                error = await asyncio.wait_for(session.wait_failure(), timeout=1)
            finally:
                await session.close(deadline=time.monotonic() + 1)
            assert not session._thread.is_alive()
        assert error is not None
        assert error.cause is cause
        assert error.__cause__ is cause
        assert detail not in str(error)
        if typed:
            assert isinstance(error, TransportAuthorizationError)

    run(scenario())


def test_native_cleanup_exception_retains_cause_and_chain(monkeypatch):
    """Keep cleanup's native cause while exposing only the stable cleanup summary.

    :param monkeypatch: Fixture installing the controlled native client.
    :returns: ``None`` after the original cleanup exception and worker exit are checked.
    """
    native = Native()
    setup_native(monkeypatch, native)
    cause = RuntimeError("sensitive-cleanup-detail")

    def fail_close():
        """Raise a native cleanup failure with a detail unsuitable for safe reports.

        :raises RuntimeError: The original failure that cleanup must preserve.
        """
        raise cause

    async def scenario():
        """Close the owned session and inspect its normalized cleanup failure.

        :returns: ``None`` once the worker is confirmed terminated.
        """
        session = await KafkaAdapter(KafkaConfig("kafka")).open(
            TransportSource("events"), StartPosition.latest(), context=context()
        )
        monkeypatch.setattr(native.clients[0], "close", fail_close)
        with pytest.raises(ObserverCleanupError) as raised:
            await session.close(deadline=time.monotonic() + 1)
        assert raised.value.cause is cause
        assert raised.value.__cause__ is cause
        assert "sensitive-cleanup-detail" not in str(raised.value)
        assert not session._thread.is_alive()

    run(scenario())
