"""Blocking Redis Streams drivers preserve atomic boundaries without a loop."""

import asyncio
import copy
import threading
import time

import pytest

from event_observer.config import TransportBufferLimits
from event_observer.errors import (
    ConfigurationError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    StartPositionUnavailableError,
    TransportBackpressureError,
    TransportConnectionError,
    TransportInterruptedError,
    TransportSubscriptionError,
    UnsupportedCapabilityError,
)
from event_observer.models import OpenContext, StartPosition, TransportSource
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig
from event_observer.transports.sync_redis_streams import SyncRedisStreamsAdapter


class Native:
    def __init__(self):
        self.Redis = self
        self.lock = threading.RLock()
        self.clients = []
        self.entries = []
        self.last_id = "0-0"
        self.added = 0
        self.max_deleted = "0-0"
        self.client_id = 1
        self.run_id = "a" * 40
        self.executions = []
        self.calls = []
        self.error = None
        self.close_error = None
        self.close_gate = None
        self.closed_started = threading.Event()

    def from_url(self, url, **kwargs):
        self.calls.append(threading.get_ident())
        client = Client(self, kwargs)
        self.clients.append(client)
        return client

    def add(self, entry_id, payload=b"value", fields=None):
        with self.lock:
            self.entries.append((entry_id, fields if fields is not None else [b"payload", payload]))
            self.last_id = entry_id
            self.added += 1

    def delete(self, entry_id, *, trim=False):
        with self.lock:
            self.entries = [entry for entry in self.entries if entry[0] != entry_id]
            if not trim:
                self.max_deleted = entry_id

    def metadata(self):
        return {
            "length": len(self.entries),
            "entries-added": self.added,
            "last-generated-id": self.last_id.encode("ascii"),
            "max-deleted-entry-id": self.max_deleted.encode("ascii"),
            "first-entry": self.entries[0] if self.entries else None,
        }


class Client:
    def __init__(self, native, kwargs):
        self.native, self.kwargs = native, kwargs
        self.callbacks = {}
        self.close_calls = 0
        self.closed = False

    def set_response_callback(self, command, callback):
        self.native.calls.append(threading.get_ident())
        self.callbacks[command] = callback

    def pipeline(self, *, transaction):
        self.native.calls.append(threading.get_ident())
        assert transaction is True
        return Pipeline(self)

    def close(self):
        self.native.calls.append(threading.get_ident())
        self.close_calls += 1
        self.native.closed_started.set()
        if self.native.close_gate is not None:
            self.native.close_gate.wait()
        if self.native.close_error is not None:
            raise self.native.close_error
        self.closed = True


class Pipeline:
    def __init__(self, client):
        self.client = client
        self.commands = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def client_id(self):
        self.commands.append(("CLIENT ID",))

    def info(self, section):
        assert section == "server"
        self.commands.append(("INFO", section))

    def xinfo_stream(self, source):
        self.commands.append(("XINFO", source))

    def xread(self, streams, *, count):
        self.commands.append(("XREAD", streams, count))

    def execute(self):
        native = self.client.native
        native.calls.append(threading.get_ident())
        with native.lock:
            native.executions.append(self.commands)
            if native.error is not None:
                raise native.error
            response = []
            for command in self.commands:
                if command[0] == "CLIENT ID":
                    response.append(native.client_id)
                elif command[0] == "INFO":
                    response.append({"run_id": native.run_id})
                elif command[0] == "XINFO":
                    response.append(native.metadata())
                elif command[0] == "XREAD":
                    source, cursor = next(iter(command[1].items()))
                    assert cursor != "$"
                    pair = tuple(map(int, cursor.split("-")))
                    entries = [
                        [entry_id.encode("ascii"), fields]
                        for entry_id, fields in native.entries
                        if tuple(map(int, entry_id.split("-"))) > pair
                    ][: command[2]]
                    raw = [[source.encode("utf-8"), entries]] if entries else None
                    response.append(self.client.callbacks["XREAD"](copy.deepcopy(raw)))
                else:
                    raise AssertionError("Unexpected native command")
            return copy.deepcopy(response)


@pytest.fixture(autouse=True)
def prohibit_async_runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Blocking Redis must not create or access an asynchronous runtime")

    for name in ("run", "new_event_loop", "get_running_loop"):
        monkeypatch.setattr(asyncio, name, forbidden)


def transport(monkeypatch, native):
    monkeypatch.setattr("event_observer.transports.sync_redis_streams._load_native", lambda: native)
    monkeypatch.setattr("event_observer.transports.sync_redis_streams._no_retry", lambda: "zero-retry")
    return SyncRedisStreamsAdapter(RedisStreamsConfig("redis://user:SECRET@broker/0", poll_interval=0.001))


def open_session(transport, start=None, *, max_events=10, max_bytes=65536):
    ctx = OpenContext(
        "sync-redis-unit",
        time.monotonic() + 1,
        TransportBufferLimits(max_events=max_events, max_bytes=max_bytes),
    )
    return transport.open(TransportSource("events"), start or StartPosition.latest(), context=ctx)


def next_event(session):
    for _ in range(20):
        raw = session.poll(0.05)
        if raw is not None:
            return raw
    raise AssertionError("Expected a native stream entry within the bounded wait")


def await_failure(session):
    with session._condition:
        assert session._condition.wait_for(lambda: session._error is not None, timeout=1)
    return session.failure()


def test_provider_preserves_config_capabilities_and_report_title():
    original = RedisStreamsAdapter(RedisStreamsConfig("redis://broker/0"))
    blocking = original.sync_adapter()
    assert isinstance(blocking, SyncRedisStreamsAdapter)
    assert blocking.config is original.config
    assert blocking.capabilities == original.capabilities
    assert blocking.transport_name == original.transport_name == "RedisStreamsAdapter"


def test_atomic_readiness_native_options_and_owner_thread(monkeypatch):
    native = Native()
    session = open_session(transport(monkeypatch, native))
    try:
        assert session.readiness.source_metadata["redis.start_id"] == "0-0"
        assert session.readiness.source_metadata["redis.client_id"] == 1
        assert native.executions[0] == [("CLIENT ID",), ("INFO", "server"), ("XINFO", "events")]
        assert [command[0] for command in native.executions[1]] == ["CLIENT ID", "INFO", "XINFO", "XREAD"]
        options = native.clients[0].kwargs
        assert options["protocol"] == 2 and options["decode_responses"] is False
        assert options["max_connections"] == 1 and options["single_connection_client"] is False
        assert options["retry"] == "zero-retry" and options["retry_on_error"] == []
        assert options["health_check_interval"] == 0
        native.add("30-7", fields=[b"payload", b"\x00\xff", b"header:operation-id", b"\x00\xfe"])
        raw = next_event(session)
        assert raw.payload == b"\x00\xff" and raw.headers == (("operation-id", b"\x00\xfe"),)
        assert raw.key is None and raw.metadata["redis.stream_id"] == "30-7"
    finally:
        session.close(deadline=time.monotonic() + 1)
    session.close(deadline=time.monotonic() + 1)
    assert set(native.calls) == {session._thread.ident}
    assert session._thread.ident != threading.get_ident()
    assert native.clients[0].closed and native.clients[0].close_calls == 1
    assert session.poll(0) is None and session.failure() is None


def test_latest_resolves_deleted_tail_and_never_uses_dollar_cursor(monkeypatch):
    native = Native()
    native.add("10-0")
    native.add("30-0")
    native.delete("30-0")
    session = open_session(transport(monkeypatch, native))
    try:
        assert session.readiness.source_metadata["redis.start_id"] == "30-0"
        assert session.poll(0) is None
        native.add("50-0", b"new")
        assert next_event(session).payload == b"new"
    finally:
        session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("explicit", [False, True])
def test_earliest_and_exclusive_native_id_preserve_gaps(monkeypatch, explicit):
    native = Native()
    native.add("10-0", b"first")
    native.add("30-2", b"second")
    start = StartPosition.after_stream_id("10-0") if explicit else StartPosition.earliest()
    session = open_session(transport(monkeypatch, native), start)
    try:
        if not explicit:
            assert next_event(session).payload == b"first"
        raw = next_event(session)
        assert raw.payload == b"second" and raw.metadata["redis.stream_id"] == "30-2"
    finally:
        session.close(deadline=time.monotonic() + 1)


def test_future_cursor_waits_for_strictly_greater_id(monkeypatch):
    native = Native()
    session = open_session(transport(monkeypatch, native), StartPosition.after_stream_id("90-0"))
    try:
        native.add("80-0")
        assert session.poll(0.005) is None
        native.add("100-0", b"future")
        assert next_event(session).payload == b"future"
    finally:
        session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("trim", [False, True])
def test_new_deletion_precedes_candidate_handoff_even_when_buffer_full(monkeypatch, trim):
    native = Native()
    native.add("10-0")
    session = open_session(transport(monkeypatch, native), StartPosition.earliest(), max_events=1)
    try:
        with native.lock:
            native.delete("10-0", trim=trim)
            native.add("30-0", b"candidate")
        assert isinstance(await_failure(session), TransportInterruptedError)
        with pytest.raises(TransportInterruptedError):
            session.poll(0)
    finally:
        session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("restart", [False, True])
def test_connection_or_server_replacement_is_terminal_before_handoff(monkeypatch, restart):
    native = Native()
    session = open_session(transport(monkeypatch, native))
    try:
        with native.lock:
            if restart:
                native.run_id = "b" * 40
            else:
                native.client_id += 1
            native.add("20-0", b"candidate")
        assert isinstance(await_failure(session), TransportInterruptedError)
        with pytest.raises(TransportInterruptedError):
            session.poll(0)
    finally:
        session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("bytes_limit", [False, True])
def test_handoff_capacity_loss_is_explicit_and_independently_visible(monkeypatch, bytes_limit):
    native = Native()
    session = open_session(
        transport(monkeypatch, native), max_events=1, max_bytes=256 if bytes_limit else 65536
    )
    try:
        native.add("10-0", b"x" * 1000 if bytes_limit else b"first")
        if not bytes_limit:
            with session._condition:
                assert session._condition.wait_for(lambda: bool(session._buffer), timeout=1)
            native.add("20-0", b"second")
        assert isinstance(await_failure(session), TransportBackpressureError)
        with pytest.raises(TransportBackpressureError):
            session.poll(0)
    finally:
        session.close(deadline=time.monotonic() + 1)


def test_explicit_cursor_rejects_known_missing_history_and_closes_client(monkeypatch):
    native = Native()
    native.add("10-0")
    native.delete("10-0")
    with pytest.raises(StartPositionUnavailableError):
        open_session(transport(monkeypatch, native), StartPosition.after_stream_id("0-0"))
    assert native.clients[0].closed


def test_duplicate_raw_fields_are_rejected_before_handoff(monkeypatch):
    native = Native()
    native.add("10-0", fields=[b"payload", b"value", b"header:x", b"first", b"header:x", b"second"])
    with pytest.raises(TransportSubscriptionError):
        open_session(transport(monkeypatch, native), StartPosition.earliest())
    assert native.clients[0].closed


def test_native_failure_is_sanitized_not_retried_and_retains_cause(monkeypatch):
    native = Native()
    session = open_session(transport(monkeypatch, native))
    original = RuntimeError("SECRET native connection failure")
    try:
        native.error = original
        error = await_failure(session)
        assert isinstance(error, TransportInterruptedError)
        assert error.cause is original and error.__cause__ is original
        assert "SECRET" not in str(error)
        session._thread.join(timeout=1)
        count = len(native.executions)
        assert session.failure() is error
        with pytest.raises(TransportInterruptedError):
            session.poll(0)
        assert len(native.executions) == count
    finally:
        session.close(deadline=time.monotonic() + 1)


def test_startup_failure_preserves_native_cause_and_cleans_up(monkeypatch):
    native = Native()
    native.error = RuntimeError("SECRET native connection failure")
    with pytest.raises(TransportConnectionError) as caught:
        open_session(transport(monkeypatch, native))
    assert caught.value.cause is native.error and caught.value.__cause__ is native.error
    assert "SECRET" not in str(caught.value)
    assert native.clients[0].closed


def test_stalled_cleanup_is_bounded_then_joins_same_worker(monkeypatch):
    native = Native()
    session = open_session(transport(monkeypatch, native))
    native.close_gate = threading.Event()
    try:
        with pytest.raises(ObserverCleanupTimeoutError):
            session.close(deadline=time.monotonic() + 0.01)
        assert native.closed_started.is_set() and session._thread.is_alive()
    finally:
        native.close_gate.set()
        session.close(deadline=time.monotonic() + 1)
    assert native.clients[0].close_calls == 1


def test_cleanup_failure_retains_cause_and_chain(monkeypatch):
    native = Native()
    session = open_session(transport(monkeypatch, native))
    original = RuntimeError("SECRET close failure")
    native.close_error = original
    with pytest.raises(ObserverCleanupError) as caught:
        session.close(deadline=time.monotonic() + 1)
    assert caught.value.cause is original and caught.value.__cause__ is original
    assert "SECRET" not in str(caught.value)
    assert not session._thread.is_alive()


@pytest.mark.parametrize("timeout", [-1, True, None, float("inf"), float("nan"), 10**1000])
def test_poll_timeout_validation(monkeypatch, timeout):
    session = open_session(transport(monkeypatch, Native()))
    try:
        with pytest.raises(ConfigurationError):
            session.poll(timeout)
    finally:
        session.close(deadline=time.monotonic() + 1)


def test_unsupported_start_does_not_create_native_client(monkeypatch):
    native = Native()
    with pytest.raises(UnsupportedCapabilityError):
        open_session(transport(monkeypatch, native), StartPosition.at_offset({0: 0}))
    assert not native.clients
