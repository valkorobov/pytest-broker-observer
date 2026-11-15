"""Controlled Redis client tests without a service or optional Redis import."""

import asyncio
from contextlib import aclosing
import copy
import subprocess
import sys
import time

import pytest

from event_observer.config import TransportBufferLimits
from event_observer.errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    StartPositionUnavailableError,
    TransportAuthenticationError,
    TransportAuthorizationError,
    TransportBackpressureError,
    TransportConnectionError,
    TransportInterruptedError,
    TransportSubscriptionError,
    UnsupportedCapabilityError,
)
from event_observer.models import EpochMilliseconds, OpenContext, StartPosition, TransportSource
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig


class Pipeline:
    def __init__(self, client):
        self.client = client
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    def client_id(self):
        self.commands.append(("CLIENT ID",))
        return self

    def info(self, section):
        assert section == "server"
        self.commands.append(("INFO", section))
        return self

    def xinfo_stream(self, source):
        self.commands.append(("XINFO", source))
        return self

    def xread(self, streams, *, count):
        self.commands.append(("XREAD", streams, count))
        return self

    async def execute(self):
        native = self.client.native
        native.executions.append(self.commands)
        native.started.set()
        if native.execute_gate is not None:
            await native.execute_gate.wait()
        if native.error is not None:
            raise native.error
        response = []
        # No awaits occur while constructing this transaction's reply: source
        # metadata and entries refer to one stable native snapshot.
        for command in self.commands:
            if command[0] == "CLIENT ID":
                response.append(native.client_id)
            elif command[0] == "INFO":
                response.append({"run_id": native.run_id})
            elif command[0] == "XINFO":
                response.append(native.metadata())
            elif command[0] == "XREAD":
                streams, count = command[1:]
                source, cursor = next(iter(streams.items()))
                assert cursor != "$"
                position = tuple(map(int, cursor.split("-")))
                entries = [
                    [entry_id.encode("ascii"), fields]
                    for entry_id, fields in native.entries
                    if tuple(map(int, entry_id.split("-"))) > position
                ][:count]
                raw = [[source.encode("utf-8"), entries]] if entries else None
                response.append(self.client.callbacks["XREAD"](copy.deepcopy(raw)))
            else:
                raise AssertionError("Unexpected native command")
        native.polled.set()
        return response


class Client:
    def __init__(self, native, kwargs):
        self.native = native
        self.kwargs = kwargs
        self.callbacks = {}
        self.closed = False
        self.close_calls = 0

    def set_response_callback(self, command, callback):
        self.callbacks[command] = callback

    def pipeline(self, *, transaction):
        assert transaction is True
        return Pipeline(self)

    async def aclose(self, *, close_connection_pool):
        assert close_connection_pool is True
        self.close_calls += 1
        if self.native.close_gate is not None:
            await self.native.close_gate.wait()
        if self.native.close_error is not None:
            raise self.native.close_error
        self.closed = True


class Native:
    def __init__(self):
        self.Redis = self
        self.clients = []
        self.entries = []
        self.last_id = "0-0"
        self.added = 0
        self.max_deleted = "0-0"
        self.client_id = 1
        self.run_id = "a" * 40
        self.executions = []
        self.started = asyncio.Event()
        self.polled = asyncio.Event()
        self.execute_gate = None
        self.close_gate = None
        self.close_error = None
        self.error = None

    def from_url(self, url, **kwargs):
        client = Client(self, kwargs)
        self.clients.append(client)
        return client

    def add(self, entry_id, payload=b"value", *, fields=None):
        self.entries.append((entry_id, fields if fields is not None else [b"payload", payload]))
        self.last_id = entry_id
        self.added += 1

    def delete(self, entry_id, *, trim=False):
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

    async def next_poll(self):
        self.polled.clear()
        await asyncio.wait_for(self.polled.wait(), 1)


def context(*, max_events=10, max_bytes=65536, startup_timeout=2):
    return OpenContext(
        "redis-unit",
        time.monotonic() + startup_timeout,
        TransportBufferLimits(max_events=max_events, max_bytes=max_bytes),
    )


def adapter(monkeypatch, native, **kwargs):
    monkeypatch.setattr("event_observer.transports.redis_streams._load_native", lambda: native)
    monkeypatch.setattr("event_observer.transports.redis_streams._no_retry", lambda: "zero-retry")
    return RedisStreamsAdapter(
        RedisStreamsConfig("redis://user:SECRET@broker/0", poll_interval=0.001, **kwargs)
    )


async def open_session(transport, *, start=None, ctx=None):
    return await transport.open(
        TransportSource("events"), start or StartPosition.latest(), context=ctx or context()
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"url": ""},
        {"url": "https://broker/0"},
        {"url": "redis://broker:not-a-port"},
        {"url": "redis://broker/0?decode_responses=true"},
        {"url": "redis://broker/0?protocol=3"},
        {"url": "redis://broker/0?retry_on_timeout=false"},
        {"url": "redis://broker/0?max_connections=10"},
        {"url": "redis://broker/0?socket_timeout=0"},
        {"payload_field": ""},
        {"header_prefix": b"header:"},
        {"read_count": True},
        {"read_count": 0},
        {"poll_interval": float("nan")},
        {"poll_interval": 0},
        {"socket_timeout": float("inf")},
        {"socket_timeout": False},
    ],
)
def test_config_rejects_unsafe_or_invalid_values_without_disclosure(kwargs):
    with pytest.raises(ConfigurationError) as error:
        RedisStreamsConfig(**{"url": "redis://user:SECRET@broker/0", **kwargs})
    assert "SECRET" not in str(error.value)


def test_config_allows_explicit_tls_options_and_hides_url():
    config = RedisStreamsConfig("rediss://user:SECRET@broker/0?ssl_ca_certs=ca.pem")
    assert config.read_count == 100 and config.socket_timeout == 1 and config.poll_interval == 0.05
    assert "SECRET" not in repr(config)


def test_module_and_factory_do_not_import_optional_client():
    script = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'redis' or fullname.startswith('redis.'):
            raise AssertionError('Optional Redis import during factory construction')
sys.meta_path.insert(0, Block())
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig
RedisStreamsAdapter(RedisStreamsConfig('redis://localhost/0'))
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)


@pytest.mark.asyncio
async def test_missing_client_has_actionable_optional_dependency_error(monkeypatch):
    def missing():
        raise ImportError("not installed")

    monkeypatch.setattr("event_observer.transports.redis_streams._load_native", missing)
    with pytest.raises(MissingOptionalDependencyError, match="redis"):
        await open_session(RedisStreamsAdapter(RedisStreamsConfig("redis://broker/0")))


@pytest.mark.parametrize(
    "start,source",
    [
        (StartPosition.at_offset({0: 0}), TransportSource("events")),
        (StartPosition.at_timestamp(EpochMilliseconds(0)), TransportSource("events")),
        (StartPosition.latest(), TransportSource("events", partitions=(0,))),
    ],
)
@pytest.mark.asyncio
async def test_unsupported_positions_and_partitions_do_not_create_client(monkeypatch, start, source):
    native = Native()
    with pytest.raises(UnsupportedCapabilityError):
        await adapter(monkeypatch, native).open(source, start, context=context())
    assert not native.clients


@pytest.mark.asyncio
async def test_readiness_uses_atomic_read_path_and_managed_binary_options(monkeypatch):
    native = Native()
    session = await open_session(adapter(monkeypatch, native, read_count=1000), ctx=context(max_events=3))
    options = native.clients[0].kwargs
    assert options["decode_responses"] is False and options["protocol"] == 2
    assert options["retry"] == "zero-retry" and options["retry_on_error"] == []
    assert options["single_connection_client"] is True and options["max_connections"] == 1
    assert [command[0] for command in native.executions[0]] == ["CLIENT ID", "INFO", "XINFO"]
    assert [command[0] for command in native.executions[1]] == ["CLIENT ID", "INFO", "XINFO", "XREAD"]
    assert native.executions[1][-1] == ("XREAD", {"events": "0-0"}, 3)
    assert session.readiness.boundary.positions == {}
    assert session.readiness.source_metadata == {"redis.start_id": "0-0", "redis.client_id": 1}
    caps = session.readiness.resolved_capabilities
    assert caps.start_modes == {"latest", "earliest", "stream_id"}
    assert caps.supports_headers and not caps.supports_key and not caps.supports_settlement
    assert not caps.supports_null_payload and not caps.supports_null_header_values
    assert not caps.supports_duplicate_headers and not caps.supports_topology_detection
    assert not hasattr(session, "settle")
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_latest_resolves_last_generated_id_even_when_last_entry_was_deleted(monkeypatch):
    native = Native()
    native.add("1-0", b"retained")
    native.add("2-0", b"deleted")
    native.delete("2-0")
    session = await open_session(adapter(monkeypatch, native))
    assert session.readiness.source_metadata["redis.start_id"] == "2-0"
    assert not session._buffer
    native.add("3-0", b"fresh")
    raw = await asyncio.wait_for(anext(session.receive()), 1)
    assert raw.payload == b"fresh" and raw.metadata == {"redis.stream_id": "3-0"}
    assert all(
        command[1]["events"] != "$"
        for batch in native.executions
        for command in batch
        if command[0] == "XREAD"
    )
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_empty_existing_stream_is_ready_without_synthetic_writes(monkeypatch):
    native = Native()
    native.add("1-0")
    native.delete("1-0")
    session = await open_session(adapter(monkeypatch, native))
    assert session.readiness.source_metadata["redis.start_id"] == "1-0"
    assert {command[0] for batch in native.executions for command in batch} == {
        "CLIENT ID",
        "INFO",
        "XINFO",
        "XREAD",
    }
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.asyncio
async def test_available_history_and_explicit_exclusive_cursor(monkeypatch, explicit):
    native = Native()
    native.add("1-0", b"one")
    native.add("1-1", b"two")
    native.add("2-0", b"three")
    start = StartPosition.after_stream_id("1-0") if explicit else StartPosition.earliest()
    session = await open_session(adapter(monkeypatch, native), start=start)
    expected = [b"two", b"three"] if explicit else [b"one", b"two", b"three"]
    async with aclosing(session.receive()) as stream:
        assert [(await anext(stream)).payload for _ in expected] == expected
        with pytest.raises(TransportSubscriptionError):
            session.receive()
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_future_cursor_waits_for_strictly_greater_id(monkeypatch):
    native = Native()
    native.add("1-0")
    session = await open_session(adapter(monkeypatch, native), start=StartPosition.after_stream_id("10-0"))
    native.add("3-0", b"before")
    await native.next_poll()
    assert not session._buffer
    native.add("10-1", b"after")
    assert (await asyncio.wait_for(anext(session.receive()), 1)).payload == b"after"
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("trim,empty", [(False, False), (True, False), (True, True)])
@pytest.mark.asyncio
async def test_explicit_cursor_cannot_claim_known_missing_history(monkeypatch, trim, empty):
    native = Native()
    native.add("1-0")
    native.add("2-0")
    native.delete("1-0", trim=trim)
    if empty:
        native.delete("2-0", trim=trim)
    with pytest.raises(StartPositionUnavailableError):
        await open_session(adapter(monkeypatch, native), start=StartPosition.after_stream_id("0-0"))
    assert native.clients[0].closed


@pytest.mark.asyncio
async def test_earliest_intentionally_replays_available_baseline_history(monkeypatch):
    native = Native()
    native.add("1-0", b"deleted")
    native.add("2-0", b"retained")
    native.delete("1-0", trim=True)
    session = await open_session(adapter(monkeypatch, native), start=StartPosition.earliest())
    assert (await anext(session.receive())).payload == b"retained"
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_payload_field_headers_and_unrelated_fields_preserve_binary_data(monkeypatch):
    native = Native()
    native.add("1-0", fields=[b"body", b"", b"h:type", b"\xff", b"ignored", b"secret"])
    session = await open_session(
        adapter(monkeypatch, native, payload_field="body", header_prefix="h:"), start=StartPosition.earliest()
    )
    raw = await anext(session.receive())
    assert raw.payload == b"" and raw.headers == (("type", b"\xff"),)
    assert raw.key is None and raw.delivery_id is None and raw.source == "events"
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize(
    "fields",
    [
        [b"other", b"SECRET"],
        [b"payload", None],
        [b"payload", "SECRET"],
        [b"payload", b"one", b"payload", b"two"],
        [b"payload", b"one", b"header:type", b"one", b"header:type", b"two"],
        [b"payload", b"one", b"header:type", None],
        [b"payload", b"one", b"header:\xff", b"SECRET"],
        [b"payload"],
    ],
)
@pytest.mark.asyncio
async def test_invalid_or_duplicate_fields_fail_before_handoff(monkeypatch, fields):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    native.add("1-0", fields=fields)
    error = await asyncio.wait_for(session.wait_failure(), 1)
    assert isinstance(error, TransportSubscriptionError)
    assert "SECRET" not in str(error) and not session._buffer
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("trim", [False, True])
@pytest.mark.asyncio
async def test_new_deletion_is_terminal_before_candidate_handoff_even_when_consumed(monkeypatch, trim):
    native = Native()
    native.add("1-0", b"old")
    session = await open_session(adapter(monkeypatch, native), start=StartPosition.earliest())
    assert (await anext(session.receive())).payload == b"old"
    native.delete("1-0", trim=trim)
    native.add("2-0", b"candidate")
    error = await asyncio.wait_for(session.wait_failure(), 1)
    assert isinstance(error, TransportInterruptedError)
    assert not session._buffer
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("rewind", ["added", "last_id", "max_deleted", "first"])
@pytest.mark.asyncio
async def test_metadata_rewind_is_terminal(monkeypatch, rewind):
    native = Native()
    native.add("2-0")
    native.added = 10
    native.max_deleted = "1-0"
    session = await open_session(adapter(monkeypatch, native))
    if rewind == "added":
        native.added = 9
    elif rewind == "last_id":
        native.last_id = "1-1"
    elif rewind == "max_deleted":
        native.max_deleted = "0-0"
    else:
        native.entries[0] = ("0-1", [b"payload", b"old"])
    assert isinstance(await asyncio.wait_for(session.wait_failure(), 1), TransportInterruptedError)
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.asyncio
async def test_recycled_socket_or_restarted_server_never_hands_off_candidate(monkeypatch, restart):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    if restart:
        native.run_id = "b" * 40
    else:
        native.client_id += 1
    native.add("1-0", b"candidate")
    assert isinstance(await asyncio.wait_for(session.wait_failure(), 1), TransportInterruptedError)
    assert not session._buffer
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.asyncio
async def test_full_handoff_still_checks_continuity_without_active_receiver(monkeypatch):
    native = Native()
    native.add("1-0")
    session = await open_session(
        adapter(monkeypatch, native), start=StartPosition.earliest(), ctx=context(max_events=1)
    )
    assert len(session._buffer) == 1
    native.delete("1-0")
    assert isinstance(await asyncio.wait_for(session.wait_failure(), 1), TransportInterruptedError)
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("bytes_limit", [False, True])
@pytest.mark.asyncio
async def test_handoff_capacity_failure_is_bounded_and_independently_visible(monkeypatch, bytes_limit):
    native = Native()
    session = await open_session(
        adapter(monkeypatch, native), ctx=context(max_events=1, max_bytes=300 if bytes_limit else 65536)
    )
    native.add("1-0", b"x" * 1000 if bytes_limit else b"one")
    if not bytes_limit:
        await native.next_poll()
        native.add("2-0", b"two")
    error = await asyncio.wait_for(session.wait_failure(), 1)
    assert isinstance(error, TransportBackpressureError)
    assert len(session._buffer) <= 1 and session._buffer_bytes <= session._context.buffers.max_bytes
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize(
    "name,expected",
    [
        ("AuthenticationError", TransportAuthenticationError),
        ("NoPermissionError", TransportAuthorizationError),
        ("ResponseError", TransportSubscriptionError),
        ("ConnectionError", TransportConnectionError),
    ],
)
@pytest.mark.asyncio
async def test_startup_errors_are_typed_sanitized_and_rolled_back(monkeypatch, name, expected):
    native = Native()
    cause = type(name, (Exception,), {})("SECRET source or credentials")
    native.error = cause
    with pytest.raises(expected) as error:
        await open_session(adapter(monkeypatch, native))
    assert "SECRET" not in str(error.value) and error.value.__cause__ is cause
    assert native.clients[0].closed


@pytest.mark.asyncio
async def test_terminal_error_is_not_retried_and_failure_signal_survives_cancellation(monkeypatch):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    waiter = asyncio.create_task(session.wait_failure())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    native.error = OSError("SECRET")
    error = await asyncio.wait_for(session.wait_failure(), 1)
    assert isinstance(error, TransportInterruptedError) and "SECRET" not in str(error)
    await asyncio.sleep(0)
    assert session._poll_task.done()
    assert native.clients[0].close_calls == 0
    with pytest.raises(TransportInterruptedError):
        await anext(session.receive())
    await session.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.asyncio
async def test_startup_cancellation_or_deadline_closes_owned_client(monkeypatch, cancel):
    native = Native()
    native.execute_gate = asyncio.Event()
    task = asyncio.create_task(
        open_session(adapter(monkeypatch, native), ctx=context(startup_timeout=0.03 if not cancel else 2))
    )
    await asyncio.wait_for(native.started.wait(), 1)
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
        await task
    assert native.clients[0].closed


@pytest.mark.asyncio
async def test_close_is_idempotent_stops_polling_and_completes_waiters(monkeypatch):
    native = Native()
    session = await open_session(adapter(monkeypatch, native))
    await asyncio.gather(*(session.close(deadline=time.monotonic() + 1) for _ in range(3)))
    assert native.clients[0].closed and native.clients[0].close_calls == 1
    assert session._poll_task.done() and await session.wait_failure() is None
    with pytest.raises(StopAsyncIteration):
        await anext(session.receive())


@pytest.mark.parametrize("stalled", [False, True])
@pytest.mark.asyncio
async def test_cleanup_failure_never_claims_closed(monkeypatch, stalled):
    native = Native()
    if stalled:
        native.close_gate = asyncio.Event()
    else:
        native.close_error = RuntimeError("SECRET")
    session = await open_session(adapter(monkeypatch, native))
    with pytest.raises(ObserverCleanupTimeoutError if stalled else ObserverCleanupError) as error:
        await session.close(deadline=time.monotonic() + 0.03)
    assert "SECRET" not in str(error.value) and not session._closed
    assert isinstance(await session.wait_failure(), TransportInterruptedError)
