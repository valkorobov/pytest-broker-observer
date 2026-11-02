"""Blocking Kafka bridge regressions using the shared controlled native client."""

import asyncio
import threading
import time

import pytest

from event_observer.errors import (
    ConfigurationError,
    ObserverCleanupError,
    TopologyChangedError,
    TransportConnectionError,
    TransportReadinessError,
)
from event_observer.models import StartPosition, TransportSource
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig
from event_observer.transports.sync_kafka import SyncKafkaAdapter

from tests.test_kafka import Message, Native, context


@pytest.fixture(autouse=True)
def prohibit_async_runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Blocking Kafka must not create or access an asynchronous runtime")

    for name in ("run", "new_event_loop", "get_running_loop"):
        monkeypatch.setattr(asyncio, name, forbidden)


def transport(monkeypatch, native, **options):
    monkeypatch.setattr("event_observer.transports.sync_kafka._load_native", lambda: native)
    monkeypatch.setattr("event_observer.transports.kafka._load_admin", lambda: native)
    return SyncKafkaAdapter(KafkaConfig("broker", **options))


def next_event(session):
    for _ in range(20):
        raw = session.poll(0.05)
        if raw is not None:
            return raw
    raise AssertionError("Expected a native delivery within the bounded wait")


def test_factory_provider_shares_config_without_starting_clients():
    original = KafkaAdapter(KafkaConfig("broker"))
    blocking = original.sync_adapter()
    assert isinstance(blocking, SyncKafkaAdapter)
    assert blocking.config is original.config
    assert blocking.capabilities == original.capabilities
    assert blocking.transport_name == original.transport_name == "KafkaAdapter"


def test_latest_replays_publication_during_probe_and_owns_native_calls(monkeypatch):
    native = Native()
    native.after_probe_assignment = lambda value: value.messages.append(Message(0, 0, b"new"))
    session = transport(monkeypatch, native).open(
        TransportSource("events"), StartPosition.latest(), context=context()
    )
    try:
        assert session.readiness.boundary.positions == {0: 0}
        assert next_event(session).payload == b"new"
        assert session.failure() is None
    finally:
        session.close(deadline=time.monotonic() + 1)
    session.close(deadline=time.monotonic() + 1)
    assert not session._thread.is_alive()
    assert native.clients[0].closed
    assert set(native.clients[0].calls) == {session._thread.ident}
    assert set(native.admin_threads) == {session._thread.ident}
    assert session._thread.ident != threading.get_ident()
    assert session.poll(0) is None


def test_replay_preserves_tombstones_binary_headers_and_native_gaps(monkeypatch):
    native = Native(
        messages=[
            Message(0, 2, None, headers=(("x", None), ("x", b"\x00\xff"))),
            Message(0, 9, b"second"),
        ]
    )
    session = transport(monkeypatch, native).open(
        TransportSource("events"), StartPosition.earliest(), context=context()
    )
    try:
        first, second = next_event(session), next_event(session)
        assert first.payload is None
        assert first.headers == (("x", None), ("x", b"\x00\xff"))
        assert first.metadata["kafka.offset"] == 2
        assert second.metadata["kafka.offset"] == 9
    finally:
        session.close(deadline=time.monotonic() + 1)


def test_read_committed_uses_shared_physical_high_resolution(monkeypatch):
    native = Native(messages=[Message(0, 7)])
    native.consumer_high_override = 2
    session = transport(monkeypatch, native).open(
        TransportSource("events"), StartPosition.latest(), context=context()
    )
    try:
        assert session.readiness.boundary.positions == {0: 8}
        assert session.poll(0) is None
    finally:
        session.close(deadline=time.monotonic() + 1)


def test_topology_failure_precedes_full_handoff_without_polling(monkeypatch):
    native = Native(messages=[Message(0, 0)])
    session = transport(monkeypatch, native, topology_check_interval=0.001).open(
        TransportSource("events"), StartPosition.earliest(), context=context()
    )
    try:
        with session._condition:
            assert session._condition.wait_for(lambda: bool(session._queue), timeout=1)
        native.partitions = (0, 1)
        with session._condition:
            assert session._condition.wait_for(lambda: session._error is not None, timeout=1)
        assert isinstance(session.failure(), TopologyChangedError)
        with pytest.raises(TopologyChangedError):
            session.poll(0)
    finally:
        session.close(deadline=time.monotonic() + 1)


def test_startup_native_exception_preserves_cause_and_rolls_back(monkeypatch):
    native = Native()
    original = RuntimeError("sensitive native failure")

    def fail_probe(value):
        raise original

    native.after_probe_assignment = fail_probe
    with pytest.raises(TransportConnectionError) as caught:
        transport(monkeypatch, native).open(
            TransportSource("events"), StartPosition.latest(), context=context()
        )
    assert caught.value.cause is original
    assert caught.value.__cause__ is original
    assert "sensitive" not in str(caught.value)
    assert native.clients[0].closed


def test_cleanup_failure_retains_native_cause(monkeypatch):
    native = Native()
    session = transport(monkeypatch, native).open(
        TransportSource("events"), StartPosition.latest(), context=context()
    )
    native.close_failure = True
    with pytest.raises(ObserverCleanupError) as caught:
        session.close(deadline=time.monotonic() + 1)
    assert isinstance(caught.value.cause, RuntimeError)
    assert caught.value.__cause__ is caught.value.cause
    assert "sensitive" not in str(caught.value)
    assert not session._thread.is_alive()


def test_unconfirmed_fetch_startup_is_bounded_and_closes_client(monkeypatch):
    native = Native()
    native.suppress_fetch = True
    with pytest.raises((TimeoutError, TransportReadinessError)):
        transport(monkeypatch, native).open(
            TransportSource("events"), StartPosition.latest(), context=context(timeout=0.02)
        )
    assert native.clients[0].closed


@pytest.mark.parametrize("timeout", [-1, True, None, float("inf"), float("nan"), 10**1000])
def test_poll_rejects_invalid_or_unrepresentable_timeout(monkeypatch, timeout):
    native = Native()
    session = transport(monkeypatch, native).open(
        TransportSource("events"), StartPosition.latest(), context=context()
    )
    try:
        with pytest.raises(ConfigurationError):
            session.poll(timeout)
    finally:
        session.close(deadline=time.monotonic() + 1)
