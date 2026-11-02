"""Public blocking-backend selection and explicit namespace regressions."""

import asyncio
import concurrent.futures
from dataclasses import replace
from types import SimpleNamespace

import pytest

from event_observer import EventObserver, SyncEventObserver, TransportSource
from event_observer.errors import (
    ConfigurationError,
    ObserverFailedError,
    ObserverStartupTimeoutError,
    SyncRuntimeUnresponsiveError,
    TransportError,
)
from event_observer.models import RawEvent, TransportCapabilities
from event_observer.testing import FakeTransport


def test_async_namespace_preserves_existing_classes():
    from event_observer.aio import EventObserver as ExplicitAsyncObserver

    assert ExplicitAsyncObserver is EventObserver


def test_async_only_extension_is_rejected_without_opening_it():
    calls = []

    async def open(source, start, *, context):
        calls.append(source)

    adapter = SimpleNamespace(open=open, capabilities=TransportCapabilities())
    with pytest.raises(ConfigurationError, match="async-only"):
        SyncEventObserver(transport=adapter)
    assert calls == []


def test_awaitable_provider_is_closed_and_rejected_synchronously():
    async def pending():
        pytest.fail("The blocking factory must not execute this coroutine")

    coroutine = pending()
    adapter = SimpleNamespace(open=lambda: None, sync_adapter=lambda: coroutine)
    with pytest.raises(ConfigurationError, match="synchronously"):
        SyncEventObserver(transport=adapter)
    assert coroutine.cr_frame is None


def test_blocking_open_cannot_return_a_hidden_coroutine():
    async def pending():
        pytest.fail("The blocking runtime must not execute this coroutine")

    coroutine = pending()
    adapter = SimpleNamespace(open=lambda *args, **kwargs: coroutine, capabilities=TransportCapabilities())
    with pytest.raises(ConfigurationError, match="awaitable"):
        with SyncEventObserver(transport=adapter).watch(source="events"):
            pytest.fail("Invalid blocking startup must not enter the body")
    assert coroutine.cr_frame is None


def test_builtin_provider_shares_publications_and_stays_lazy():
    adapter = FakeTransport()
    blocking = adapter.sync_adapter()
    events = SyncEventObserver(transport=blocking)
    assert adapter.open_count == 0
    with events.watch(source=TransportSource("events")) as watcher:
        mark = watcher.mark()
        adapter.publish(b"completed")
        assert (
            watcher.wait_for(lambda payload: payload == b"completed", since=mark, timeout=1) == b"completed"
        )
    assert adapter.open_count == adapter.close_count == 1


def test_invalid_session_rolls_back_its_allocated_synchronous_handle():
    closed = []

    async def invalid_poll(timeout):
        pytest.fail("An invalid poll must never be awaited by the blocking runtime")

    session = SimpleNamespace(
        poll=invalid_poll, failure=lambda: None, close=lambda **kwargs: closed.append(True)
    )
    adapter = SimpleNamespace(open=lambda *args, **kwargs: session, capabilities=TransportCapabilities())
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    with pytest.raises(ConfigurationError, match="synchronous"):
        watcher.__enter__()
    assert closed == [True]
    assert not watcher._runtime.thread.is_alive()


def test_invalid_async_close_is_reported_without_creating_an_event_loop():
    async def close(*, deadline):
        pytest.fail("The blocking runtime must never drive an async close")

    session = SimpleNamespace(poll=lambda timeout: None, failure=lambda: None, close=close)
    adapter = SimpleNamespace(open=lambda *args, **kwargs: session, capabilities=TransportCapabilities())
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    with pytest.raises(ConfigurationError, match="synchronous"):
        watcher.__enter__()
    assert watcher.cleanup_state == "failed"
    assert watcher.diagnostics.snapshot().records[-1].kind == "closed"
    assert not watcher._runtime.thread.is_alive()


def test_native_startup_timeout_uses_the_shared_public_domain_error():
    native_error = TimeoutError("Native startup timeout details")

    def open(*args, **kwargs):
        raise native_error

    adapter = SimpleNamespace(open=open, capabilities=TransportCapabilities())
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    with pytest.raises(ObserverStartupTimeoutError) as caught:
        watcher.__enter__()
    assert caught.value.cause is native_error
    assert caught.value.__cause__ is native_error
    assert not watcher._runtime.thread.is_alive()


def test_async_settlement_is_rejected_before_ready_and_rolls_back_native_session():
    adapter = FakeTransport()
    blocking = adapter.sync_adapter()
    open = blocking.open
    invoked = []

    async def settle(delivery_id, outcome):
        invoked.append(delivery_id)

    def open_with_async_settlement(source, start, *, context):
        session = open(source, start, context=context)
        session.readiness = replace(
            session.readiness,
            resolved_capabilities=replace(session.readiness.resolved_capabilities, supports_settlement=True),
        )
        session.settle = settle
        return session

    blocking.open = open_with_async_settlement
    watcher = SyncEventObserver(transport=blocking).watch(source="events")
    with pytest.raises(ConfigurationError, match="synchronous settle"):
        watcher.__enter__()
    assert invoked == []
    assert adapter.open_count == adapter.close_count == 1
    assert watcher.cleanup_state == "complete"
    assert not watcher._runtime.thread.is_alive()


def test_hidden_settlement_awaitable_is_closed_and_prevents_matching_success():
    adapter = FakeTransport()
    blocking = adapter.sync_adapter()
    open = blocking.open
    returned = []

    async def pending():
        pytest.fail("The blocking actor must never execute async settlement")

    def settle(delivery_id, outcome):
        coroutine = pending()
        returned.append(coroutine)
        return coroutine

    def open_with_hidden_awaitable(source, start, *, context):
        session = open(source, start, context=context)
        session.readiness = replace(
            session.readiness,
            resolved_capabilities=replace(session.readiness.resolved_capabilities, supports_settlement=True),
        )
        session.settle = settle
        return session

    blocking.open = open_with_hidden_awaitable
    watcher = SyncEventObserver(transport=blocking).watch(source="events")
    try:
        watcher.__enter__()
        adapter.publish(RawEvent(b"candidate", "events", delivery_id="delivery-1"))
        with pytest.raises(ObserverFailedError) as caught:
            watcher.wait_for(lambda _: True, timeout=1)
        assert isinstance(caught.value.cause, ConfigurationError)
        assert "settle() returned an awaitable" in str(caught.value.cause)
    finally:
        watcher.close()
    assert len(returned) == 1 and returned[0].cr_frame is None
    assert adapter.open_count == adapter.close_count == 1
    assert not watcher._runtime.thread.is_alive()


@pytest.mark.parametrize(
    "cleanup_type,is_interruption",
    [
        (KeyboardInterrupt, True),
        (SystemExit, True),
        (asyncio.CancelledError, True),
        (concurrent.futures.CancelledError, True),
        (SyncRuntimeUnresponsiveError, False),
    ],
)
def test_startup_cleanup_preserves_new_caller_interruption_priority(
    monkeypatch, cleanup_type, is_interruption
):
    primary = TransportError("Original startup failure")
    secondary = cleanup_type("Runtime stop failed")

    def open(*args, **kwargs):
        raise primary

    def interrupted_stop():
        raise secondary

    adapter = SimpleNamespace(open=open, capabilities=TransportCapabilities())
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    real_stop = watcher._runtime.stop
    monkeypatch.setattr(watcher._runtime, "stop", interrupted_stop)
    expected = secondary if is_interruption else primary
    try:
        with pytest.raises(type(expected)) as caught:
            watcher.__enter__()
        assert caught.value is expected
        assert watcher._stop_error is secondary
        if not is_interruption:
            assert any("cleanup also failed" in note for note in primary.__notes__)
    finally:
        real_stop()
    assert not watcher._runtime.thread.is_alive()
