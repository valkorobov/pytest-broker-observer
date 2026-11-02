"""Regressions for in-flight commands and primary failures at runtime shutdown."""

import asyncio
import concurrent.futures
import threading

import pytest

from event_observer import LifecyclePolicy, SyncEventObserver
from event_observer.errors import ObserverFailedError, SyncRuntimeUnresponsiveError, TransportError
from event_observer.sync import _Portal
from event_observer.testing import FakeTransport


def test_blocked_matcher_does_not_allow_additional_runtime_commands():
    adapter = FakeTransport()
    entered = threading.Event()
    release = threading.Event()
    lifecycle = LifecyclePolicy(sync_control_timeout=0.1, cancellation_grace=0.05)
    watcher = SyncEventObserver(transport=adapter, lifecycle=lifecycle).watch(source="events")

    def blocked(payload):
        entered.set()
        assert release.wait(5)
        return True

    watcher.__enter__()
    try:
        adapter.publish(b"completed")
        with pytest.raises(SyncRuntimeUnresponsiveError):
            watcher.wait_for(blocked, timeout=0.05)
        assert entered.is_set()
        pending = watcher._portal._pending
        assert pending is not None and not pending.is_set()
        queued = len(watcher._portal.loop._ready)
        for operation in (watcher.snapshot, watcher.mark, watcher.diagnostics.snapshot):
            with pytest.raises(SyncRuntimeUnresponsiveError):
                operation()
        assert len(watcher._portal.loop._ready) == queued
    finally:
        release.set()
        pending = watcher._portal._pending
        if pending is not None:
            assert pending.wait(2)
        watcher.close()
    assert not watcher._portal.thread.is_alive()
    assert adapter.close_count == 1


def test_cancellation_of_a_submission_that_has_not_started_is_acknowledged():
    portal = _Portal(0.05, 0.01)
    entered = threading.Event()
    release = threading.Event()
    invoked = []

    def block_loop():
        entered.set()
        assert release.wait(5)

    async def operation():
        invoked.append(True)
        return "completed"

    portal.start()
    try:
        portal.loop.call_soon_threadsafe(block_loop)
        assert entered.wait(2)
        with pytest.raises(SyncRuntimeUnresponsiveError):
            portal.call(operation)
        pending = portal._pending
        assert pending is not None
        with pytest.raises(SyncRuntimeUnresponsiveError):
            portal.call(operation)
        release.set()
        assert pending.wait(2)
        assert invoked == []
        assert portal.call(operation) == "completed"
        assert invoked == [True]
    finally:
        release.set()
        if portal._pending is not None:
            assert portal._pending.wait(2)
        portal.stop()


def test_acknowledged_cancellation_releases_the_runtime_for_next_command():
    portal = _Portal(0.05, 0.5)
    cancelled = threading.Event()

    async def operation():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def recovered():
        return "recovered"

    portal.start()
    try:
        with pytest.raises(SyncRuntimeUnresponsiveError, match="exceeded its control budget"):
            portal.call(operation)
        assert cancelled.is_set()
        assert portal.call(recovered) == "recovered"
    finally:
        portal.stop()


def test_cancellation_between_task_creation_and_first_step_skips_operation():
    portal = _Portal(0.05, 0.01)
    configured = threading.Event()
    created = threading.Event()
    release = threading.Event()
    invoked = []

    def task_factory(loop, coro, **kwargs):
        task = asyncio.Task(coro, loop=loop, **kwargs)
        created.set()
        assert release.wait(5)
        return task

    def install_factory():
        portal.loop.set_task_factory(task_factory)
        configured.set()

    async def operation():
        invoked.append(True)

    portal.start()
    try:
        portal.loop.call_soon_threadsafe(install_factory)
        assert configured.wait(2)
        with pytest.raises(SyncRuntimeUnresponsiveError):
            portal.call(operation)
        assert created.is_set()
        pending = portal._pending
        assert pending is not None
        release.set()
        assert pending.wait(2)
        assert invoked == []
    finally:
        release.set()
        if portal._pending is not None:
            assert portal._pending.wait(2)
        portal.loop.call_soon_threadsafe(portal.loop.set_task_factory, None)
        portal.stop()


@pytest.mark.parametrize("after_enqueue", [False, True])
def test_interrupt_during_handoff_keeps_a_cancellable_submission(monkeypatch, after_enqueue):
    portal = _Portal(0.1, 0.5)
    started = threading.Event()
    finished = threading.Event()
    submission = {}
    prepare = portal._prepare_command

    async def operation():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            finished.set()

    async def recovered():
        return "recovered"

    def prepare_interruptible(operation, acknowledged):
        future, enqueue = prepare(operation, acknowledged)
        submission.update(future=future, acknowledged=acknowledged)

        def interrupt_handoff():
            if after_enqueue:
                enqueue()
            raise KeyboardInterrupt

        return future, interrupt_handoff

    portal.start()
    try:
        monkeypatch.setattr(portal, "_prepare_command", prepare_interruptible)
        with pytest.raises(KeyboardInterrupt):
            portal.call(operation)
        assert submission["future"].cancelled()
        assert submission["acknowledged"].wait(2)
        assert not started.is_set() or finished.is_set()
        if not after_enqueue:
            assert not started.is_set()
        monkeypatch.setattr(portal, "_prepare_command", prepare)
        assert portal.call(recovered) == "recovered"
    finally:
        monkeypatch.setattr(portal, "_prepare_command", prepare)
        portal.stop()


def test_terminal_failure_is_primary_when_runtime_stop_also_fails(monkeypatch):
    adapter = FakeTransport()
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    transport_error = TransportError("Private transport details")
    stop_error = SyncRuntimeUnresponsiveError("Runtime stop failed")
    watcher.__enter__()
    real_stop = watcher._portal.stop

    def fail_stop():
        raise stop_error

    try:
        adapter.fail(transport_error)
        with pytest.raises(ObserverFailedError):
            watcher.wait_for(lambda _: True, timeout=1)
        monkeypatch.setattr(watcher._portal, "stop", fail_stop)
        with pytest.raises(ObserverFailedError) as caught:
            watcher.__exit__(None, None, None)
        assert caught.value.cause is transport_error
        assert caught.value.__cause__ is transport_error
        assert any("cleanup also failed" in note for note in caught.value.__notes__)
        assert watcher._stop_error is stop_error
    finally:
        real_stop()


def test_context_error_is_primary_when_runtime_stop_also_fails(monkeypatch):
    watcher = SyncEventObserver(transport=FakeTransport()).watch(source="events")
    primary = ValueError("Application assertion failed")
    stop_error = SyncRuntimeUnresponsiveError("Runtime stop failed")
    real_stop = watcher._portal.stop

    def fail_stop():
        raise stop_error

    try:
        with pytest.raises(ValueError) as caught, watcher:
            monkeypatch.setattr(watcher._portal, "stop", fail_stop)
            raise primary
        assert caught.value is primary
        assert any("cleanup also failed" in note for note in primary.__notes__)
        assert watcher._stop_error is stop_error
    finally:
        real_stop()


def test_stop_error_is_raised_when_no_primary_failure_exists(monkeypatch):
    watcher = SyncEventObserver(transport=FakeTransport()).watch(source="events")
    stop_error = SyncRuntimeUnresponsiveError("Runtime stop failed")
    watcher.__enter__()
    real_stop = watcher._portal.stop

    def fail_stop():
        raise stop_error

    try:
        monkeypatch.setattr(watcher._portal, "stop", fail_stop)
        with pytest.raises(SyncRuntimeUnresponsiveError) as caught:
            watcher.close()
        assert caught.value is stop_error
    finally:
        real_stop()


def test_context_error_is_primary_when_cleanup_command_dispatch_fails(monkeypatch):
    adapter = FakeTransport()
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    primary = AssertionError("Application assertion failed")
    dispatch_error = SyncRuntimeUnresponsiveError("Cleanup command response failed")
    real_dispatch = watcher._dispatch

    def fail_cleanup_response(operation, **kwargs):
        result = real_dispatch(operation, **kwargs)
        if operation.__name__ == "finish":
            raise dispatch_error
        return result

    with pytest.raises(AssertionError) as caught, watcher:
        monkeypatch.setattr(watcher, "_dispatch", fail_cleanup_response)
        raise primary
    assert caught.value is primary
    assert watcher._finish_error is dispatch_error
    assert any("cleanup command also failed" in note for note in primary.__notes__)
    assert adapter.close_count == 1
    assert not watcher._portal.thread.is_alive()


def test_startup_error_is_primary_when_runtime_stop_also_fails(monkeypatch):
    primary = TransportError("Startup failed")

    class UnavailableTransport(FakeTransport):
        async def open(self, source, start, *, context):
            raise primary

    watcher = SyncEventObserver(transport=UnavailableTransport()).watch(source="events")
    real_stop = watcher._portal.stop

    def fail_stop():
        raise SyncRuntimeUnresponsiveError("Runtime stop failed")

    try:
        monkeypatch.setattr(watcher._portal, "stop", fail_stop)
        with pytest.raises(TransportError) as caught:
            watcher.__enter__()
        assert caught.value is primary
        assert any("cleanup also failed" in note for note in primary.__notes__)
    finally:
        real_stop()


@pytest.mark.parametrize("stage", ["finish", "stop"])
@pytest.mark.parametrize(
    "interrupt_type",
    [KeyboardInterrupt, SystemExit, asyncio.CancelledError, concurrent.futures.CancelledError],
)
def test_new_caller_interruption_propagates_over_body_error_after_cleanup(monkeypatch, stage, interrupt_type):
    adapter = FakeTransport()
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    interrupt = interrupt_type()
    real_dispatch = watcher._dispatch
    real_stop = watcher._portal.stop

    def interrupt_finish(operation, **kwargs):
        result = real_dispatch(operation, **kwargs)
        if operation.__name__ == "finish":
            raise interrupt
        return result

    def interrupt_stop():
        raise interrupt

    try:
        with pytest.raises(interrupt_type) as caught, watcher:
            if stage == "finish":
                monkeypatch.setattr(watcher, "_dispatch", interrupt_finish)
            else:
                monkeypatch.setattr(watcher._portal, "stop", interrupt_stop)
            raise AssertionError("Application assertion failed before interruption")
        assert caught.value is interrupt
        assert adapter.close_count == 1
    finally:
        real_stop()
    assert not watcher._portal.thread.is_alive()
