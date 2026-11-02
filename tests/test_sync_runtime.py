"""Regressions for acknowledged blocking commands and primary shutdown failures."""

import asyncio
import concurrent.futures
import contextvars
import threading

import pytest

from event_observer import LifecyclePolicy, SyncEventObserver
from event_observer._runtime.threaded import CommandCancelled, ThreadRuntime
from event_observer.errors import ObserverFailedError, SyncRuntimeUnresponsiveError, TransportError
from event_observer.testing import FakeTransport


def start_runtime(runtime, *, poll=None):
    idle = threading.Event()
    runtime.start(lambda: None, poll or idle.wait, lambda: None, budget=2)


def test_blocking_observation_never_creates_or_drives_an_asyncio_loop(monkeypatch):
    adapter = FakeTransport()
    caller = threading.get_ident()
    callback_threads = []

    def forbidden(*args, **kwargs):
        raise AssertionError("Blocking observation must not create an asyncio runtime")

    def matcher(payload):
        callback_threads.append(threading.get_ident())
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio.get_running_loop()
        return payload == b"completed"

    monkeypatch.setattr(asyncio, "new_event_loop", forbidden)
    monkeypatch.setattr(asyncio.events, "new_event_loop", forbidden)
    monkeypatch.setattr(asyncio, "run", forbidden)
    monkeypatch.setattr(asyncio, "Runner", forbidden)
    with SyncEventObserver(transport=adapter).watch(source="events") as watcher:
        adapter.publish(b"completed")
        assert watcher.wait_for(matcher, timeout=1) == b"completed"
        assert callback_threads == [watcher._runtime.thread.ident]
        assert callback_threads[0] != caller
    assert adapter.close_count == 1


def test_worker_keeps_collecting_while_caller_has_no_pending_command():
    adapter = FakeTransport()
    decoded = threading.Event()
    worker_threads = []

    class Decoder:
        def decode(self, raw):
            worker_threads.append(threading.get_ident())
            decoded.set()
            return raw.payload

    with SyncEventObserver(transport=adapter).watch(source="events", decoder=Decoder()) as watcher:
        adapter.publish(b"collected while caller was idle")
        assert decoded.wait(2)
        assert watcher._runtime._pending is None
        assert worker_threads == [watcher._runtime.thread.ident]
        assert watcher.snapshot().events[0].payload == b"collected while caller was idle"
        assert watcher.wait_for(lambda _: True, timeout=1) == b"collected while caller was idle"


def test_decoder_and_matcher_contexts_are_independent_and_never_leak_to_caller():
    adapter = FakeTransport()
    value = contextvars.ContextVar("observation_test_context", default="default")
    decoding_context = []
    matching_context = []

    class Decoder:
        def decode(self, raw):
            decoding_context.append(value.get())
            return raw.payload

    def matcher(payload):
        matching_context.append(value.get())
        value.set("modified by callback")
        return True

    token = value.set("startup context")
    try:
        with SyncEventObserver(transport=adapter).watch(source="events", decoder=Decoder()) as watcher:
            value.set("expectation context")
            adapter.publish(b"first")
            assert watcher.wait_for(matcher, timeout=1) == b"first"
            assert value.get() == "expectation context"
            assert watcher.wait_for(matcher, timeout=1) == b"first"
        assert decoding_context == ["startup context"]
        assert matching_context == ["expectation context", "expectation context"]
        assert value.get() == "expectation context"
    finally:
        value.reset(token)


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
        pending = watcher._runtime._pending
        assert pending is not None and not pending.done.is_set()
        queued = watcher._runtime._queue.qsize()
        for operation in (watcher.snapshot, watcher.mark, watcher.diagnostics.snapshot):
            with pytest.raises(SyncRuntimeUnresponsiveError):
                operation()
        assert watcher._runtime._queue.qsize() == queued
    finally:
        release.set()
        pending = watcher._runtime._pending
        if pending is not None:
            assert pending.done.wait(2)
        watcher.close()
    assert not watcher._runtime.thread.is_alive()


def test_worker_closes_publication_before_entering_slow_final_cleanup():
    runtime = ThreadRuntime(0.1, 0.05)
    fail = threading.Event()
    cleaning = threading.Event()
    release = threading.Event()

    def poll(timeout):
        if fail.is_set():
            raise RuntimeError("Synthetic worker failure")
        fail.wait(timeout)

    def cleanup():
        cleaning.set()
        assert release.wait(5)

    runtime.start(lambda: None, poll, cleanup, budget=1)
    try:
        fail.set()
        assert cleaning.wait(2)
        assert not runtime.running
        before = runtime._queue.qsize()
        with pytest.raises(SyncRuntimeUnresponsiveError):
            runtime.call(lambda: pytest.fail("A stopping worker must not accept a new command"))
        assert runtime._queue.qsize() == before
        assert runtime._pending is None
    finally:
        release.set()
        runtime.stop()
    assert not runtime.thread.is_alive()


def test_known_observation_failure_wins_over_native_cleanup_failure():
    primary = TransportError("Primary observation failure")
    secondary = TransportError("Secondary native cleanup failure")

    class CleanupFailure(FakeTransport):
        def sync_adapter(self):
            adapter = super().sync_adapter()
            original_open = adapter.open

            def open(source, start, *, context):
                session = original_open(source, start, context=context)
                original_close = session.close

                def close(*, deadline):
                    original_close(deadline=deadline)
                    raise secondary

                session.close = close
                return session

            adapter.open = open
            return adapter

    adapter = CleanupFailure()
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    with pytest.raises(ObserverFailedError) as caught, watcher:
        adapter.fail(primary)
        with pytest.raises(ObserverFailedError):
            watcher.wait_for(lambda _: True, timeout=1)
    assert caught.value.cause is primary
    assert caught.value.__cause__ is primary
    assert any("cleanup also failed" in note for note in caught.value.__notes__)
    assert watcher._cleanup_error.cause is secondary
    assert adapter.close_count == 1
    assert not watcher._runtime.thread.is_alive()
    assert adapter.close_count == 1


def test_cancellation_of_a_submission_that_has_not_started_is_acknowledged():
    runtime = ThreadRuntime(0.05, 0.01)
    entered = threading.Event()
    release = threading.Event()
    invoked = []

    def blocked_receive(timeout):
        entered.set()
        assert release.wait(5)

    def operation():
        invoked.append(True)
        return "completed"

    start_runtime(runtime, poll=blocked_receive)
    try:
        assert entered.wait(2)
        with pytest.raises(SyncRuntimeUnresponsiveError):
            runtime.call(operation)
        pending = runtime._pending
        assert pending is not None and not pending.done.is_set()
        assert runtime._queue.qsize() == 1
        with pytest.raises(SyncRuntimeUnresponsiveError):
            runtime.call(operation)
        assert runtime._queue.qsize() == 1
        release.set()
        assert pending.done.wait(2)
        assert invoked == []
        assert runtime.call(operation) == "completed"
        assert invoked == [True]
    finally:
        release.set()
        if runtime._pending is not None:
            assert runtime._pending.done.wait(2)
        runtime.stop()


def test_acknowledged_cancellation_releases_the_runtime_for_next_command():
    runtime = ThreadRuntime(0.05, 0.5)
    cancelled = threading.Event()

    class Pending:
        def step(self):
            if runtime.current_command.cancelled.is_set():
                cancelled.set()
                raise CommandCancelled()
            return False, None

    start_runtime(runtime)
    try:
        with pytest.raises(SyncRuntimeUnresponsiveError, match="exceeded its control budget"):
            runtime.call(Pending)
        assert cancelled.is_set()
        assert runtime.call(lambda: "recovered") == "recovered"
    finally:
        runtime.stop()


def test_cancellation_between_dequeue_and_first_step_skips_operation(monkeypatch):
    runtime = ThreadRuntime(0.05, 0.01)
    dequeued = threading.Event()
    release = threading.Event()
    invoked = []
    dequeue = runtime._queue.get_nowait

    def delayed_dequeue():
        command = dequeue()
        dequeued.set()
        assert release.wait(5)
        return command

    def operation():
        invoked.append(True)

    monkeypatch.setattr(runtime._queue, "get_nowait", delayed_dequeue)
    start_runtime(runtime)
    try:
        with pytest.raises(SyncRuntimeUnresponsiveError):
            runtime.call(operation)
        assert dequeued.is_set()
        pending = runtime._pending
        assert pending is not None and not pending.done.is_set()
        release.set()
        assert pending.done.wait(2)
        assert invoked == []
    finally:
        release.set()
        if runtime._pending is not None:
            assert runtime._pending.done.wait(2)
        runtime.stop()


@pytest.mark.parametrize("after_enqueue", [False, True])
def test_interrupt_during_handoff_keeps_a_cancellable_submission(monkeypatch, after_enqueue):
    runtime = ThreadRuntime(0.1, 0.01)
    entered = threading.Event()
    release = threading.Event()
    invoked = []
    submission = {}
    publish = runtime._queue.put_nowait

    def blocked_receive(timeout):
        entered.set()
        assert release.wait(5)

    def operation():
        invoked.append(True)

    def interrupted_publish(command):
        submission["command"] = command
        if after_enqueue:
            publish(command)
        raise KeyboardInterrupt

    start_runtime(runtime, poll=blocked_receive)
    try:
        assert entered.wait(2)
        monkeypatch.setattr(runtime._queue, "put_nowait", interrupted_publish)
        with pytest.raises(KeyboardInterrupt):
            runtime.call(operation)
        command = submission["command"]
        assert command.cancelled.is_set()
        if not after_enqueue:
            assert command.done.is_set()
        release.set()
        assert command.done.wait(2)
        assert invoked == []
        monkeypatch.setattr(runtime._queue, "put_nowait", publish)
        assert runtime.call(lambda: "recovered") == "recovered"
    finally:
        release.set()
        monkeypatch.setattr(runtime._queue, "put_nowait", publish)
        if runtime._pending is not None:
            assert runtime._pending.done.wait(2)
        runtime.stop()


def test_terminal_failure_is_primary_when_runtime_stop_also_fails(monkeypatch):
    adapter = FakeTransport()
    watcher = SyncEventObserver(transport=adapter).watch(source="events")
    transport_error = TransportError("Private transport details")
    stop_error = SyncRuntimeUnresponsiveError("Runtime stop failed")
    watcher.__enter__()
    real_stop = watcher._runtime.stop

    def fail_stop():
        raise stop_error

    try:
        adapter.fail(transport_error)
        with pytest.raises(ObserverFailedError):
            watcher.wait_for(lambda _: True, timeout=1)
        monkeypatch.setattr(watcher._runtime, "stop", fail_stop)
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
    real_stop = watcher._runtime.stop

    def fail_stop():
        raise stop_error

    try:
        with pytest.raises(ValueError) as caught, watcher:
            monkeypatch.setattr(watcher._runtime, "stop", fail_stop)
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
    real_stop = watcher._runtime.stop

    def fail_stop():
        raise stop_error

    try:
        monkeypatch.setattr(watcher._runtime, "stop", fail_stop)
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
    assert not watcher._runtime.thread.is_alive()


def test_startup_error_is_primary_when_runtime_stop_also_fails(monkeypatch):
    primary = TransportError("Startup failed")

    class UnavailableTransport:
        capabilities = FakeTransport.capabilities

        def open(self, source, start, *, context):
            raise primary

    watcher = SyncEventObserver(transport=UnavailableTransport()).watch(source="events")
    real_stop = watcher._runtime.stop

    def fail_stop():
        raise SyncRuntimeUnresponsiveError("Runtime stop failed")

    try:
        monkeypatch.setattr(watcher._runtime, "stop", fail_stop)
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
    real_stop = watcher._runtime.stop

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
                monkeypatch.setattr(watcher._runtime, "stop", interrupt_stop)
            raise AssertionError("Application assertion failed before interruption")
        assert caught.value is interrupt
        assert adapter.close_count == 1
    finally:
        real_stop()
    assert not watcher._runtime.thread.is_alive()
