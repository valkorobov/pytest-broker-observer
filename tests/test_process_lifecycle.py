"""Exercise real process interruption and owned observation resources."""

import asyncio
from contextlib import contextmanager, nullcontext
import json
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.config import LifecyclePolicy
from event_observer.errors import ObserverCleanupTimeoutError, ObserverFailedError, TransportError
from event_observer.testing import FakeTransport


pytestmark = pytest.mark.reliability
PROBE = Path(__file__).with_name("process_lifecycle_probe.py")
PROCESS_TIMEOUT = 20


@contextmanager
def probe(tmp_path, *args, isolated_console=False):
    """Contain the probe in an independently reaped process with bounded output waits."""
    barrier = tmp_path / "reached"
    options = {}
    if isolated_console and sys.platform == "win32":
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = subprocess.SW_HIDE
        options.update(creationflags=subprocess.CREATE_NEW_CONSOLE, startupinfo=startup)
    process = subprocess.Popen(
        [sys.executable, "-I", str(PROBE), *args, str(barrier)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=tmp_path,
        **options,
    )
    try:
        yield process, barrier
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=PROCESS_TIMEOUT)


def await_barrier(process, barrier):
    """Wait for a child-reported state instead of guessing when to interrupt."""
    deadline = time.monotonic() + PROCESS_TIMEOUT
    while not barrier.is_file() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    if not barrier.is_file():
        if process.poll() is None:
            process.kill()
        output, _ = process.communicate(timeout=PROCESS_TIMEOUT)
        pytest.fail(f"Probe did not reach its interruption barrier. Child output:\n{output}")
    return int(barrier.read_text(encoding="ascii"))


def completed(process):
    """Require the child to exit gracefully and return its lifecycle observation."""
    output, _ = process.communicate(timeout=PROCESS_TIMEOUT)
    assert process.returncode == 0, output
    return json.loads(output)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_ctrl_c_interrupts_active_watch_and_cleans_up(tmp_path, mode):
    with probe(tmp_path, "interrupt", mode, isolated_console=True) as (process, barrier):
        interpreter_pid = await_barrier(process, barrier)
        if sys.platform == "win32":
            sender = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(PROBE),
                    "send-windows-interrupt",
                    str(interpreter_pid),
                    str(process.pid),
                ],
                creationflags=subprocess.CREATE_NO_WINDOW,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=PROCESS_TIMEOUT,
            )
            assert sender.returncode == 0, sender.stderr
        else:
            assert interpreter_pid == process.pid
            process.send_signal(signal.SIGINT)
        assert completed(process) == {
            "state": "closed",
            "cleanup": "complete",
            "opened": 1,
            "closed": 1,
            "actor_threads": 0,
            "error": "KeyboardInterrupt",
            "pending_tasks": 0 if mode == "async" else None,
        }


@pytest.mark.parametrize("target", ["matcher", "native-close"])
def test_stalled_blocking_call_reports_abandonment_and_process_exits(tmp_path, target):
    with probe(tmp_path, "stall", "sync", target) as (process, barrier):
        await_barrier(process, barrier)
        result = completed(process)
        assert result["error"] == "sync_runtime_unresponsive"
        assert result["abandoned"] is True
        assert result["actor_threads"] == result["opened"] == 1
        assert result["closed"] == 0
        assert result["state"] != "closed" and result["cleanup"] != "complete"


@pytest.mark.parametrize("target", ["matcher", "native-close"])
def test_stalled_async_call_requires_external_containment(tmp_path, target):
    with probe(tmp_path, "stall", "async", target) as (process, barrier):
        await_barrier(process, barrier)
        with pytest.raises(subprocess.TimeoutExpired):
            process.wait(timeout=0.7)
        process.kill()
        output, _ = process.communicate(timeout=PROCESS_TIMEOUT)
        assert process.returncode != 0
        assert output == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("body_failed", [False, True], ids=["normal-exit", "primary-error"])
async def test_async_close_budget_is_honest_and_releases_tasks(body_failed):
    tasks_before = asyncio.all_tasks()
    entered_close = asyncio.Event()
    cancelled_close = asyncio.Event()

    class ClosingTransport(FakeTransport):
        async def open(self, source, start, *, context):
            session = await super().open(source, start, context=context)

            async def stalled_close(*, deadline):
                entered_close.set()
                try:
                    await asyncio.Future()
                finally:
                    cancelled_close.set()

            session.close = stalled_close
            return session

    adapter = ClosingTransport()
    watcher = EventObserver(transport=adapter, lifecycle=LifecyclePolicy(shutdown_timeout=0.5)).watch(
        source="events"
    )
    primary = AssertionError("Body failure takes priority over stalled cleanup")
    with pytest.raises(AssertionError if body_failed else ObserverCleanupTimeoutError) as caught:
        async with watcher:
            if body_failed:
                raise primary

    if body_failed:
        assert caught.value is primary
        assert any("cleanup also failed" in note for note in caught.value.__notes__)
    assert entered_close.is_set() and cancelled_close.is_set()
    assert watcher.cleanup_state == "abandoned" and watcher.state.value == "failed"
    assert adapter.open_count == 1 and adapter.close_count == 0
    assert asyncio.all_tasks() == tasks_before


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_repeated_lifecycles_release_owned_resources(mode):
    adapter = FakeTransport()
    cycles = 40
    threads_before = {thread for thread in threading.enumerate() if thread.name.startswith("event-observer-")}

    def exercise_sync():
        observer = SyncEventObserver(transport=adapter)
        for index in range(cycles):
            failure = TransportError("Injected failure after admission") if index % 2 else None
            watcher = observer.watch(source="events")
            outcome = pytest.raises(ObserverFailedError) if failure is not None else nullcontext()
            with outcome as caught:
                with watcher:
                    mark = watcher.mark()
                    adapter.publish(b"owned event")
                    assert watcher.wait_for(lambda _: True, timeout=2, since=mark) == b"owned event"
                    if failure is not None:
                        adapter.fail(failure)
                        with pytest.raises(ObserverFailedError):
                            watcher.wait_for(lambda _: False, timeout=2, since=mark)
                        watcher.wait_for(lambda _: True, timeout=2, since=mark)
            if failure is not None:
                assert caught.value.cause is failure
            assert not watcher._runtime.thread.is_alive()
            assert watcher.cleanup_state == "complete"

    async def exercise_async():
        tasks_before = asyncio.all_tasks()
        observer = EventObserver(transport=adapter)
        for index in range(cycles):
            failure = TransportError("Injected failure after admission") if index % 2 else None
            watcher = observer.watch(source="events")
            outcome = pytest.raises(ObserverFailedError) if failure is not None else nullcontext()
            with outcome as caught:
                async with watcher:
                    mark = watcher.mark()
                    adapter.publish(b"owned event")
                    assert await watcher.wait_for(lambda _: True, timeout=2, since=mark) == b"owned event"
                    if failure is not None:
                        adapter.fail(failure)
                        with pytest.raises(ObserverFailedError):
                            await watcher.wait_for(lambda _: False, timeout=2, since=mark)
                        await watcher.wait_for(lambda _: True, timeout=2, since=mark)
            if failure is not None:
                assert caught.value.cause is failure
            assert asyncio.all_tasks() == tasks_before
            assert watcher.cleanup_state == "complete"

    if mode == "sync":
        exercise_sync()
    else:
        asyncio.run(exercise_async())
    assert adapter.open_count == adapter.close_count == cycles
    assert {
        thread for thread in threading.enumerate() if thread.name.startswith("event-observer-")
    } == threads_before
