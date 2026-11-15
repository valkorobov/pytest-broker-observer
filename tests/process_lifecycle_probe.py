"""Run an isolated observation with a real interrupt or a deliberately stalled call."""

import asyncio
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time

from event_observer import EventObserver, SyncEventObserver
from event_observer.config import LifecyclePolicy
from event_observer.errors import SyncRuntimeUnresponsiveError
from event_observer.testing import FakeTransport


def send_windows_interrupt(pid, launcher_pid):
    """Attach this helper to the isolated probe console and generate Ctrl+C."""
    import ctypes

    console = ctypes.WinDLL("kernel32", use_last_error=True)
    console.FreeConsole()
    if not console.AttachConsole(pid):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        # Verify the target console before broadcasting. On Windows a venv may
        # launch the actual interpreter as a second process in the same console.
        processes = (ctypes.c_ulong * 8)()
        count = console.GetConsoleProcessList(processes, len(processes))
        expected = {pid, launcher_pid, os.getpid()}
        if not 1 <= count <= len(processes) or not {pid, os.getpid()} <= set(processes[:count]) <= expected:
            raise RuntimeError("Ctrl+C probe console is not isolated")
        if not console.SetConsoleCtrlHandler(None, True):
            raise ctypes.WinError(ctypes.get_last_error())
        if not console.GenerateConsoleCtrlEvent(signal.CTRL_C_EVENT, 0):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        console.FreeConsole()


def reached(barrier):
    """Publish the actual interpreter PID only once the fault target is active."""
    pending = barrier.with_suffix(".tmp")
    pending.write_text(str(os.getpid()), encoding="ascii")
    pending.replace(barrier)


def report(watcher, adapter, **details):
    """Emit state observed by the child before it exits."""
    print(
        json.dumps(
            dict(
                state=watcher.state.value,
                cleanup=watcher.cleanup_state,
                opened=adapter.open_count,
                closed=adapter.close_count,
                actor_threads=sum(thread.name == "event-observer-sync" for thread in threading.enumerate()),
                **details,
            )
        ),
        flush=True,
    )


def interrupt_active_watch(mode, barrier):
    """Allow a real Ctrl+C to interrupt a caller action inside a READY watcher."""
    if sys.platform == "win32":
        import ctypes

        console = ctypes.WinDLL("kernel32", use_last_error=True)
        if not console.SetConsoleCtrlHandler(None, False):
            raise ctypes.WinError(ctypes.get_last_error())
    signal.signal(signal.SIGINT, signal.default_int_handler)
    adapter = FakeTransport()
    pending_tasks = None

    if mode == "sync":
        watcher = SyncEventObserver(transport=adapter).watch(source="events")

        def run():
            with watcher:
                adapter.publish(b"before interruption")
                assert watcher.wait_for(lambda _: True, timeout=5) == b"before interruption"
                reached(barrier)
                while True:
                    time.sleep(0.05)
    else:
        watcher = EventObserver(transport=adapter).watch(source="events")

        async def action():
            nonlocal pending_tasks
            try:
                async with watcher:
                    adapter.publish(b"before interruption")
                    assert await watcher.wait_for(lambda _: True, timeout=5) == b"before interruption"
                    reached(barrier)
                    while True:
                        await asyncio.sleep(0.05)
            finally:
                pending_tasks = sum(task is not asyncio.current_task() for task in asyncio.all_tasks())

        def run():
            asyncio.run(action())

    try:
        run()
    except KeyboardInterrupt:
        report(watcher, adapter, error="KeyboardInterrupt", pending_tasks=pending_tasks)
    else:
        raise AssertionError("watch completed without Ctrl+C")


def stall_call(mode, target, barrier):
    """Hold a callback or a protocol close forever without releasing it during the test."""
    transport = FakeTransport()
    lifecycle = LifecyclePolicy(shutdown_timeout=0.5, sync_control_timeout=0.5, cancellation_grace=0.2)
    never = threading.Event()

    def stall():
        reached(barrier)
        never.wait()

    def matcher(payload):
        stall()
        return True

    if mode == "async":
        if target == "native-close":
            original_open = transport.open

            async def close(*, deadline):
                stall()

            async def open_session(source, start, *, context):
                session = await original_open(source, start, context=context)
                session.close = close
                return session

            transport.open = open_session

        async def action():
            async with EventObserver(transport=transport, lifecycle=lifecycle).watch(
                source="events"
            ) as watcher:
                if target == "matcher":
                    transport.publish(b"observed")
                    await watcher.wait_for(matcher, timeout=5)

        asyncio.run(action())
        raise AssertionError("stalled async call unexpectedly completed")

    adapter = transport.sync_adapter()
    if target == "native-close":
        original_open = adapter.open

        def close(*, deadline):
            stall()

        def open_session(source, start, *, context):
            session = original_open(source, start, context=context)
            session.close = close
            return session

        adapter.open = open_session

    watcher = SyncEventObserver(transport=adapter, lifecycle=lifecycle).watch(source="events")
    try:
        with watcher:
            if target == "matcher":
                transport.publish(b"observed")
                watcher.wait_for(matcher, timeout=1.5)
    except SyncRuntimeUnresponsiveError as error:
        report(watcher, transport, error=error.code, abandoned=watcher._runtime.abandoned)
    else:
        raise AssertionError("stalled sync call unexpectedly completed")


if __name__ == "__main__":
    operation, *arguments = sys.argv[1:]
    if operation == "send-windows-interrupt":
        send_windows_interrupt(*map(int, arguments))
    elif operation == "interrupt":
        mode, path = arguments
        interrupt_active_watch(mode, Path(path))
    elif operation == "stall":
        mode, target, path = arguments
        stall_call(mode, target, Path(path))
    else:
        raise ValueError(f"Unknown probe operation {operation!r}")
