"""A single-owner blocking actor with acknowledged, bounded commands."""

from __future__ import annotations

import contextvars
from dataclasses import dataclass, field
import os
import queue
import threading
from typing import Any, Callable, Protocol, runtime_checkable

from ..errors import (
    ProcessAffinityError,
    ReentrantSyncCallError,
    SyncAPIInAsyncContextError,
    SyncRuntimeUnresponsiveError,
    ThreadAffinityError,
)


class CommandCancelled(Exception):
    """Signal cooperative cancellation inside the blocking worker."""


@runtime_checkable
class PendingOperation(Protocol):
    """Advance an expectation without blocking reception on a future match."""

    def step(self) -> tuple[bool, Any]:
        """Perform a bounded slice of work.

        :returns: Completion flag and the final value when complete.
        """
        ...


@dataclass
class _Command:
    """One caller-owned request acknowledged only after actual worker completion.

    :ivar operation: Owner-thread operation submitted to the actor.
    :ivar context: Caller context copied before publication.
    :ivar cancelled: Cooperative cancellation request.
    :ivar done: Actual completion acknowledgement.
    :ivar value: Completed result, detached by the observation layer.
    :ivar error: Original operation error, if any.
    """

    operation: Callable[[], Any]
    context: contextvars.Context
    cancelled: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    value: Any = None
    error: BaseException | None = None


class ThreadRuntime:
    """Own one worker and allow at most one externally outstanding command.

    :ivar control_timeout: Default caller wait and thread-join budget.
    :ivar cancellation_grace: Budget for actual command cancellation acknowledgement.
    :ivar owner: Creating thread identity.
    :ivar pid: Creating process identity.
    :ivar thread: Daemon actor thread after startup.
    :ivar running: Whether the worker is servicing observation.
    :ivar abandoned: Whether a stop attempt left unresponsive work.
    :ivar current_command: Request whose callback or expectation currently executes.
    :ivar _queue: Bounded publication queue with one slot.
    :ivar _handoff: Lock making publication and worker ownership transfer observable atomically.
    :ivar _pending: Last request until its actual acknowledgement.
    :ivar _active: Whether the caller is already in a blocking public call.
    :ivar _stop: Worker stop request.
    :ivar _ready: Startup completion signal, including failed startup.
    :ivar _startup_error: Original startup error, if any.
    :ivar _startup_context: Context captured on entry to the watcher.
    :ivar _startup: Worker-owned resource startup callback.
    :ivar _poll: Finite receive/failure callback run between expectation slices.
    :ivar _shutdown: Idempotent owned-resource cleanup callback.
    """

    def __init__(self, control_timeout: float, cancellation_grace: float) -> None:
        """Create a lazy runtime without starting resources.

        :param control_timeout: Positive default control budget in seconds.
        :param cancellation_grace: Positive acknowledgement grace in seconds.
        """
        self.control_timeout, self.cancellation_grace = control_timeout, cancellation_grace
        self.owner, self.pid = threading.get_ident(), os.getpid()
        self.thread: threading.Thread | None = None
        self.running = self.abandoned = self._active = False
        self.current_command: _Command | None = None
        self._queue: queue.Queue[_Command] = queue.Queue(maxsize=1)
        self._handoff = threading.Lock()
        self._pending: _Command | None = None
        self._stop, self._ready = threading.Event(), threading.Event()
        self._startup_error: BaseException | None = None
        self._startup_context = contextvars.copy_context()
        self._startup: Callable[[], Any] = lambda: None
        self._poll: Callable[[float], Any] = lambda timeout: None
        self._shutdown: Callable[[], Any] = lambda: None

    def guard(self) -> None:
        """Reject foreign process/thread access and blocking use in an async context.

        :raises ProcessAffinityError: If the handle crossed a process boundary.
        :raises ThreadAffinityError: If another caller thread uses this handle.
        :raises ReentrantSyncCallError: If a worker callback reenters the blocking API.
        :raises SyncAPIInAsyncContextError: If the creating caller is running an async loop.
        """
        if os.getpid() != self.pid:
            raise ProcessAffinityError("Sync watcher belongs to another process")
        if self.thread is not None and threading.get_ident() == self.thread.ident:
            raise ReentrantSyncCallError("A runtime callback cannot call its blocking watcher")
        if threading.get_ident() != self.owner:
            raise ThreadAffinityError("Sync watcher belongs to its creating thread")
        # This is a misuse check only: it never creates or drives an event loop.
        import asyncio

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise SyncAPIInAsyncContextError("Use EventObserver in an active asynchronous context")

    def start(self, startup: Callable, poll: Callable, shutdown: Callable, *, budget: float) -> None:
        """Start owned observation and wait for the startup handshake.

        :param startup: Callback opening the native session on the worker.
        :param poll: Callback receiving within a finite budget and recording failures.
        :param shutdown: Idempotent worker-owned cleanup callback.
        :param budget: Caller startup-control budget in seconds.
        :raises BaseException: Original startup failure or an unresponsive runtime error.
        """
        self.guard()
        self._startup, self._poll, self._shutdown = startup, poll, shutdown
        self._startup_context = contextvars.copy_context()
        self.thread = threading.Thread(target=self._entry, name="event-observer-sync", daemon=True)
        self.thread.start()
        if not self._ready.wait(budget):
            raise SyncRuntimeUnresponsiveError("Sync startup was not acknowledged within its control budget")
        if self._startup_error is not None:
            raise self._startup_error

    def _entry(self) -> None:
        """Enter the copied startup context for the lifetime of the worker."""
        self._startup_context.run(self._run)

    def _run(self) -> None:
        """Interleave finite native reception, control commands, and expectation slices."""
        active: _Command | None = None
        pending: PendingOperation | None = None
        try:
            self._startup()
            self.running = True
            self._ready.set()
            while not self._stop.is_set():
                if active is None:
                    with self._handoff:
                        try:
                            active = self._queue.get_nowait()
                            self.current_command = active
                        except queue.Empty:
                            pass
                    if active is not None:
                        try:
                            if active.cancelled.is_set():
                                raise CommandCancelled()
                            value = active.context.run(active.operation)
                            if isinstance(value, PendingOperation):
                                pending = value
                            else:
                                active.value = value
                        except BaseException as error:
                            active.error = error
                        if pending is None:
                            self._queue.task_done()
                            active.done.set()
                            active = None
                            self.current_command = None
                self._poll(getattr(pending, "poll_timeout", 0.01) if active is not None else 0.01)
                if active is not None and pending is not None:
                    try:
                        complete, value = active.context.run(pending.step)
                        if complete:
                            active.value = value
                    except BaseException as error:
                        active.error = error
                        complete = True
                    if complete:
                        self._queue.task_done()
                        active.done.set()
                        active = pending = None
                        self.current_command = None
        except BaseException as error:
            if not self._ready.is_set():
                self._startup_error = error
            if active is not None:
                active.error = error
        finally:
            # Close command publication before draining; cleanup can block independently.
            with self._handoff:
                self.running = False
            if active is not None:
                active.cancelled.set()
                try:
                    if pending is not None:
                        active.context.run(pending.step)
                except BaseException as error:
                    active.error = error
                finally:
                    if active.error is None:
                        active.error = CommandCancelled()
                    active.done.set()
                    self.current_command = None
            while True:
                try:
                    queued = self._queue.get_nowait()
                except queue.Empty:
                    break
                queued.error = CommandCancelled()
                queued.done.set()
                self._queue.task_done()
            try:
                self._shutdown()
            except BaseException as error:
                if self._startup_error is None and not self._ready.is_set():
                    self._startup_error = error
            self.running = False
            self._ready.set()

    def call(self, operation: Callable[[], Any], *, budget: float | None = None):
        """Publish one bounded command and acknowledge cancellation before accepting another.

        :param operation: Callback returning a value or an incrementally advanced operation.
        :param budget: Optional caller-control budget; independent from domain admission deadlines.
        :returns: Actual completed operation result.
        :raises SyncRuntimeUnresponsiveError: If work is pending or control/acknowledgement expires.
        :raises BaseException: The original operation failure or caller interruption.
        """
        self.guard()
        if (
            not self.running
            or self._active
            or (self._pending is not None and not self._pending.done.is_set())
        ):
            raise SyncRuntimeUnresponsiveError("Sync runtime is unavailable or already handling a command")
        command = _Command(operation, contextvars.copy_context())
        try:
            with self._handoff:
                if (
                    not self.running
                    or self._stop.is_set()
                    or self._active
                    or (self._pending is not None and not self._pending.done.is_set())
                ):
                    raise SyncRuntimeUnresponsiveError("Sync runtime is stopping or handling a command")
                self._pending = command
                self._active = True
                self._queue.put_nowait(command)
            if not command.done.wait(self.control_timeout if budget is None else budget):
                command.cancelled.set()
                responsive = command.done.wait(self.cancellation_grace)
                raise SyncRuntimeUnresponsiveError(
                    "Sync runtime exceeded its control budget"
                    if responsive
                    else "Sync runtime did not acknowledge cancellation"
                )
            if command.error is not None:
                raise command.error
            return command.value
        except (KeyboardInterrupt, SystemExit):
            command.cancelled.set()
            # If interruption preceded publication there is no worker operation to acknowledge.
            with self._handoff, self._queue.mutex:
                published = command in self._queue.queue or self.current_command is command
            if not published:
                command.done.set()
            command.done.wait(self.cancellation_grace)
            raise
        finally:
            if self._pending is command:
                self._active = False
                if command.done.is_set():
                    self._pending = None

    def stop(self) -> None:
        """Request worker-owned cleanup and join within the control budget.

        :raises SyncRuntimeUnresponsiveError: If owned work cannot stop in time.
        """
        self.guard()
        self._stop.set()
        if self.thread is not None:
            self.thread.join(self.control_timeout)
            if self.thread.is_alive():
                self.abandoned = True
                raise SyncRuntimeUnresponsiveError("Sync runtime thread did not stop")
