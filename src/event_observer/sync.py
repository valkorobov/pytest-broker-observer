"""Owner-thread blocking facade over the same continuously running async core."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import os
import threading
from typing import TYPE_CHECKING, Generic, Sequence, TypeVar, overload

from .config import (
    DecodingPolicy,
    ObservationIdentity,
    JournalPolicy,
    LifecyclePolicy,
    RuntimeLimits,
    TransportBufferLimits,
)
from .decoders import Decoder, PayloadAccess
from .diagnostics import DiagnosticPolicy, DiagnosticReporter
from .errors import (
    ConfigurationError,
    ObserverClosedError,
    ObserverFailedError,
    ProcessAffinityError,
    ReentrantSyncCallError,
    SyncAPIInAsyncContextError,
    SyncRuntimeUnresponsiveError,
    ThreadAffinityError,
)
from .models import ObservedEvent, ObservationMark, StartPosition, TransportSource
from .observer import EventObserver, EventScope, EventWatcher
from .transports.base import TransportAdapter
from ._runtime.clock import Clock

if TYPE_CHECKING:
    from .transports.kafka import KafkaConfig

T = TypeVar("T")


class _Portal:
    def __init__(self, control_timeout: float, cancellation_grace: float) -> None:
        self.owner = threading.get_ident()
        self.pid = os.getpid()
        self.control_timeout = control_timeout
        self.cancellation_grace = cancellation_grace
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.running = False
        self._active = False
        self.abandoned = False

    def guard(self) -> None:
        if os.getpid() != self.pid:
            raise ProcessAffinityError("Sync watcher belongs to another process")
        if self.thread is not None and threading.get_ident() == self.thread.ident:
            raise ReentrantSyncCallError("A runtime callback cannot call its blocking watcher")
        if threading.get_ident() != self.owner:
            raise ThreadAffinityError("Sync watcher belongs to its creating thread")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise SyncAPIInAsyncContextError("Use EventObserver in an active asynchronous context")

    def start(self) -> None:
        self.guard()
        ready = threading.Event()
        context = contextvars.copy_context()

        def run() -> None:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.running = True
            ready.set()
            try:
                self.loop.run_forever()
            finally:
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
                self.loop.close()
                self.running = False

        self.thread = threading.Thread(
            target=lambda: context.run(run), name="event-observer-runtime", daemon=True
        )
        self.thread.start()
        if not ready.wait(self.control_timeout):
            raise SyncRuntimeUnresponsiveError("Sync runtime did not start within the control budget")

    def call(self, operation, *, budget: float | None = None):
        self.guard()
        if not self.running or self.loop is None or self._active:
            raise SyncRuntimeUnresponsiveError("Sync runtime is unavailable or already handling a command")
        self._active = True
        acknowledged = threading.Event()

        async def command():
            try:
                return await operation()
            finally:
                acknowledged.set()

        future = asyncio.run_coroutine_threadsafe(command(), self.loop)
        try:
            return future.result(timeout=budget or self.control_timeout)
        except (concurrent.futures.TimeoutError, KeyboardInterrupt) as exc:
            future.cancel()
            responsive = acknowledged.wait(self.cancellation_grace)
            if isinstance(exc, KeyboardInterrupt):
                raise
            raise SyncRuntimeUnresponsiveError(
                "Sync runtime exceeded its control budget"
                if responsive
                else "Sync runtime did not acknowledge cancellation"
            ) from exc
        finally:
            self._active = False

    def stop(self) -> None:
        self.guard()
        if self.running and self.loop is not None:

            async def drain():
                pending = {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}
                if pending:
                    _, pending = await asyncio.wait(pending, timeout=self.control_timeout)
                if pending:
                    for task in pending:
                        task.cancel()
                    _, pending = await asyncio.wait(pending, timeout=self.cancellation_grace)
                if pending:
                    raise SyncRuntimeUnresponsiveError("Sync runtime still owns unfinished operations")

            try:
                self.call(drain, budget=self.control_timeout + self.cancellation_grace + 0.5)
            except BaseException:
                self.abandoned = True
                # Leave an unresponsive daemon alive rather than close a loop with live resources.
                raise
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self.thread is not None:
            self.thread.join(self.control_timeout)
            if self.thread.is_alive():
                raise SyncRuntimeUnresponsiveError("Sync runtime thread did not stop")


class SyncEventObserver:
    def __init__(
        self,
        *,
        transport: TransportAdapter | KafkaConfig,
        journal: JournalPolicy | None = None,
        lifecycle: LifecyclePolicy | None = None,
        buffers: TransportBufferLimits | None = None,
        runtime_limits: RuntimeLimits | None = None,
        diagnostics: DiagnosticPolicy | None = None,
        reporters: Sequence[DiagnosticReporter] = (),
        clock: Clock | None = None,
    ) -> None:
        self._factory = EventObserver(
            transport=transport,
            journal=journal,
            lifecycle=lifecycle,
            buffers=buffers,
            runtime_limits=runtime_limits,
            diagnostics=diagnostics,
            reporters=reporters,
            clock=clock,
        )

    @overload
    def watch(
        self,
        *,
        source: str | TransportSource,
        message_type: type[T],
        decoder: None = None,
        payload_access: PayloadAccess[T] | None = None,
        start: StartPosition | None = None,
        decoding: DecodingPolicy | None = None,
        identity: ObservationIdentity | None = None,
    ) -> SyncEventWatcher[T]: ...

    @overload
    def watch(
        self,
        *,
        source: str | TransportSource,
        decoder: Decoder[T],
        message_type: None = None,
        payload_access: PayloadAccess[T] | None = None,
        start: StartPosition | None = None,
        decoding: DecodingPolicy | None = None,
        identity: ObservationIdentity | None = None,
    ) -> SyncEventWatcher[T]: ...

    @overload
    def watch(
        self,
        *,
        source: str | TransportSource,
        message_type: None = None,
        decoder: None = None,
        payload_access: PayloadAccess[bytes | None] | None = None,
        start: StartPosition | None = None,
        decoding: DecodingPolicy | None = None,
        identity: ObservationIdentity | None = None,
    ) -> SyncEventWatcher[bytes | None]: ...

    def watch(self, **kwargs):
        return SyncEventWatcher(self._factory.watch(**kwargs))


class _SyncDiagnostics:
    def __init__(self, watcher: SyncEventWatcher) -> None:
        self._watcher = watcher

    def snapshot(self):
        owner = self._watcher
        owner._portal.guard()
        if not owner._portal.running:
            return owner._cached_diagnostics
        return owner._call(lambda: owner._async._diagnostics.snapshot(), report=False)

    def export(self):
        owner = self._watcher
        owner._portal.guard()
        if not owner._portal.running:
            return owner._cached_export
        return owner._call(lambda: owner._async._diagnostics.export(), report=False)


class SyncEventScope(Generic[T]):
    def __init__(self, watcher: SyncEventWatcher[T], scope: EventScope[T] | EventWatcher[T]) -> None:
        self._owner = watcher
        self._scope = scope

    @property
    def diagnostics(self):
        return self._owner.diagnostics

    def scope(self, matcher, *, name=None, start=None) -> SyncEventScope[T]:
        result = self._owner._call(lambda: self._scope.scope(matcher, name=name, start=start), report=False)
        return SyncEventScope(self._owner, result)

    def snapshot(self):
        return self._owner._call(self._scope.snapshot, report=False)

    def mark(self) -> ObservationMark:
        self._owner._portal.guard()
        if not self._owner._portal.running:
            if self._owner._terminal_error is not None:
                raise ObserverFailedError(
                    "Observer background operation failed",
                    observer_id=self._owner.observer_id,
                    cause=self._owner._terminal_error,
                    diagnostics=self._owner._cached_diagnostics,
                ) from self._owner._terminal_error
            raise ObserverClosedError("Watcher is not open", observer_id=self._owner.observer_id)
        return self._owner._call(self._scope.mark, report=False)

    def _expect(
        self, matcher, count, timeout, description, require_complete, since
    ) -> list[ObservedEvent[T]]:
        self._owner._portal.guard()
        # The admission deadline starts before the command enters the runtime queue.
        from .config import positive

        positive(timeout, "timeout")
        deadline = self._owner._async._clock.monotonic() + timeout

        async def expect():
            return await self._owner._async._expect(
                self._scope,
                matcher,
                count,
                timeout,
                description,
                require_complete,
                absolute_deadline=deadline,
                since=since,
            )

        return self._owner._dispatch(expect, budget=timeout + self._owner._portal.control_timeout)

    def wait_for(
        self,
        matcher,
        *,
        timeout,
        description=None,
        require_complete=True,
        since: ObservationMark | None = None,
    ) -> T:
        return self._expect(matcher, 1, timeout, description, require_complete, since)[0].payload

    def wait_for_event(
        self,
        matcher,
        *,
        timeout,
        description=None,
        require_complete=True,
        since: ObservationMark | None = None,
    ) -> ObservedEvent[T]:
        return self._expect(matcher, 1, timeout, description, require_complete, since)[0]

    def wait_for_count(
        self,
        matcher,
        count,
        *,
        timeout,
        description=None,
        require_complete=True,
        since: ObservationMark | None = None,
    ) -> list[T]:
        return [
            e.payload for e in self._expect(matcher, count, timeout, description, require_complete, since)
        ]


class SyncEventWatcher(SyncEventScope[T]):
    def __init__(self, watcher: EventWatcher[T]) -> None:
        self._async = watcher
        policy = watcher.factory.lifecycle
        self._portal = _Portal(policy.sync_control_timeout, policy.cancellation_grace)
        self._entered = False
        self._cached_snapshot = None
        self._cached_diagnostics = None
        self._cached_export = None
        self._terminal_error = None
        self._diagnostics_proxy = _SyncDiagnostics(self)
        super().__init__(self, watcher)
        watcher._defer_reporting = True

    @property
    def observer_id(self) -> str:
        return self._async.observer_id

    @property
    def diagnostics(self):
        return self._diagnostics_proxy

    @property
    def readiness(self):
        return self._async.readiness

    def _dispatch(self, operation, *, budget=None, report=True, force_report=False):
        error = None
        try:
            return self._portal.call(operation, budget=budget)
        except BaseException as exc:
            error = exc
            raise
        finally:
            if report and self._portal.running and self._async.reporters:

                async def prepare_report():
                    return self._async._diagnostics.prepare_report(force=force_report or error is not None)

                try:
                    exported = self._portal.call(prepare_report)
                    if exported is not None:
                        for reporter in self._async.reporters:
                            try:
                                reporter.emit(exported)
                            except Exception:

                                async def record_reporter_error():
                                    self._async._diagnostics.record(
                                        "reporter_error", error_code="reporter_failed"
                                    )
                                    return self._async._diagnostics.snapshot()

                                self._cached_diagnostics = self._portal.call(record_reporter_error)
                except Exception:
                    pass

    def _call(self, operation, *, report=True):
        async def call():
            return operation()

        return self._dispatch(call, report=report)

    def __enter__(self) -> SyncEventWatcher[T]:
        self._portal.guard()
        if self._entered:
            raise ConfigurationError("A watch context can be entered only once")
        self._entered = True
        try:
            self._portal.start()
            self._dispatch(
                self._async.__aenter__,
                budget=self._async.factory.lifecycle.startup_timeout + self._portal.control_timeout,
            )
            return self
        except BaseException as exc:
            try:
                self._portal.stop()
            except BaseException:
                if hasattr(exc, "add_note"):
                    exc.add_note("Sync startup cleanup could not finish within its resource budget")
            raise

    def snapshot(self):
        self._portal.guard()
        if not self._portal.running:
            return self._cached_snapshot
        return self._call(self._async.snapshot, report=False)

    def _finish(self, exc_type=None, exc_value=None, traceback=None) -> None:
        self._portal.guard()
        if not self._portal.running:
            if exc_type is not False and exc_value is None and self._terminal_error is not None:
                raise ObserverFailedError(
                    "Observer background operation failed", cause=self._terminal_error
                ) from self._terminal_error
            return

        async def finish():
            try:
                if exc_type is False:
                    await self._async.close()
                else:
                    await self._async.__aexit__(exc_type, exc_value, traceback)
            finally:
                self._terminal_error = self._async.terminal_error
                try:
                    self._cached_snapshot = self._async.snapshot()
                    self._cached_diagnostics = self._async._diagnostics.snapshot()
                except Exception:
                    self._async._diagnostics.record("diagnostic_cache_failure")

        try:
            self._dispatch(
                finish,
                budget=self._async.factory.lifecycle.shutdown_timeout + self._portal.control_timeout,
                force_report=exc_value is not None,
            )
        finally:
            try:
                # Automatic reports have priority over the optional full
                # post-close export cache in the remaining byte budget.
                if self._portal.running:

                    async def cache_export():
                        try:
                            return self._async._diagnostics.export()
                        except Exception:
                            self._async._diagnostics.record("diagnostic_cache_failure")
                            return None

                    try:
                        self._cached_export = self._portal.call(cache_export)
                    except Exception:
                        pass
            finally:
                try:
                    self._portal.stop()
                except BaseException:
                    if exc_value is None:
                        raise

    def close(self) -> None:
        self._finish(False)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._finish(exc_type, exc_value, traceback)
