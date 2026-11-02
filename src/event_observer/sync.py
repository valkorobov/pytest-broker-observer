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

#: Caller interruption signals take precedence over ordinary cleanup failures.
_INTERRUPTIONS = (KeyboardInterrupt, SystemExit, asyncio.CancelledError, concurrent.futures.CancelledError)


class _Portal:
    """Serialize owner-thread commands onto one bounded background event loop.

    :ivar owner: Thread identity permitted to use blocking operations.
    :ivar pid: Process identity captured at construction.
    :ivar control_timeout: Default command and thread-control budget in seconds.
    :ivar cancellation_grace: Budget for actual task-cancellation acknowledgement.
    :ivar loop: Background event loop after startup.
    :ivar thread: Daemon thread owning the background loop.
    :ivar running: Whether the background loop is running.
    :ivar _active: Whether the owner thread is waiting on a command.
    :ivar _pending: Actual-completion signal for the last submitted command.
    :ivar abandoned: Whether stopping left unresponsive runtime work outstanding.
    """

    def __init__(self, control_timeout: float, cancellation_grace: float) -> None:
        """Capture caller affinity and initialize an idle, unopened runtime bridge.

        :param control_timeout: Default command-control and thread-join budget in seconds.
        :param cancellation_grace: Budget in seconds for actual task-cancellation acknowledgement.
        """
        self.owner = threading.get_ident()
        self.pid = os.getpid()
        self.control_timeout = control_timeout
        self.cancellation_grace = cancellation_grace
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.running = False
        self._active = False
        self._pending: threading.Event | None = None
        self.abandoned = False

    def guard(self) -> None:
        """Require the creating process and thread outside an active async context.

        :raises ThreadAffinityError: If access occurs outside the creating thread.
        :raises ProcessAffinityError: If access occurs outside the creating process.
        :raises ReentrantSyncCallError: If a runtime callback calls its blocking facade.
        :raises SyncAPIInAsyncContextError: If the caller already runs an async event loop.
        """
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
        """Start the owned event loop with a bounded readiness handshake.

        :raises SyncRuntimeUnresponsiveError: If runtime startup is not acknowledged within its control
            budget.
        """
        self.guard()
        ready = threading.Event()
        context = contextvars.copy_context()

        def run() -> None:
            """Own the background loop and finish async-generator cleanup before closing it."""
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

    def _prepare_command(self, operation, acknowledged: threading.Event):
        """Prepare cancellation before handing a command to the runtime.

        Cancelling the caller's Future does not prove runtime completion. The
        task callback also acknowledges cancellation before its first step.

        :param operation: Factory for a coroutine owned by the runtime loop.
        :param acknowledged: Signal set only after completion or skipped submission.
        :returns: The cancellable result Future and a callable that publishes the command.
        """
        assert self.loop is not None
        loop = self.loop
        result: concurrent.futures.Future = concurrent.futures.Future()
        task: asyncio.Task | None = None

        def submit() -> None:
            """Create the task unless its queued submission was already cancelled."""
            nonlocal task
            if result.cancelled():
                acknowledged.set()
                return

            async def invoke():
                """Skip cancelled submissions before invoking user code on the owner loop.

                :returns: The operation result, or None when the caller already cancelled.
                """
                if result.cancelled():
                    return None
                return await operation()

            task = loop.create_task(invoke())

            def completed(task: asyncio.Task) -> None:
                """Transfer a finished task's result and release its in-flight slot.

                :param task: Runtime task, including one cancelled before execution.
                """
                acknowledged.set()
                try:
                    if task.cancelled():
                        result.cancel()
                    elif (error := task.exception()) is not None:
                        result.set_exception(error)
                    else:
                        result.set_result(task.result())
                except concurrent.futures.InvalidStateError:
                    # A caller can cancel concurrently with result delivery.
                    pass

            task.add_done_callback(completed)

        def cancel_on_loop() -> None:
            """Cancel a created task, or acknowledge that no operation was submitted."""
            if task is None:
                acknowledged.set()
            else:
                task.cancel()

        def cancel(future: concurrent.futures.Future) -> None:
            """Forward cancellation, including an interruption before command publication.

            :param future: Caller Future whose cancellation requests runtime cleanup.
            """
            if future.cancelled():
                loop.call_soon_threadsafe(cancel_on_loop)

        def enqueue() -> None:
            """Publish the prepared command after the caller retains its cancellation handle."""
            loop.call_soon_threadsafe(submit)

        result.add_done_callback(cancel)
        return result, enqueue

    def call(self, operation, *, budget: float | None = None):
        """Run one command within its control budget and await actual cancellation acknowledgement.

        :param operation: Callable producing a coroutine to execute on the owner runtime.
        :param budget: Optional command-control budget in seconds.
        :returns: The command result after its runtime operation completes.
        :raises SyncRuntimeUnresponsiveError: If a command remains pending, the runtime is unavailable,
            or its control budget expires.
        """
        self.guard()
        if (
            not self.running
            or self.loop is None
            or self._active
            or (self._pending is not None and not self._pending.is_set())
        ):
            raise SyncRuntimeUnresponsiveError("Sync runtime is unavailable or already handling a command")
        acknowledged = threading.Event()
        future, enqueue = self._prepare_command(operation, acknowledged)
        try:
            self._active = True
            self._pending = acknowledged
            enqueue()
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
            if acknowledged.is_set():
                self._pending = None

    def stop(self) -> None:
        """Drain owned tasks and stop the thread without hiding abandoned work.

        :raises SyncRuntimeUnresponsiveError: If unfinished resources or the runtime thread cannot stop
            in time.
        """
        self.guard()
        if self.running and self.loop is not None:

            async def drain():
                """Await then cancel unfinished runtime tasks within the available control budgets."""
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
    """Create blocking observation contexts over the shared async engine.

    :ivar _factory: Lazy async factory containing the same policies and adapters.
    """

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
        """Retain the shared factory policies without starting a thread or broker session.

        :param transport: Adapter or supported Kafka configuration for independent observation.
        :param journal: Retained-history limits and overflow policy; defaults to JournalPolicy.
        :param lifecycle: Startup, shutdown, and command-control budgets.
        :param buffers: Bounded event and byte limits for transport handoff.
        :param runtime_limits: Scope, waiter, result, and cooperative-scan limits.
        :param diagnostics: Disclosure and bounded diagnostic-reporting policy.
        :param reporters: Reporter prototypes bound independently for each watcher.
        :param clock: Shared monotonic and UTC clock; defaults to the system clock.
        """
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
    ) -> SyncEventWatcher[T]:
        """Construct a single-use blocking watcher without performing broker I/O.

        :param source: Broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Independent-copy and accounted-size strategy for decoded payloads.
        :param start: Native transport start position; defaults to latest.
        :param decoding: Policy for unknown message types and decoding failures.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A lazy blocking watcher with the selected payload type.
        """
        ...

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
    ) -> SyncEventWatcher[T]:
        """Construct a single-use blocking watcher without performing broker I/O.

        :param source: Broker source name or normalized transport source.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param payload_access: Independent-copy and accounted-size strategy for decoded payloads.
        :param start: Native transport start position; defaults to latest.
        :param decoding: Policy for unknown message types and decoding failures.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A lazy blocking watcher with the selected payload type.
        """
        ...

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
    ) -> SyncEventWatcher[bytes | None]:
        """Construct a single-use blocking watcher without performing broker I/O.

        :param source: Broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Independent-copy and accounted-size strategy for decoded payloads.
        :param start: Native transport start position; defaults to latest.
        :param decoding: Policy for unknown message types and decoding failures.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A lazy blocking watcher with the selected payload type.
        """
        ...

    def watch(self, **kwargs):
        """Construct a single-use blocking watcher without performing broker I/O.

        :param kwargs: Watcher arguments forwarded unchanged to the shared async factory.
        :returns: A lazy blocking watcher with the selected payload type.
        """
        return SyncEventWatcher(self._factory.watch(**kwargs))


class _SyncDiagnostics:
    """Expose detached runtime diagnostics and cached values after shutdown.

    :ivar _watcher: Sync watcher whose portal owns collector access.
    """

    def __init__(self, watcher: SyncEventWatcher) -> None:
        """Associate cached diagnostic access with its owning sync watcher.

        :param watcher: Watcher owning the runtime and shared Journal.
        """
        self._watcher = watcher

    def snapshot(self):
        """Read a detached diagnostic snapshot or its cache after shutdown.

        :returns: The safe snapshot, or the cached value after runtime shutdown.
        """
        owner = self._watcher
        owner._portal.guard()
        if not owner._portal.running:
            return owner._cached_diagnostics
        return owner._call(lambda: owner._async._diagnostics.snapshot(), report=False)

    def export(self):
        """Export retained diagnostics or return the full export cached at shutdown.

        :returns: The policy-controlled export, or its post-close cached value.
        """
        owner = self._watcher
        owner._portal.guard()
        if not owner._portal.running:
            return owner._cached_export
        return owner._call(lambda: owner._async._diagnostics.export(), report=False)


class SyncEventScope(Generic[T]):
    """Provide owner-thread blocking expectations for one async view.

    :ivar _owner: Sync watcher owning the portal and post-close caches.
    :ivar _scope: Underlying async scope or watcher.
    :ivar diagnostics: Safe diagnostics proxy shared with the watcher.
    """

    def __init__(self, watcher: SyncEventWatcher[T], scope: EventScope[T] | EventWatcher[T]) -> None:
        """Associate an async search view with the watcher that owns its portal.

        :param watcher: Watcher owning the runtime and shared Journal.
        :param scope: Async scope or watcher exposed through this blocking view.
        """
        self._owner = watcher
        self._scope = scope

    @property
    def diagnostics(self):
        """Return the shared safe diagnostics proxy.

        :returns: The shared owner-thread diagnostic proxy.
        """
        return self._owner.diagnostics

    def scope(self, matcher, *, name=None, start=None) -> SyncEventScope[T]:
        """Create a child correlation scope on the owner runtime.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param name: Optional caller-defined scope label.
        :param start: Local Journal boundary intersected with the parent scope definition.
        :returns: A blocking child scope over the same observation.
        """
        result = self._owner._call(lambda: self._scope.scope(matcher, name=name, start=start), report=False)
        return SyncEventScope(self._owner, result)

    def snapshot(self):
        """Copy the underlying scope definition on the owner runtime.

        :returns: A detached scope-definition snapshot.
        """
        return self._owner._call(self._scope.snapshot, report=False)

    def mark(self) -> ObservationMark:
        """Capture a runtime admission boundary before returning to the caller thread.

        :returns: A mark captured by the runtime before this call returns.
        :raises ObserverClosedError: If the watcher is not open.
        :raises ObserverFailedError: If a cached or active terminal observation failure is known.
        """
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
        """Dispatch an expectation whose admission deadline starts at public-call entry.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param count: Positive number of distinct matching Journal events to return.
        :param timeout: Positive finite admission-window duration in seconds.
        :param description: Reserved expectation label; currently not recorded or exported.
        :param require_complete: Whether any known retention loss in the required range must fail.
        :param since: Reusable mark from this watcher intersected with the view boundary.
        :returns: Detached matching events returned by the shared expectation engine.
        """
        self._owner._portal.guard()
        # The admission deadline starts before the command enters the runtime queue.
        from .config import positive

        positive(timeout, "timeout")
        deadline = self._owner._async._clock.monotonic() + timeout

        async def expect():
            """Execute the shared expectation engine with the caller-computed deadline.

            :returns: Matching events after shared deadline and completeness checks.
            """
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
        """Block until the first matching independent payload is available.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param timeout: Positive finite admission-window duration in seconds.
        :param description: Reserved expectation label; currently not recorded or exported.
        :param require_complete: Whether any known retention loss in the required range must fail.
        :param since: Reusable mark from this watcher intersected with the view boundary.
        :returns: An independent copy of the first matching payload.
        :raises EventTimeoutError: If the required matches are not admitted before the deadline.
        :raises IncompleteObservationError: If completeness is required and the range contains known
            loss.
        :raises ObserverFailedError: If a terminal observation failure is known.
        """
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
        """Block until a matching detached event is available.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param timeout: Positive finite admission-window duration in seconds.
        :param description: Reserved expectation label; currently not recorded or exported.
        :param require_complete: Whether any known retention loss in the required range must fail.
        :param since: Reusable mark from this watcher intersected with the view boundary.
        :returns: A detached matching event and payload.
        :raises EventTimeoutError: If the required matches are not admitted before the deadline.
        :raises IncompleteObservationError: If completeness is required and the range contains known
            loss.
        :raises ObserverFailedError: If a terminal observation failure is known.
        """
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
        """Block until distinct matching events provide the requested payload count.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param count: Positive number of distinct matching Journal events to return.
        :param timeout: Positive finite admission-window duration in seconds.
        :param description: Reserved expectation label; currently not recorded or exported.
        :param require_complete: Whether any known retention loss in the required range must fail.
        :param since: Reusable mark from this watcher intersected with the view boundary.
        :returns: Independent payloads from the requested distinct matching events.
        :raises EventTimeoutError: If the required matches are not admitted before the deadline.
        :raises IncompleteObservationError: If completeness is required and the range contains known
            loss.
        :raises ObserverFailedError: If a terminal observation failure is known.
        """
        return [
            e.payload for e in self._expect(matcher, count, timeout, description, require_complete, since)
        ]


class SyncEventWatcher(SyncEventScope[T]):
    """Manage one continuously running observer from its creating thread.

    :ivar _async: Underlying async watcher that owns transport and Journal state.
    :ivar _portal: Serialized command bridge to the background event loop.
    :ivar _entered: Whether this single-use context has been entered.
    :ivar _cached_snapshot: Detached Journal snapshot retained after shutdown.
    :ivar _cached_diagnostics: Detached diagnostic snapshot retained after shutdown.
    :ivar _cached_export: Optional full retained export cached after automatic reporting.
    :ivar _terminal_error: Cached primary background failure after finalization.
    :ivar _finish_error: Secondary failure of the dispatched cleanup command, if any.
    :ivar _stop_error: Secondary runtime-stop failure retained without replacing a primary error.
    :ivar _diagnostics_proxy: Owner-thread proxy for safe collector reads.
    :ivar observer_id: Identity shared with the async watcher.
    :ivar diagnostics: Collector proxy supporting cached post-close reads.
    :ivar readiness: Confirmed readiness of the underlying transport.
    """

    def __init__(self, watcher: EventWatcher[T]) -> None:
        """Wrap a lazy async watcher and assign caller-thread reporting to the facade.

        :param watcher: Watcher owning the runtime and shared Journal.
        """
        self._async = watcher
        policy = watcher.factory.lifecycle
        self._portal = _Portal(policy.sync_control_timeout, policy.cancellation_grace)
        self._entered = False
        self._cached_snapshot = None
        self._cached_diagnostics = None
        self._cached_export = None
        self._terminal_error = None
        self._finish_error: BaseException | None = None
        self._stop_error: BaseException | None = None
        self._diagnostics_proxy = _SyncDiagnostics(self)
        super().__init__(self, watcher)
        watcher._defer_reporting = True

    @property
    def observer_id(self) -> str:
        """Return the identity shared by the async watcher and its events.

        :returns: The shared observation identity.
        """
        return self._async.observer_id

    @property
    def diagnostics(self):
        """Return the owner-thread safe diagnostic proxy.

        :returns: The owner-thread diagnostics proxy.
        """
        return self._diagnostics_proxy

    @property
    def readiness(self):
        """Return transport readiness confirmed by the shared async watcher.

        :returns: The confirmed transport readiness, or None before startup.
        """
        return self._async.readiness

    def _dispatch(self, operation, *, budget=None, report=True, force_report=False):
        """Run a command and deliver newly prepared reports in the caller thread.

        :param operation: Callable producing a coroutine to execute on the owner runtime.
        :param budget: Optional command-control budget in seconds.
        :param report: Whether to deliver new reports after the command completes.
        :param force_report: Whether to prepare a report even without a newly completed expectation.
        :returns: The runtime command result.
        """
        error = None
        try:
            return self._portal.call(operation, budget=budget)
        except BaseException as exc:
            error = exc
            raise
        finally:
            if report and self._portal.running and self._async.reporters:

                async def prepare_report():
                    """Prepare new results or failures on the collector owner loop.

                    :returns: A new bounded export, or None when no report is pending.
                    """
                    return self._async._diagnostics.prepare_report(force=force_report or error is not None)

                try:
                    exported = self._portal.call(prepare_report)
                    if exported is not None:
                        for reporter in self._async.reporters:
                            try:
                                reporter.emit(exported)
                            except Exception:

                                async def record_reporter_error():
                                    """Account for a callback failure on the collector owner loop.

                                    :returns: The updated safe diagnostic snapshot.
                                    """
                                    self._async._diagnostics.record_reporter_error()
                                    return self._async._diagnostics.snapshot()

                                self._cached_diagnostics = self._portal.call(record_reporter_error)
                except Exception:
                    pass

    def _call(self, operation, *, report=True):
        """Dispatch a synchronous owner-loop operation through the command bridge.

        :param operation: Synchronous owner-loop callable to invoke.
        :param report: Whether to deliver new reports after the command completes.
        :returns: The value returned by the synchronous operation.
        """

        async def call():
            """Invoke the supplied synchronous operation on the runtime loop.

            :returns: The synchronous operation result.
            """
            return operation()

        return self._dispatch(call, report=report)

    def __enter__(self) -> SyncEventWatcher[T]:
        """Start the background runtime and wait for independent observation readiness.

        :returns: This watcher after startup readiness.
        :raises ConfigurationError: If the context was already entered.
        :raises ObserverStartupTimeoutError: If transport startup exceeds its budget.
        :raises SyncRuntimeUnresponsiveError: If the owned runtime cannot start or respond.
        """
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
            self._stop_runtime(exc)
            raise

    def snapshot(self):
        """Read a detached Journal snapshot or its post-close cache.

        :returns: The detached Journal snapshot, or its post-close cached value.
        """
        self._portal.guard()
        if not self._portal.running:
            return self._cached_snapshot
        return self._call(self._async.snapshot, report=False)

    def _stop_runtime(self, primary_error: BaseException | None) -> None:
        """Stop the portal while retaining an existing error as the primary failure.

        :param primary_error: Operation or context error already being propagated.
        :raises BaseException: The stop error when no primary error exists, or a new caller interruption.
        """
        try:
            self._portal.stop()
        except BaseException as error:
            self._stop_error = error
            if primary_error is None or isinstance(error, _INTERRUPTIONS):
                raise
            primary_error.add_note("Sync runtime cleanup also failed; inspect the watcher's cleanup state")

    def _finish(self, exc_type=None, exc_value=None, traceback=None) -> None:
        """Finalize the async context, cache safe diagnostics, and stop the runtime.

        A new caller interruption propagates after bounded cleanup even when
        the context body already raised an ordinary exception.

        :param exc_type: Context exception type, or False for explicit close without context semantics.
        :param exc_value: Primary context exception, if any.
        :param traceback: Original traceback supplied by the context manager protocol.
        """
        self._portal.guard()
        if not self._portal.running:
            if exc_type is not False and exc_value is None and self._terminal_error is not None:
                raise ObserverFailedError(
                    "Observer background operation failed", cause=self._terminal_error
                ) from self._terminal_error
            return

        async def finish():
            """Close the shared watcher and cache its terminal state on the owner loop."""
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

        primary_error = exc_value
        try:
            self._dispatch(
                finish,
                budget=self._async.factory.lifecycle.shutdown_timeout + self._portal.control_timeout,
                force_report=exc_value is not None,
            )
        except BaseException as error:
            primary_error = exc_value if exc_value is not None else error
            self._finish_error = error
            if exc_value is None or isinstance(error, _INTERRUPTIONS):
                primary_error = error
                raise
            exc_value.add_note("Observer cleanup command also failed; inspect the watcher's cleanup state")
        finally:
            try:
                # Automatic reports have priority over the optional full
                # post-close export cache in the remaining byte budget.
                if self._portal.running:

                    async def cache_export():
                        """Cache a full retained export after automatic reports have priority.

                        :returns: The retained diagnostic export, or None if its budget is exhausted.
                        """
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
                self._stop_runtime(primary_error)

    def close(self) -> None:
        """Close owned observation resources without synthesizing a context failure."""
        self._finish(False)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Finalize a context while keeping its primary exception intact.

        :param exc_type: Context exception type, or False for explicit close without context semantics.
        :param exc_value: Primary context exception, if any.
        :param traceback: Original traceback supplied by the context manager protocol.
        """
        self._finish(exc_type, exc_value, traceback)
