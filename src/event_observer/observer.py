"""Shared asynchronous observation runtime. All state belongs to one event loop."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from enum import Enum
import inspect
import os
from typing import TYPE_CHECKING, Any, Generic, Sequence, TypeVar, cast, overload
from uuid import uuid4

from ._runtime.clock import Clock, SystemClock

from .config import (
    DecodeErrorPolicy,
    DecodingPolicy,
    JournalPolicy,
    LifecyclePolicy,
    ObservationIdentity,
    RuntimeLimits,
    TransportBufferLimits,
    positive,
)
from .decoders import Decoder, DefaultPayloadAccess, PayloadAccess, ProtobufDecoder, RawDecoder
from .diagnostics import DiagnosticCollector, DiagnosticPolicy, DiagnosticReporter
from .errors import (
    ConfigurationError,
    EventDecodingError,
    EventLoopMismatchError,
    EventTimeoutError,
    ExpectationResourceLimitError,
    IncompleteObservationError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    ObserverClosedError,
    ObserverFailedError,
    ObserverStartupTimeoutError,
    PayloadIsolationError,
    ProcessAffinityError,
    ScopeResourceLimitError,
    TransportError,
    UnknownMessageTypeError,
    UnexpectedStreamEndError,
    UnsupportedCapabilityError,
    InvalidMatcherResultError,
    MatcherEvaluationError,
    EventObserverError,
    EventAssertionError,
)
from .journal import EventJournal, JournalSnapshot
from .matchers import EventMatcher, MatchStatus, bind_matcher
from .models import (
    ObservedEvent,
    OpenContext,
    ScopeStart,
    ScopeStartMode,
    StartPosition,
    TransportSource,
)
from .trace import extract_trace_context
from .transports.base import DeliveryOutcome, TransportAdapter, TransportSession

T = TypeVar("T")

if TYPE_CHECKING:
    from .transports.kafka import KafkaConfig


class LifecycleState(str, Enum):
    CREATED = "created"
    STARTING = "starting"
    READY = "ready"
    FAILED = "failed"
    STOPPING = "stopping"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class ScopeSnapshot:
    observer_id: str
    scope_id: str
    lower_bound: int
    complete: bool
    expectations: tuple[Any, ...]
    name: str | None = None
    filter_count: int = 0


class EventObserver:
    """A lazy, reusable factory. watch() performs no I/O."""

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
        # Isolated convenience layer: importing Core does not import Kafka's client.
        if not hasattr(transport, "open"):
            from .transports.kafka import KafkaAdapter, KafkaConfig

            if isinstance(transport, KafkaConfig):
                transport = KafkaAdapter(transport)
            else:
                raise ConfigurationError("transport must implement TransportAdapter")
        self.transport: TransportAdapter = cast(TransportAdapter, transport)
        self.journal = journal or JournalPolicy()
        self.lifecycle = lifecycle or LifecyclePolicy()
        self.buffers = buffers or TransportBufferLimits()
        self.runtime_limits = runtime_limits or RuntimeLimits()
        self.diagnostic_policy = diagnostics or DiagnosticPolicy()
        self.reporters = tuple(reporters)
        self.clock = clock or SystemClock()
        if self.journal.limits.max_event_bytes > self.buffers.max_bytes:
            raise ConfigurationError("max_event_bytes cannot exceed transport handoff bytes")

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
    ) -> EventWatcher[T]: ...

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
    ) -> EventWatcher[T]: ...

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
    ) -> EventWatcher[bytes | None]: ...

    def watch(
        self,
        *,
        source,
        message_type=None,
        decoder=None,
        payload_access=None,
        start=None,
        decoding=None,
        identity=None,
    ) -> EventWatcher:
        if message_type is not None and decoder is not None:
            raise ConfigurationError("Specify either message_type or decoder")
        chosen_decoder = (
            decoder
            if decoder is not None
            else (ProtobufDecoder(message_type) if message_type is not None else RawDecoder())
        )
        return EventWatcher(
            self,
            source if isinstance(source, TransportSource) else TransportSource(source),
            chosen_decoder,
            payload_access or DefaultPayloadAccess(),
            start or StartPosition.latest(),
            decoding or DecodingPolicy(),
            identity or ObservationIdentity(),
        )


class _EventView(Generic[T]):
    def __init__(
        self,
        watcher: EventWatcher[T],
        *,
        lower: int = 0,
        filters: tuple[EventMatcher, ...] = (),
        scope_id: str | None = None,
        name: str | None = None,
    ) -> None:
        self._watcher = watcher
        self._lower_bound = lower
        self._filters = filters
        self._scope_id = scope_id or uuid4().hex
        self._name = name

    @property
    def lower_bound(self) -> int:
        return self._lower_bound

    @property
    def scope_id(self) -> str:
        return self._scope_id

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def diagnostics(self) -> DiagnosticCollector:
        self._watcher._check_affinity()
        return self._watcher._diagnostics

    def scope(self, matcher, *, name: str | None = None, start: ScopeStart | None = None) -> EventScope[T]:
        watcher = self._watcher
        watcher._check_open()
        if watcher._scope_count >= watcher.factory.runtime_limits.max_scopes:
            raise ScopeResourceLimitError("Scope definition limit reached")
        start = start or ScopeStart.all_retained()
        lower = self.lower_bound
        if start.mode == ScopeStartMode.FROM_NOW:
            lower = max(lower, watcher._journal.next_sequence)
        elif start.mode == ScopeStartMode.FROM_SEQUENCE:
            if (
                start.sequence is None
                or start.sequence < 0
                or start.sequence > watcher._journal.next_sequence
            ):
                raise ConfigurationError("Scope sequence is outside this watcher's admitted range")
            lower = max(lower, start.sequence)
        bound = watcher._bind(matcher)
        watcher._scope_count += 1
        return EventScope(watcher, lower=lower, filters=(*self._filters, bound), name=name)

    async def wait_for(
        self, matcher, *, timeout: float, description: str | None = None, require_complete: bool = True
    ) -> T:
        results = await self._watcher._expect(self, matcher, 1, timeout, description, require_complete)
        return results[0].payload

    async def wait_for_event(
        self, matcher, *, timeout: float, description: str | None = None, require_complete: bool = True
    ) -> ObservedEvent[T]:
        results = await self._watcher._expect(self, matcher, 1, timeout, description, require_complete)
        return results[0]

    async def wait_for_count(
        self,
        matcher,
        count: int,
        *,
        timeout: float,
        description: str | None = None,
        require_complete: bool = True,
    ) -> list[T]:
        results = await self._watcher._expect(self, matcher, count, timeout, description, require_complete)
        return [event.payload for event in results]


class EventScope(_EventView[T]):
    def snapshot(self) -> ScopeSnapshot:
        watcher = self._watcher
        watcher._check_affinity()
        summaries = tuple(
            s
            for s in watcher._diagnostics.snapshot().expectations
            if getattr(s, "scope_id", None) == self.scope_id
        )
        return ScopeSnapshot(
            watcher.observer_id,
            self.scope_id,
            self.lower_bound,
            watcher._journal.complete(self.lower_bound),
            summaries,
            self.name,
            len(self._filters),
        )


class EventWatcher(_EventView[T]):
    def __init__(
        self,
        factory: EventObserver,
        source: TransportSource,
        decoder: Decoder[T],
        access: PayloadAccess[T],
        start: StartPosition,
        decoding: DecodingPolicy,
        identity: ObservationIdentity,
    ) -> None:
        self.factory = factory
        self._clock = factory.clock
        self.source = source
        self.decoder = decoder
        self.access = access
        self.start_position = start
        self.decoding = decoding
        self.identity = identity
        self.observer_id = uuid4().hex
        super().__init__(self)
        self.state = LifecycleState.CREATED
        self.cleanup_state = "not_started"
        self.terminal_error: BaseException | None = None
        self._journal = EventJournal(self.observer_id, factory.journal)
        self._diagnostics = DiagnosticCollector(self.observer_id, factory.diagnostic_policy)
        bound_reporters = []
        for reporter in factory.reporters:
            try:
                observation_binder = getattr(reporter, "bind_observation", None)
                if callable(observation_binder):
                    transport_name = type(factory.transport).__name__
                    aliases = factory.diagnostic_policy.aliases
                    bound_reporters.append(
                        observation_binder(
                            transport=aliases.get(transport_name, transport_name),
                            source=aliases.get(source.name, source.name),
                        )
                    )
                else:
                    binder = getattr(reporter, "bind", None)
                    bound_reporters.append(binder() if callable(binder) else reporter)
            except Exception:
                self._diagnostics.record("reporter_error", error_code="reporter_bind_failed")
        self.reporters: tuple[DiagnosticReporter, ...] = tuple(bound_reporters)
        self._session: TransportSession | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pid = os.getpid()
        self._changed: asyncio.Event | None = None
        self._tasks: list[asyncio.Task] = []
        self._close_task: asyncio.Task | None = None
        self._waiter_tasks: set[asyncio.Task] = set()
        self._scope_count = 0
        self._result_bytes = 0
        self._defer_reporting = False
        self.readiness = None

    def _check_affinity(self) -> None:
        if os.getpid() != self._pid:
            raise ProcessAffinityError("Watcher belongs to another process")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._loop is not None and loop is not self._loop:
            raise EventLoopMismatchError("Watcher belongs to another event loop")

    def _check_open(self) -> None:
        self._check_affinity()
        if self.terminal_error is not None:
            self._raise_terminal()
        if self.state != LifecycleState.READY:
            raise ObserverClosedError("Watcher is not ready or has closed", observer_id=self.observer_id)

    def _raise_terminal(self) -> None:
        error = ObserverFailedError(
            "Observer background operation failed",
            observer_id=self.observer_id,
            diagnostics=self._diagnostics.snapshot(),
            cause=self.terminal_error,
        )
        raise error from self.terminal_error

    def _notify(self) -> None:
        if self._changed is not None:
            self._changed.set()
        self._changed = asyncio.Event()

    def _fail(self, error: BaseException) -> None:
        if self.terminal_error is None:
            self.terminal_error = error
            self.state = LifecycleState.FAILED
            self._diagnostics.record(
                "terminal_failure",
                error_code=getattr(error, "code", "runtime_error"),
                error_type=type(error).__name__,
            )
            self._notify()

    def _copy_event(self, event: ObservedEvent[T]) -> ObservedEvent[T]:
        try:
            return replace(event, payload=self.access.clone_for_user(event.payload))
        except PayloadIsolationError:
            raise
        except Exception as exc:
            raise PayloadIsolationError("Payload isolation failed") from exc

    def _bind(self, matcher) -> EventMatcher:
        known = getattr(self.decoder, "known_message_types", None)
        if isinstance(self.decoder, RawDecoder):
            known = (bytes, type(None))
        return bind_matcher(matcher, known, copy_event=self._copy_event)

    async def __aenter__(self) -> EventWatcher[T]:
        if self.state != LifecycleState.CREATED:
            raise ConfigurationError("A watch context can be entered only once")
        self._check_affinity()
        self._loop = asyncio.get_running_loop()
        self._changed = asyncio.Event()
        self.state = LifecycleState.STARTING
        deadline = self._clock.monotonic() + self.factory.lifecycle.startup_timeout
        context = OpenContext(
            self.observer_id, deadline, self.factory.buffers, self.identity, self._diagnostics, self._clock
        )
        if self.start_position.mode.value not in self.factory.transport.capabilities.start_modes:
            self.state = LifecycleState.FAILED
            raise UnsupportedCapabilityError("Transport does not support the requested start mode")
        try:
            async with asyncio.timeout(max(0.0, deadline - self._clock.monotonic())):
                self._session = await self.factory.transport.open(
                    self.source, self.start_position, context=context
                )
                self.readiness = self._session.readiness
                if self.start_position.mode.value not in self.readiness.resolved_capabilities.start_modes:
                    raise UnsupportedCapabilityError("Session cannot provide requested start mode")
                if not {"independent_reader", "dedicated_subscription"}.intersection(
                    self.readiness.resolved_capabilities.isolation_modes
                ):
                    raise UnsupportedCapabilityError("Session must provide independent observation")
                if self.readiness.resolved_capabilities.supports_settlement and not callable(
                    getattr(self._session, "settle", None)
                ):
                    raise UnsupportedCapabilityError("Session advertises settlement without implementing it")
                self._tasks = [
                    asyncio.create_task(self._receive(), name="event-observer-receive"),
                    asyncio.create_task(self._monitor(), name="event-observer-monitor"),
                ]
                self.state = LifecycleState.READY
                # Reader starts before READY is returned, preserving immediate errors.
                await asyncio.sleep(0)
                if self.terminal_error is not None:
                    raise self.terminal_error
            self._diagnostics.record(
                "ready",
                start_mode=self.start_position.mode.value,
                requested_start_mode=self.start_position.mode.value,
                requested_offsets=dict(self.start_position.offsets),
                resolved_positions=dict(self.readiness.boundary.positions),
                ready_monotonic=self.readiness.ready_monotonic,
            )
            return self
        except BaseException as error:
            self.state = LifecycleState.FAILED
            self.terminal_error = error
            try:
                await self.close()
            except BaseException:
                self._diagnostics.record("startup_cleanup_failure")
            if isinstance(error, TimeoutError):
                raise ObserverStartupTimeoutError("Observer startup deadline expired") from error
            raise

    async def _receive(self) -> None:
        assert self._session is not None
        try:
            async for raw in self._session.receive():
                if self.state not in (LifecycleState.READY, LifecycleState.STARTING):
                    return
                size = self._journal.check_raw(raw)
                try:
                    payload = self.decoder.decode(raw)
                    if inspect.isawaitable(payload):
                        if inspect.iscoroutine(payload):
                            payload.close()
                        raise EventDecodingError("Decoder must return a value synchronously")
                except UnknownMessageTypeError:
                    if self.decoding.error_policy == DecodeErrorPolicy.SKIP_UNKNOWN_TYPE:
                        self._journal.skip_filtered()
                        self._diagnostics.record("filtered", reason_code="unknown_type")
                        self._notify()
                        await self._settle(raw, DeliveryOutcome.FILTERED)
                        continue
                    raise
                except EventDecodingError:
                    raise
                except Exception as exc:
                    raise EventDecodingError("Custom decoder failed") from exc
                decoded_size = self.access.estimate_bytes(payload)
                if isinstance(decoded_size, bool) or not isinstance(decoded_size, int) or decoded_size < 0:
                    raise PayloadIsolationError("Payload size estimate must be a nonnegative integer")
                event = ObservedEvent(
                    self.observer_id,
                    self._journal.next_sequence,
                    payload,
                    raw,
                    self._clock.utcnow(),
                    self._clock.monotonic(),
                    getattr(getattr(payload, "DESCRIPTOR", None), "full_name", None),
                    extract_trace_context(raw.headers),
                )
                admitted = self._journal.append(event, size + decoded_size + 128)
                self._diagnostics.record("admission", event_sequence=event.sequence, retained=admitted)
                self._notify()
                await self._settle(
                    raw,
                    DeliveryOutcome.ADMITTED
                    if admitted
                    else DeliveryOutcome.DISCARDED_BY_EXPLICIT_JOURNAL_POLICY,
                )
                # Fast async iterators must not starve waiters and failure monitor.
                if self._journal.next_sequence % self.factory.runtime_limits.scan_batch_events == 0:
                    await asyncio.sleep(0)
            if self.state in (LifecycleState.READY, LifecycleState.STARTING):
                raise UnexpectedStreamEndError("Transport receive stream ended unexpectedly")
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._fail(exc)

    async def _settle(self, raw, outcome: DeliveryOutcome) -> None:
        if self.readiness is not None and self.readiness.resolved_capabilities.supports_settlement:
            if raw.delivery_id is None:
                raise TransportError("Settlement transport supplied no delivery token")
            await getattr(self._session, "settle")(raw.delivery_id, outcome)

    async def _monitor(self) -> None:
        assert self._session is not None
        try:
            error = await self._session.wait_failure()
            if error is not None:
                self._fail(error)
            elif self.state in (LifecycleState.READY, LifecycleState.STARTING):
                self._fail(UnexpectedStreamEndError("Failure monitor ended before session close"))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._fail(
                exc
                if isinstance(exc, TransportError)
                else TransportError("Failure monitor failed", cause=exc)
            )

    def snapshot(self) -> JournalSnapshot:
        self._check_affinity()
        return self._journal.snapshot(self._copy_event)

    async def _expect(
        self,
        scope: _EventView[T],
        matcher,
        count: int,
        timeout: float,
        description: str | None,
        require_complete: bool,
        *,
        absolute_deadline: float | None = None,
    ) -> list[ObservedEvent[T]]:
        self._check_open()
        positive(timeout, "timeout")
        if not isinstance(require_complete, bool):
            raise ConfigurationError("require_complete must be bool")
        limits = self.factory.runtime_limits
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise ConfigurationError("count must be a positive integer")
        if count > limits.max_result_events or len(self._waiter_tasks) >= limits.max_active_waiters:
            raise ExpectationResourceLimitError("Expectation resource limit reached")
        began = self._clock.monotonic()
        deadline = absolute_deadline if absolute_deadline is not None else began + timeout
        bound = self._bind(matcher)
        expectation_id = uuid4().hex
        task = asyncio.current_task()
        assert task is not None
        self._waiter_tasks.add(task)
        cursor = scope.lower_bound
        results: list[ObservedEvent[T]] = []
        reserved = 0
        evaluated = excluded = not_applicable = 0
        outcome = "error"
        final_upper: int | None = None
        outcome_error: BaseException | None = None

        def check_state() -> None:
            if task.cancelling():
                raise asyncio.CancelledError
            self._check_open()
            if require_complete and not self._journal.complete(
                scope.lower_bound, final_upper, deadline=deadline
            ):
                raise IncompleteObservationError(
                    "Required observation range contains known retention loss",
                    observer_id=self.observer_id,
                    expectation_id=expectation_id,
                    diagnostics=self._diagnostics.snapshot(),
                )

        def evaluate(matcher, event):
            try:
                return matcher.match_event(self._copy_event(event))
            except (MatcherEvaluationError, InvalidMatcherResultError, PayloadIsolationError):
                # A callback can expose a known observer failure before it fails itself.
                check_state()
                raise

        try:
            while True:
                check_state()
                if self._clock.monotonic() >= deadline and final_upper is None:
                    final_upper = self._journal.next_sequence
                version = self._journal.version
                scanned_upper = self._journal.next_sequence
                changed = self._changed
                batch = self._journal.batch(
                    cursor,
                    limits.scan_batch_events,
                    final_upper if final_upper is not None else scanned_upper,
                )
                for event, size in batch:
                    cursor = event.sequence + 1
                    if event.recorded_monotonic > deadline:
                        continue
                    callback_start = self._clock.monotonic()
                    included = True
                    for scope_filter in scope._filters:
                        status = evaluate(scope_filter, event).status
                        if status != MatchStatus.MATCH:
                            included = False
                            excluded += 1
                            break
                    if not included:
                        continue
                    result = evaluate(bound, event)
                    evaluated += 1
                    duration = self._clock.monotonic() - callback_start
                    if duration > 0.05:
                        self._diagnostics.record("slow_callback", elapsed=duration)
                    if result.status == MatchStatus.NOT_APPLICABLE:
                        not_applicable += 1
                    elif result.status == MatchStatus.MISMATCH:
                        self._diagnostics.record_mismatch(
                            expectation_id, event.sequence, result.mismatches, payload=event.payload
                        )
                    elif result.status == MatchStatus.MATCH:
                        if self._result_bytes + size > limits.max_result_bytes:
                            raise ExpectationResourceLimitError("Pending result bytes budget exhausted")
                        results.append(self._copy_event(event))
                        reserved += size
                        self._result_bytes += size
                        if len(results) == count:
                            check_state()
                            outcome = "matched"
                            return results
                if batch:
                    # Release pinned canonical references before cooperative yield.
                    batch.clear()
                    await asyncio.sleep(0)
                    continue
                cursor = max(cursor, scanned_upper if final_upper is None else final_upper)
                if final_upper is not None:
                    check_state()
                    outcome = "timeout"
                    raise EventTimeoutError(
                        "No sufficient matching events within the observation deadline",
                        observer_id=self.observer_id,
                        expectation_id=expectation_id,
                        diagnostics=self._diagnostics.snapshot(),
                    )
                remaining = deadline - self._clock.monotonic()
                if remaining <= 0:
                    continue
                # Single loop: compare version and capture event without an intervening yield.
                if version == self._journal.version and changed is self._changed:
                    try:
                        assert changed is not None
                        await asyncio.wait_for(changed.wait(), remaining)
                    except TimeoutError:
                        pass
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except BaseException as exc:
            outcome_error = exc
            if isinstance(exc, (EventObserverError, EventAssertionError)):
                exc.observer_id = self.observer_id
                exc.expectation_id = expectation_id
                exc.sequence = cursor - 1 if cursor > scope.lower_bound else None
            raise
        finally:
            self._waiter_tasks.discard(task)
            self._result_bytes -= reserved
            self._diagnostics.record_expectation(
                expectation_id=expectation_id,
                scope_id=scope.scope_id,
                outcome=outcome,
                evaluated=evaluated,
                matched=len(results),
                scope_excluded=excluded,
                not_applicable=not_applicable,
                deadline=deadline,
                elapsed=self._clock.monotonic() - began,
                deadline_overrun=max(0.0, self._clock.monotonic() - deadline),
                requested_count=count,
                lower_bound=scope.lower_bound,
                error_code=getattr(outcome_error, "code", None),
                error_type=type(outcome_error).__name__ if outcome_error is not None else None,
                require_complete=require_complete,
                complete=self._journal.complete(scope.lower_bound, final_upper, deadline=deadline),
            )
            if isinstance(outcome_error, (EventObserverError, EventAssertionError)):
                outcome_error.diagnostics = self._diagnostics.snapshot()
            if not self._defer_reporting:
                self._diagnostics.emit(self.reporters)

    async def close(self) -> None:
        self._check_affinity()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._cleanup(), name="event-observer-cleanup")
        await asyncio.shield(self._close_task)

    async def _cleanup(self) -> None:
        self.cleanup_state = "running"
        self.state = LifecycleState.STOPPING
        self._notify()
        deadline = self._clock.monotonic() + self.factory.lifecycle.shutdown_timeout
        try:
            async with asyncio.timeout(max(0.0, deadline - self._clock.monotonic())):
                for task in self._tasks:
                    task.cancel()
                if self._tasks:
                    await asyncio.gather(*self._tasks, return_exceptions=True)
                if self._session is not None:
                    await self._session.close(deadline=deadline)
                waiters = [task for task in self._waiter_tasks if task is not asyncio.current_task()]
                if waiters:
                    await asyncio.gather(*waiters, return_exceptions=True)
            self.cleanup_state = "complete"
            self.state = LifecycleState.CLOSED
        except TimeoutError as exc:
            self.cleanup_state = "abandoned"
            self.state = LifecycleState.FAILED
            self._diagnostics.record("cleanup_failure", error_code="cleanup_timeout")
            raise ObserverCleanupTimeoutError("Observer cleanup deadline expired") from exc
        except BaseException as exc:
            self.cleanup_state = "failed"
            self.state = LifecycleState.FAILED
            self._diagnostics.record("cleanup_failure", error_code="cleanup_failure")
            raise ObserverCleanupError("Observer resource cleanup failed") from exc
        finally:
            self._notify()
            self._diagnostics.record("closed", cleanup_state=self.cleanup_state)
            if not self._defer_reporting:
                self._diagnostics.emit(self.reporters)

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        if exc_value is not None:
            self._diagnostics.record("context_failure", error_type=type(exc_value).__name__)
        cleanup_error = None
        try:
            await self.close()
        except asyncio.CancelledError:
            # Track and finish the shielded cleanup before propagating cancellation.
            if self._close_task is not None:
                try:
                    await self._close_task
                except BaseException:
                    self._diagnostics.record("cleanup_failure")
            raise
        except BaseException as error:
            cleanup_error = error
        if exc_value is not None:
            if cleanup_error is not None and hasattr(exc_value, "add_note"):
                exc_value.add_note("Observer cleanup also failed; see safe observer diagnostics")
            return
        if self.terminal_error is not None:
            self._raise_terminal()
        if cleanup_error is not None:
            raise cleanup_error
