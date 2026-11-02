"""Shared asynchronous observation runtime. All state belongs to one event loop."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import os
from typing import TYPE_CHECKING, Generic, Sequence, TypeVar, cast, overload
from uuid import uuid4

from ._runtime.clock import Clock, SystemClock
from ._reporters import bind_observation_reporters
from ._admission import admit_raw
from ._expectations import ExpectationState, ExpectationStatus, effective_lower_bound, validate_expectation
from ._views import LifecycleState, ScopeSnapshot

from .config import (
    DecodingPolicy,
    JournalPolicy,
    LifecyclePolicy,
    ObservationIdentity,
    RuntimeLimits,
    TransportBufferLimits,
)
from .decoders import Decoder, DefaultPayloadAccess, PayloadAccess, ProtobufDecoder, RawDecoder
from .diagnostics import DiagnosticCollector, DiagnosticPolicy, DiagnosticReporter
from .errors import (
    ConfigurationError,
    EventLoopMismatchError,
    ExpectationResourceLimitError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    ObserverClosedError,
    ObserverFailedError,
    ObserverStartupTimeoutError,
    PayloadIsolationError,
    ProcessAffinityError,
    ScopeResourceLimitError,
    TransportError,
    UnexpectedStreamEndError,
    UnsupportedCapabilityError,
)
from .journal import EventJournal, JournalSnapshot
from .matchers import EventMatcher, bind_matcher
from .models import (
    ObservedEvent,
    ObservationMark,
    OpenContext,
    ScopeStart,
    ScopeStartMode,
    StartPosition,
    TransportSource,
)
from .transports.base import DeliveryOutcome, TransportAdapter, TransportSession

T = TypeVar("T")

if TYPE_CHECKING:
    from .transports.kafka import KafkaConfig


class EventObserver:
    """Configure reusable, lazy observation contexts without opening a broker.

    :ivar transport: Adapter used to create an independent session per watcher.
    :ivar journal: Retained-history limits and overflow policy.
    :ivar lifecycle: Startup, shutdown, and sync control budgets.
    :ivar buffers: Transport handoff limits shared with opened sessions.
    :ivar runtime_limits: Scope, waiter, result, and scan limits.
    :ivar diagnostic_policy: Disclosure, reporting, and diagnostic retention policy.
    :ivar reporters: Reporter prototypes bound separately for each watcher.
    :ivar clock: Shared monotonic and UTC clock for each observation.
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
        # Isolated convenience layer: importing Core does not import Kafka's client.
        """Validate observation policies and retain dependencies without opening a session.

        :param transport: Adapter or supported Kafka configuration for independent observation.
        :param journal: Retained-history limits and overflow policy; defaults to JournalPolicy.
        :param lifecycle: Startup, shutdown, and command-control budgets.
        :param buffers: Bounded event and byte limits for transport handoff.
        :param runtime_limits: Scope, waiter, result, and cooperative-scan limits.
        :param diagnostics: Disclosure and bounded diagnostic-reporting policy.
        :param reporters: Reporter prototypes bound independently for each watcher.
        :param clock: Shared monotonic and UTC clock; defaults to the system clock.
        :raises ConfigurationError: If policies or the transport configuration are incompatible.
        """
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
    ) -> EventWatcher[T]:
        """Construct a single-use async watcher without performing broker I/O.

        :param source: Broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Independent-copy and accounted-size strategy for decoded payloads.
        :param start: Native transport start position; defaults to latest.
        :param decoding: Policy for unknown message types and decoding failures.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A lazy, single-use watcher with the selected payload type.
        :raises ConfigurationError: If message_type and decoder are both provided.
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
    ) -> EventWatcher[T]:
        """Construct a single-use async watcher without performing broker I/O.

        :param source: Broker source name or normalized transport source.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param payload_access: Independent-copy and accounted-size strategy for decoded payloads.
        :param start: Native transport start position; defaults to latest.
        :param decoding: Policy for unknown message types and decoding failures.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A lazy, single-use watcher with the selected payload type.
        :raises ConfigurationError: If message_type and decoder are both provided.
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
    ) -> EventWatcher[bytes | None]:
        """Construct a single-use async watcher without performing broker I/O.

        :param source: Broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Independent-copy and accounted-size strategy for decoded payloads.
        :param start: Native transport start position; defaults to latest.
        :param decoding: Policy for unknown message types and decoding failures.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A lazy, single-use watcher with the selected payload type.
        :raises ConfigurationError: If message_type and decoder are both provided.
        """
        ...

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
        """Construct a single-use async watcher without performing broker I/O.

        :param source: Broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Independent-copy and accounted-size strategy for decoded payloads.
        :param start: Native transport start position; defaults to latest.
        :param decoding: Policy for unknown message types and decoding failures.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A lazy, single-use watcher with the selected payload type.
        :raises ConfigurationError: If message_type and decoder are both provided.
        """
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
    """Describe an immutable filtered view over one continuously collected Journal.

    :ivar _watcher: Watcher that owns the Journal, transport, and runtime state.
    :ivar _lower_bound: Inclusive Journal sequence defining this view.
    :ivar _filters: Bound filters inherited in parent-to-child order.
    :ivar _scope_id: Stable diagnostic identity retained by wait-local views.
    :ivar _name: Optional user label for this view.
    :ivar lower_bound: Inclusive sequence from which this view searches.
    :ivar scope_id: Identity associated with expectations in this view.
    :ivar name: Optional scope label.
    :ivar diagnostics: Safe diagnostic collector owned by the watcher.
    """

    def __init__(
        self,
        watcher: EventWatcher[T],
        *,
        lower: int = 0,
        filters: tuple[EventMatcher, ...] = (),
        scope_id: str | None = None,
        name: str | None = None,
    ) -> None:
        """Store the immutable search boundary, filter chain, and diagnostic identity.

        :param watcher: Watcher owning the runtime and shared Journal.
        :param lower: Inclusive lower Journal sequence.
        :param filters: Bound filters in inherited evaluation order.
        :param scope_id: Existing diagnostic identity, or None to allocate a new identity.
        :param name: Optional caller-defined scope label.
        """
        self._watcher = watcher
        self._lower_bound = lower
        self._filters = filters
        self._scope_id = scope_id or uuid4().hex
        self._name = name

    @property
    def lower_bound(self) -> int:
        """Return the inclusive Journal boundary of this view.

        :returns: The inclusive Journal sequence of the scope definition.
        """
        return self._lower_bound

    @property
    def scope_id(self) -> str:
        """Return the stable identity used in expectation diagnostics.

        :returns: The scope identity used in expectation summaries.
        """
        return self._scope_id

    @property
    def name(self) -> str | None:
        """Return the optional user-supplied scope label.

        :returns: The optional scope label.
        """
        return self._name

    @property
    def diagnostics(self) -> DiagnosticCollector:
        """Return the safe diagnostic collector after checking owner affinity.

        :returns: The watcher-owned safe diagnostic collector.
        """
        self._watcher._check_affinity()
        return self._watcher._diagnostics

    def mark(self) -> ObservationMark:
        """Capture the next admission sequence without modifying this view.

        :returns: An immutable watcher-local admission mark with informational timestamps.
        :raises ObserverClosedError: If the watcher has not started or has closed.
        :raises ObserverFailedError: If a terminal observation failure is already known.
        """
        watcher = self._watcher
        watcher._check_open()
        return ObservationMark(
            watcher.observer_id,
            watcher._journal.next_sequence,
            watcher._clock.utcnow(),
            watcher._clock.monotonic(),
        )

    def scope(self, matcher, *, name: str | None = None, start: ScopeStart | None = None) -> EventScope[T]:
        """Create a child view with an intersected start boundary and one more filter.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param name: Optional caller-defined scope label.
        :param start: Local Journal boundary intersected with the parent scope definition.
        :returns: A child scope preserving the parent boundary and filter chain.
        :raises ScopeResourceLimitError: If the registered-scope limit is exhausted.
        :raises ConfigurationError: If the requested local start is invalid or lies in the future.
        """
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
        self,
        matcher,
        *,
        timeout: float,
        description: str | None = None,
        require_complete: bool = True,
        since: ObservationMark | None = None,
    ) -> T:
        """Return the first matching payload copy in the required observation range.

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
        results = await self._watcher._expect(
            self, matcher, 1, timeout, description, require_complete, since=since
        )
        return results[0].payload

    async def wait_for_event(
        self,
        matcher,
        *,
        timeout: float,
        description: str | None = None,
        require_complete: bool = True,
        since: ObservationMark | None = None,
    ) -> ObservedEvent[T]:
        """Return the first matching event with an independent payload copy.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param timeout: Positive finite admission-window duration in seconds.
        :param description: Reserved expectation label; currently not recorded or exported.
        :param require_complete: Whether any known retention loss in the required range must fail.
        :param since: Reusable mark from this watcher intersected with the view boundary.
        :returns: A detached event containing an independent payload copy.
        :raises EventTimeoutError: If the required matches are not admitted before the deadline.
        :raises IncompleteObservationError: If completeness is required and the range contains known
            loss.
        :raises ObserverFailedError: If a terminal observation failure is known.
        """
        results = await self._watcher._expect(
            self, matcher, 1, timeout, description, require_complete, since=since
        )
        return results[0]

    async def wait_for_count(
        self,
        matcher,
        count: int,
        *,
        timeout: float,
        description: str | None = None,
        require_complete: bool = True,
        since: ObservationMark | None = None,
    ) -> list[T]:
        """Return independent payload copies from distinct matching Journal events.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param count: Positive number of distinct matching Journal events to return.
        :param timeout: Positive finite admission-window duration in seconds.
        :param description: Reserved expectation label; currently not recorded or exported.
        :param require_complete: Whether any known retention loss in the required range must fail.
        :param since: Reusable mark from this watcher intersected with the view boundary.
        :returns: Payload copies from distinct matching events in Journal order.
        :raises EventTimeoutError: If the required matches are not admitted before the deadline.
        :raises IncompleteObservationError: If completeness is required and the range contains known
            loss.
        :raises ObserverFailedError: If a terminal observation failure is known.
        """
        results = await self._watcher._expect(
            self, matcher, count, timeout, description, require_complete, since=since
        )
        return [event.payload for event in results]


class EventScope(_EventView[T]):
    """Search shared history through fixed correlation filters and a lower boundary."""

    def snapshot(self) -> ScopeSnapshot:
        """Describe this scope definition and its retained expectation summaries.

        :returns: A detached snapshot of the original scope definition.
        """
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
    """Own one transport session, bounded history, and independent async expectations.

    :ivar factory: Reusable configuration from which this watcher was constructed.
    :ivar _clock: Clock shared with transport admission and diagnostics.
    :ivar source: Normalized broker source being observed.
    :ivar decoder: Synchronous decoder that transfers payload ownership to the runtime.
    :ivar access: Payload copying and accounted-size strategy.
    :ivar start_position: Native transport start position requested at startup.
    :ivar decoding: Policy for unknown message types and decode failures.
    :ivar identity: Optional run, worker, and test identifiers.
    :ivar observer_id: Unique identity used for events, marks, and diagnostics.
    :ivar state: Current lifecycle state.
    :ivar cleanup_state: Cleanup outcome independent of the observation result.
    :ivar terminal_error: First known background or startup failure, if any.
    :ivar _journal: Bounded admitted history and conservative loss ledger.
    :ivar _diagnostics: Policy-controlled collector using the watcher clock and copy strategy.
    :ivar reporters: Reporter instances bound to this observation.
    :ivar _session: Owned transport session after opening.
    :ivar _loop: Owner event loop captured when entering the context.
    :ivar _pid: Process identity used to reject cross-process access.
    :ivar _changed: Replaceable notification for admissions and lifecycle changes.
    :ivar _tasks: Receive and independent failure-monitor tasks.
    :ivar _close_task: Shared shielded cleanup task used by concurrent close calls.
    :ivar _waiter_tasks: Tasks currently running expectations.
    :ivar _scope_count: Number of registered child scope definitions.
    :ivar _result_bytes: Bytes reserved by all pending expectation results.
    :ivar readiness: Confirmed transport capabilities and observation boundary.
    """

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
        """Create watcher-local state and bind report context without opening the broker.

        :param factory: Lazy factory providing the policies and dependencies for this watcher.
        :param source: Broker source name or normalized transport source.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param access: Independent-copy and accounted-size strategy for decoded payloads.
        :param start: Native transport start position; defaults to latest.
        :param decoding: Policy for unknown message types and decoding failures.
        :param identity: Optional run, worker, and test identifiers.
        """
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
        self._diagnostics = DiagnosticCollector(
            self.observer_id,
            factory.diagnostic_policy,
            clock=self._clock,
            clone_payload=access.clone_for_user,
        )
        self.reporters = bind_observation_reporters(
            factory.reporters,
            factory.diagnostic_policy,
            self._diagnostics,
            transport=type(factory.transport).__name__,
            source=source.name,
        )
        self._session: TransportSession | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pid = os.getpid()
        self._changed: asyncio.Event | None = None
        self._tasks: list[asyncio.Task] = []
        self._close_task: asyncio.Task | None = None
        self._waiter_tasks: set[asyncio.Task] = set()
        self._scope_count = 0
        self._result_bytes = 0
        self.readiness = None

    def _check_affinity(self) -> None:
        """Reject access from another process or an incompatible event loop.

        :raises ProcessAffinityError: If the watcher belongs to another process.
        :raises EventLoopMismatchError: If access occurs on another event loop.
        """
        if os.getpid() != self._pid:
            raise ProcessAffinityError("Watcher belongs to another process")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._loop is not None and loop is not self._loop:
            raise EventLoopMismatchError("Watcher belongs to another event loop")

    def _check_open(self) -> None:
        """Require owner affinity and readiness, prioritizing any known terminal failure.

        :raises ObserverFailedError: If a terminal failure is known.
        :raises ObserverClosedError: If observation is not ready.
        """
        self._check_affinity()
        if self.terminal_error is not None:
            self._raise_terminal()
        if self.state != LifecycleState.READY:
            raise ObserverClosedError("Watcher is not ready or has closed", observer_id=self.observer_id)

    def _raise_terminal(self) -> None:
        """Raise a stable observer error chained to the first background failure.

        :raises ObserverFailedError: Always, chained to the retained primary failure.
        """
        error = ObserverFailedError(
            "Observer background operation failed",
            observer_id=self.observer_id,
            diagnostics=self._diagnostics.snapshot(),
            cause=self.terminal_error,
        )
        raise error from self.terminal_error

    def _notify(self) -> None:
        """Wake current observers and replace the admission notification atomically."""
        if self._changed is not None:
            self._changed.set()
        self._changed = asyncio.Event()

    def _fail(self, error: BaseException) -> None:
        """Record the first terminal failure and wake waiting expectations.

        :param error: First background or transport failure to preserve.
        """
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
        """Detach an event payload using the configured copying strategy.

        :param event: Canonical event whose payload must remain isolated from user callbacks.
        :returns: An event envelope containing an independent payload copy.
        :raises PayloadIsolationError: If the payload cannot be detached safely.
        """
        try:
            return replace(event, payload=self.access.clone_for_user(event.payload))
        except PayloadIsolationError:
            raise
        except Exception as exc:
            raise PayloadIsolationError("Payload isolation failed") from exc

    def _bind(self, matcher) -> EventMatcher:
        """Bind a matcher to known decoder types and independent event copying.

        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :returns: A bound three-valued event matcher.
        """
        known = getattr(self.decoder, "known_message_types", None)
        if isinstance(self.decoder, RawDecoder):
            known = (bytes, type(None))
        return bind_matcher(matcher, known, copy_event=self._copy_event)

    async def __aenter__(self) -> EventWatcher[T]:
        """Open an independent transport session and start collection after readiness.

        :returns: This watcher after transport readiness and collector startup.
        :raises ConfigurationError: If the context was already entered.
        :raises ObserverStartupTimeoutError: If startup exceeds its admission-clock budget.
        :raises TransportError: If independent transport readiness cannot be established.
        """
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
        try:
            if self.start_position.mode.value not in self.factory.transport.capabilities.start_modes:
                raise UnsupportedCapabilityError("Transport does not support the requested start mode")
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
                if self._clock.monotonic() >= deadline:
                    # A pending caller cancellation keeps priority over startup expiry.
                    await asyncio.sleep(0)
                    raise TimeoutError("Observer startup deadline expired")
                self._tasks = [
                    asyncio.create_task(self._receive(), name="event-observer-receive"),
                    asyncio.create_task(self._monitor(), name="event-observer-monitor"),
                ]
                self.state = LifecycleState.READY
                # Reader starts before READY is returned, preserving immediate errors.
                await asyncio.sleep(0)
                if self.terminal_error is not None:
                    raise self.terminal_error
                if self._clock.monotonic() >= deadline:
                    raise TimeoutError("Observer startup deadline expired")
            self._diagnostics.record(
                "ready",
                start_mode=self.start_position.mode.value,
                requested_start_mode=self.start_position.mode.value,
                requested_offsets=dict(self.start_position.offsets),
                resolved_positions=dict(self.readiness.boundary.positions),
                ready_monotonic=self.readiness.ready_monotonic,
                **(
                    {
                        "requested_stream_id": self.start_position.stream_id,
                        "resolved_stream_id": self.readiness.source_metadata.get("redis.start_id"),
                    }
                    if "redis.start_id" in self.readiness.source_metadata
                    else {}
                ),
            )
            return self
        except BaseException as error:
            self.state = LifecycleState.FAILED
            self.terminal_error = error
            self._diagnostics.record(
                "startup_failure",
                error_type=type(error).__name__,
                error_code=getattr(error, "code", None),
            )
            try:
                await self.close()
            except BaseException:
                self._diagnostics.record("startup_cleanup_failure")
            if isinstance(error, TimeoutError):
                raise ObserverStartupTimeoutError("Observer startup deadline expired", cause=error) from error
            raise

    async def _receive(self) -> None:
        """Decode, account, admit, and settle records until closure or terminal failure."""
        assert self._session is not None
        try:
            async for raw in self._session.receive():
                if self.state not in (LifecycleState.READY, LifecycleState.STARTING):
                    return
                outcome = admit_raw(
                    raw,
                    observer_id=self.observer_id,
                    journal=self._journal,
                    decoder=self.decoder,
                    access=self.access,
                    clock=self._clock,
                    decoding=self.decoding,
                    diagnostics=self._diagnostics,
                )
                self._notify()
                await self._settle(raw, outcome)
                if outcome == DeliveryOutcome.FILTERED:
                    continue
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
        """Acknowledge a delivery only when the session advertises settlement.

        :param raw: Transport delivery whose committed outcome may require settlement.
        :param outcome: Committed admission, filtering, or explicit discard outcome.
        """
        if self.readiness is not None and self.readiness.resolved_capabilities.supports_settlement:
            if raw.delivery_id is None:
                raise TransportError("Settlement transport supplied no delivery token")
            await getattr(self._session, "settle")(raw.delivery_id, outcome)

    async def _monitor(self) -> None:
        """Observe continuity failure independently of business-message reception."""
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
        """Copy retained Journal events and loss information on the owner loop.

        :returns: A detached snapshot of retained events, counters, and loss ranges.
        """
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
        since: ObservationMark | None = None,
    ) -> list[ObservedEvent[T]]:
        """Scan an independent bounded range and commit a complete matching result.

        :param scope: Immutable search view and inherited correlation filters.
        :param matcher: Payload predicate or declarative matcher to bind and evaluate.
        :param count: Positive number of distinct matching Journal events to return.
        :param timeout: Positive finite admission-window duration in seconds.
        :param description: Reserved expectation label; currently not recorded or exported.
        :param require_complete: Whether any known retention loss in the required range must fail.
        :param absolute_deadline: Optional admission deadline already computed in the shared clock
            domain.
        :param since: Reusable mark from this watcher intersected with the view boundary.
        :returns: Detached matching events in Journal order.
        :raises EventTimeoutError: If too few eligible events match before the admission deadline.
        :raises IncompleteObservationError: If required history contains known retention loss.
        :raises ConfigurationError: If arguments or the observation mark are invalid.
        :raises ExpectationResourceLimitError: If waiter or result limits would be exceeded.
        :raises ObserverFailedError: If background collection has failed.
        """
        self._check_open()
        lower_bound = effective_lower_bound(
            observer_id=self.observer_id,
            lower_bound=scope.lower_bound,
            since=since,
            next_sequence=self._journal.next_sequence,
        )
        limits = self.factory.runtime_limits
        validate_expectation(count, timeout, require_complete, max_result_events=limits.max_result_events)
        if len(self._waiter_tasks) >= limits.max_active_waiters:
            raise ExpectationResourceLimitError("Expectation resource limit reached")
        began = self._clock.monotonic()
        deadline = absolute_deadline if absolute_deadline is not None else began + timeout
        bound = self._bind(matcher)
        expectation_id = uuid4().hex
        task = asyncio.current_task()
        assert task is not None

        def check_state() -> None:
            """Check scheduler cancellation before the shared lifecycle and terminal state."""
            if task.cancelling():
                raise asyncio.CancelledError
            self._check_open()

        def reserve_result(size: int) -> None:
            """Reserve result memory across all unfinished owner-loop expectations.

            :param size: Accounted size of the matched event to reserve.
            :raises ExpectationResourceLimitError: Aggregate result bytes exceed the runtime budget.
            """
            if self._result_bytes + size > limits.max_result_bytes:
                raise ExpectationResourceLimitError("Pending result bytes budget exhausted")
            self._result_bytes += size

        def release_result(size: int) -> None:
            """Release the finished expectation's pending-result reservation.

            :param size: Accounted bytes reserved by this expectation.
            """
            self._result_bytes -= size

        state = ExpectationState(
            observer_id=self.observer_id,
            expectation_id=expectation_id,
            scope_id=scope.scope_id,
            journal=self._journal,
            lower_bound=lower_bound,
            filters=scope._filters,
            matcher=bound,
            count=count,
            timeout=timeout,
            began=began,
            deadline=deadline,
            require_complete=require_complete,
            scan_batch_events=limits.scan_batch_events,
            clock=self._clock,
            copy_event=self._copy_event,
            diagnostics=self._diagnostics,
            check_state=check_state,
            reserve_result=reserve_result,
            release_result=release_result,
        )
        self._waiter_tasks.add(task)
        outcome_error: BaseException | None = None
        cancelled = False
        try:
            while True:
                # Capturing the notifier before a synchronous step preserves the
                # same version/notification protocol as the former inline scan.
                changed = self._changed
                step = state.step()
                if step.status is ExpectationStatus.MATCHED:
                    return state.results
                if step.status is ExpectationStatus.TIMEOUT:
                    raise state.timeout_error()
                if step.scanned:
                    await asyncio.sleep(0)
                    continue
                remaining = deadline - self._clock.monotonic()
                if remaining <= 0:
                    continue
                if step.version == self._journal.version and changed is self._changed:
                    try:
                        assert changed is not None
                        await asyncio.wait_for(changed.wait(), remaining)
                    except TimeoutError:
                        pass
        except asyncio.CancelledError:
            cancelled = True
            raise
        except BaseException as error:
            outcome_error = error
            raise
        finally:
            self._waiter_tasks.discard(task)
            state.finish(outcome_error, cancelled=cancelled)
            self._diagnostics.emit(self.reporters)

    async def close(self) -> None:
        """Await shared shielded cleanup within the configured lifecycle budget."""
        self._check_affinity()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._cleanup(), name="event-observer-cleanup")
        await asyncio.shield(self._close_task)

    async def _cleanup(self) -> None:
        """Release runtime tasks and the owned session while recording honest cleanup state."""
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
            self._diagnostics.emit(self.reporters)

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        """Finish observation while preserving a context or primary background failure.

        :param exc_type: Context exception type, or False for explicit close without context semantics.
        :param exc_value: Primary context exception, if any.
        :param traceback: Original traceback supplied by the context manager protocol.
        """
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
