"""Blocking observation driven by a dedicated thread, without an asyncio runtime."""

from __future__ import annotations

from asyncio import CancelledError
import concurrent.futures
from dataclasses import replace
import inspect
import os
from typing import TYPE_CHECKING, Generic, Sequence, TypeVar, overload
from uuid import uuid4

from ._admission import admit_raw
from ._expectations import ExpectationState, ExpectationStatus, effective_lower_bound, validate_expectation
from ._runtime.clock import Clock, SystemClock
from ._reporters import bind_observation_reporters
from ._runtime.threaded import CommandCancelled, PendingOperation, ThreadRuntime
from ._views import LifecycleState, ScopeSnapshot
from .config import (
    DecodingPolicy,
    JournalPolicy,
    LifecyclePolicy,
    ObservationIdentity,
    RuntimeLimits,
    TransportBufferLimits,
    positive,
)
from .decoders import Decoder, DefaultPayloadAccess, PayloadAccess, ProtobufDecoder, RawDecoder
from .diagnostics import (
    DiagnosticCollector,
    DiagnosticExport,
    DiagnosticPolicy,
    DiagnosticReporter,
    DiagnosticSnapshot,
)
from .errors import (
    ConfigurationError,
    EventObserverError,
    ExpectationResourceLimitError,
    ObserverCleanupError,
    ObserverClosedError,
    ObserverFailedError,
    ObserverStartupTimeoutError,
    PayloadIsolationError,
    ProcessAffinityError,
    ScopeResourceLimitError,
    TransportError,
    UnsupportedCapabilityError,
)
from .journal import EventJournal, JournalSnapshot
from .matchers import bind_matcher
from .models import (
    ObservationMark,
    ObservedEvent,
    OpenContext,
    ScopeStart,
    ScopeStartMode,
    StartPosition,
    TransportSource,
    TransportReadiness,
)
from .transports.sync_base import SyncAdapterProvider, SyncTransportAdapter, SyncTransportSession

if TYPE_CHECKING:
    from .transports.kafka import KafkaConfig

T = TypeVar("T")
_INTERRUPTION_ERRORS = (KeyboardInterrupt, SystemExit, CancelledError, concurrent.futures.CancelledError)


def _blocking_adapter(transport):
    """Resolve a lazy blocking adapter without creating native I/O or an event loop.

    :param transport: Blocking adapter, explicit sync_adapter() provider, or Kafka configuration.
    :returns: The blocking adapter selected for independent sessions.
    :raises ConfigurationError: The supplied value exposes only an asynchronous open method or no adapter API.
    """
    if not hasattr(transport, "open"):
        from .transports.kafka import KafkaConfig
        from .transports.sync_kafka import SyncKafkaAdapter

        if isinstance(transport, KafkaConfig):
            return SyncKafkaAdapter(transport)
        raise ConfigurationError("transport must implement SyncTransportAdapter")
    provider = getattr(transport, "sync_adapter", None)
    chosen = provider() if callable(provider) else transport
    if inspect.isawaitable(chosen):
        if inspect.iscoroutine(chosen):
            chosen.close()
        raise ConfigurationError("sync_adapter() must return a blocking adapter synchronously")
    if inspect.iscoroutinefunction(getattr(chosen, "open", None)):
        raise ConfigurationError(
            "An async-only transport cannot power SyncEventObserver; supply a blocking adapter or sync_adapter() provider"
        )
    if not callable(getattr(chosen, "open", None)) or not hasattr(chosen, "capabilities"):
        raise ConfigurationError("transport must implement SyncTransportAdapter")
    return chosen


class SyncEventObserver:
    """Configure reusable blocking watchers driven by independently owned worker threads.

    Constructing the factory or a watcher does not open a broker. Native resources
    are acquired only when the single-use watcher enters its context.

    :ivar transport: Resolved blocking adapter or explicitly supplied blocking provider.
    :ivar journal: Journal retention limits and overflow policy.
    :ivar lifecycle: Startup, cleanup, command-control, and cancellation budgets.
    :ivar buffers: Per-session bounded transport handoff limits.
    :ivar runtime_limits: Scope, expectation, result-memory, and scan-batch limits.
    :ivar diagnostic_policy: Disclosure, retention, and automatic reporting policy.
    :ivar reporters: Reporter prototypes bound independently for each watcher.
    :ivar clock: Shared monotonic and UTC clock used by observation and transports.
    """

    def __init__(
        self,
        *,
        transport: SyncTransportAdapter | SyncAdapterProvider | KafkaConfig,
        journal: JournalPolicy | None = None,
        lifecycle: LifecyclePolicy | None = None,
        buffers: TransportBufferLimits | None = None,
        runtime_limits: RuntimeLimits | None = None,
        diagnostics: DiagnosticPolicy | None = None,
        reporters: Sequence[DiagnosticReporter] = (),
        clock: Clock | None = None,
    ) -> None:
        """Resolve the blocking transport and validate policies without opening resources.

        :param transport: Blocking adapter, explicit blocking-provider adapter, or Kafka configuration.
        :param journal: Retained-history limits and overflow behavior; defaults to JournalPolicy.
        :param lifecycle: Startup, shutdown, command-control, and cancellation budgets.
        :param buffers: Maximum pending transport deliveries and accounted handoff bytes.
        :param runtime_limits: Scope, expectation, result, and cooperative-scan limits.
        :param diagnostics: Bounded diagnostics and disclosure policy.
        :param reporters: Reporter prototypes to bind independently for each watcher.
        :param clock: Shared clock; defaults to the system monotonic and UTC clocks.
        :raises ConfigurationError: The transport is async-only or event and handoff limits conflict.
        """
        self.transport = _blocking_adapter(transport)
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
    ) -> SyncEventWatcher[T]:
        """Construct a lazy single-use blocking watcher with the selected payload strategy.

        :param source: Existing broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Payload copy and accounted-size strategy; defaults to independent copying.
        :param start: Requested native start boundary; defaults to latest.
        :param decoding: Unknown-type and decoding-failure policy.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A watcher that opens its independent session on context entry.
        :raises ConfigurationError: Both message_type and decoder are supplied or source configuration is invalid.
        :raises MissingOptionalDependencyError: The selected Protobuf strategy is unavailable.
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
        """Construct a lazy single-use blocking watcher with the selected payload strategy.

        :param source: Existing broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Payload copy and accounted-size strategy; defaults to independent copying.
        :param start: Requested native start boundary; defaults to latest.
        :param decoding: Unknown-type and decoding-failure policy.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A watcher that opens its independent session on context entry.
        :raises ConfigurationError: Both message_type and decoder are supplied or source configuration is invalid.
        :raises MissingOptionalDependencyError: The selected Protobuf strategy is unavailable.
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
        """Construct a lazy single-use blocking watcher with the selected payload strategy.

        :param source: Existing broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Payload copy and accounted-size strategy; defaults to independent copying.
        :param start: Requested native start boundary; defaults to latest.
        :param decoding: Unknown-type and decoding-failure policy.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A watcher that opens its independent session on context entry.
        :raises ConfigurationError: Both message_type and decoder are supplied or source configuration is invalid.
        :raises MissingOptionalDependencyError: The selected Protobuf strategy is unavailable.
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
    ):
        """Construct a lazy single-use blocking watcher with the selected payload strategy.

        :param source: Existing broker source name or normalized transport source.
        :param message_type: Generated Protobuf class, mutually exclusive with decoder.
        :param decoder: Synchronous custom decoder, mutually exclusive with message_type.
        :param payload_access: Payload copy and accounted-size strategy; defaults to independent copying.
        :param start: Requested native start boundary; defaults to latest.
        :param decoding: Unknown-type and decoding-failure policy.
        :param identity: Optional run, worker, and test identifiers.
        :returns: A watcher that opens its independent session on context entry.
        :raises ConfigurationError: Both message_type and decoder are supplied or source configuration is invalid.
        :raises MissingOptionalDependencyError: The selected Protobuf strategy is unavailable.
        """
        if message_type is not None and decoder is not None:
            raise ConfigurationError("Specify either message_type or decoder")
        chosen = (
            decoder
            if decoder is not None
            else ProtobufDecoder(message_type)
            if message_type is not None
            else RawDecoder()
        )
        return SyncEventWatcher(
            self,
            source if isinstance(source, TransportSource) else TransportSource(source),
            chosen,
            payload_access if payload_access is not None else DefaultPayloadAccess(),
            start or StartPosition.latest(),
            decoding or DecodingPolicy(),
            identity or ObservationIdentity(),
        )


class _SyncDiagnostics:
    """Expose detached diagnostics through owner-thread commands and post-stop caches.

    :ivar _watcher: Blocking watcher owning the collector, runtime, and diagnostic caches.
    """

    def __init__(self, watcher) -> None:
        """Retain the watcher without copying or reading live collector state.

        :param watcher: Blocking watcher whose runtime owns the collector.
        """
        self._watcher = watcher

    def snapshot(self):
        """Read a detached collector snapshot through the worker or its final cache.

        :returns: The live or cached DiagnosticSnapshot, or None before a cache is available.
        :raises EventObserverError: Caller affinity or runtime command control prevents the read.
        """
        owner = self._watcher
        owner._runtime.guard()
        if not owner._runtime.running:
            return owner._cached_diagnostics
        return owner._call(owner._diagnostics.snapshot, report=False)

    def export(self):
        """Prepare a full diagnostic export on the worker or return its final cached export.

        :returns: A DiagnosticExport, or None when no full export has been cached after stop.
        :raises EventObserverError: Caller affinity or runtime command control prevents the export.
        """
        owner = self._watcher
        owner._runtime.guard()
        if not owner._runtime.running:
            return owner._cached_export
        return owner._call(owner._diagnostics.export, report=False)


class SyncEventScope(Generic[T]):
    """Represent an immutable correlation view over one blocking watcher's history.

    Scopes retain their parent's filters and may only narrow its lower boundary.
    Expectations keep independent cursors and do not consume Journal records.

    :ivar _owner: Watcher owning the single runtime, session, Journal, and diagnostics.
    :ivar _lower_bound: Inclusive Journal sequence fixed when this scope is created.
    :ivar _filters: Bound inherited filters in declaration order.
    :ivar _scope_id: Stable identity associating diagnostic expectation summaries.
    :ivar _name: Optional caller-supplied scope label.
    :ivar diagnostics: Watcher-wide detached diagnostic access facade.
    :ivar lower_bound: Inclusive immutable search boundary exposed with caller-affinity checks.
    :ivar scope_id: Stable diagnostic scope identity exposed with caller-affinity checks.
    :ivar name: Optional scope label exposed with caller-affinity checks.
    """

    def __init__(self, watcher, *, lower=0, filters=(), scope_id=None, name=None) -> None:
        """Capture an already bound scope definition without opening another reader.

        :param watcher: Blocking watcher owning the canonical Journal.
        :param lower: Inclusive sequence inherited or narrowed from the parent scope.
        :param filters: Bound parent-to-child correlation predicates.
        :param scope_id: Existing diagnostic identity, or None to allocate a new identity.
        :param name: Optional caller-supplied scope label.
        """
        self._owner, self._lower_bound, self._filters = watcher, lower, filters
        self._scope_id, self._name = scope_id or uuid4().hex, name

    @property
    def diagnostics(self):
        """Return the shared watcher's detached diagnostic facade.

        :returns: Access to watcher-wide snapshots and exports.
        """
        return self._owner.diagnostics

    @property
    def lower_bound(self):
        """Return this view's fixed inclusive admission sequence.

        :returns: Journal sequence fixed by the scope definition.
        :raises EventObserverError: The scope is accessed from an incompatible caller context.
        """
        self._owner._runtime.guard()
        return self._lower_bound

    @property
    def scope_id(self):
        """Return the stable identity used in this scope's expectation summaries.

        :returns: Watcher-local diagnostic scope identifier.
        :raises EventObserverError: The scope is accessed from an incompatible caller context.
        """
        self._owner._runtime.guard()
        return self._scope_id

    @property
    def name(self):
        """Return the optional caller label retained by this scope.

        :returns: The supplied label, or None when the scope is unnamed.
        :raises EventObserverError: The scope is accessed from an incompatible caller context.
        """
        self._owner._runtime.guard()
        return self._name

    def scope(self, matcher, *, name=None, start=None) -> SyncEventScope[T]:
        """Bind a child correlation filter and boundary on the existing worker.

        :param matcher: Correlation matcher evaluated after inherited parent filters.
        :param name: Optional label for the new scope.
        :param start: Boundary narrowing the parent range; defaults to all retained history.
        :returns: An immutable scope sharing this watcher's session and Journal.
        :raises ConfigurationError: The boundary is in the future or the matcher cannot be bound.
        :raises ScopeResourceLimitError: The watcher's lifetime scope budget is exhausted.
        :raises EventObserverError: The watcher is unavailable or caller affinity is invalid.
        """
        return self._owner._call(
            lambda: self._owner._make_scope(self, matcher, name=name, start=start), report=False
        )

    def snapshot(self):
        """Copy this scope's definition, completeness, and retained expectation summaries.

        :returns: A detached ScopeSnapshot without scanning or consuming Journal records.
        :raises EventObserverError: The worker is unavailable or caller affinity is invalid.
        """
        return self._owner._call(lambda: self._owner._scope_snapshot(self), report=False)

    def mark(self) -> ObservationMark:
        """Record the watcher's next admission sequence atomically on its worker.

        The mark may be reused through since without allocating another scope or reader.
        Its timestamps are informational; Journal sequence determines the search boundary.

        :returns: An immutable watcher-local ObservationMark.
        :raises ObserverClosedError: The watcher is not open and has no known terminal failure.
        :raises ObserverFailedError: A terminal observation failure is already known.
        :raises EventObserverError: Caller affinity or runtime control prevents marking.
        """
        owner = self._owner
        owner._runtime.guard()
        if not owner._runtime.running:
            owner._raise_closed()
        return owner._call(owner._mark, report=False)

    def _expect(self, matcher, count, timeout, description, require_complete, since):
        """Dispatch one bounded expectation with a deadline beginning in the caller.

        :param matcher: Predicate or event matcher to bind on the worker.
        :param count: Positive number of distinct matching Journal records required.
        :param timeout: Positive finite event-admission window, including command handoff time.
        :param description: Reserved caller label; currently excluded from diagnostics and report exports.
        :param require_complete: Reject relevant known retention loss when True.
        :param since: Optional same-watcher mark intersecting this scope's lower boundary.
        :returns: Detached matched envelopes in admission order.
        :raises EventAssertionError: The eligible range times out or is known incomplete.
        :raises EventObserverError: Validation, observation failure, or command control prevents completion.
        """
        owner = self._owner
        owner._runtime.guard()
        positive(timeout, "timeout")
        began = owner._clock.monotonic()
        if not owner._runtime.running:
            owner._raise_closed()
        return owner._dispatch(
            lambda: owner._begin_expectation(self, matcher, count, timeout, require_complete, since, began),
            budget=timeout + owner.factory.lifecycle.sync_control_timeout,
        )

    def wait_for(
        self,
        matcher,
        *,
        timeout,
        description=None,
        require_complete=True,
        since: ObservationMark | None = None,
    ) -> T:
        """Wait for one matching record and return an independently copied payload.

        :param matcher: Payload predicate or event matcher applied after this scope's filters.
        :param timeout: Positive finite admission window in seconds; command cleanup has a separate budget.
        :param description: Reserved caller label; currently excluded from diagnostics and report exports.
        :param require_complete: Reject relevant known loss; False explicitly permits a partial observation.
        :param since: Reusable mark from this watcher; its sequence intersects the immutable scope range.
        :returns: The payload of the first matching eligible event.

        :raises EventTimeoutError: Insufficient distinct matches were admitted within the window.
        :raises IncompleteObservationError: Required observation history contains relevant known loss.
        :raises ObserverFailedError: A known terminal failure prevents a trustworthy result.
        :raises SyncRuntimeUnresponsiveError: The worker exceeds control or cancellation-acknowledgement budgets.
        :raises ConfigurationError: Wait arguments or the supplied mark are invalid.
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
        """Wait for one matching record and return its detached event envelope.

        :param matcher: Payload predicate or event matcher applied after this scope's filters.
        :param timeout: Positive finite admission window in seconds; command cleanup has a separate budget.
        :param description: Reserved caller label; currently excluded from diagnostics and report exports.
        :param require_complete: Reject relevant known loss; False explicitly permits a partial observation.
        :param since: Reusable mark from this watcher; its sequence intersects the immutable scope range.
        :returns: The first matching eligible ObservedEvent with isolated payload.

        :raises EventTimeoutError: Insufficient distinct matches were admitted within the window.
        :raises IncompleteObservationError: Required observation history contains relevant known loss.
        :raises ObserverFailedError: A known terminal failure prevents a trustworthy result.
        :raises SyncRuntimeUnresponsiveError: The worker exceeds control or cancellation-acknowledgement budgets.
        :raises ConfigurationError: Wait arguments or the supplied mark are invalid.
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
        """Wait for distinct matching records without consuming them for later waits.

        :param matcher: Payload predicate or event matcher applied after this scope's filters.
        :param timeout: Positive finite admission window in seconds; command cleanup has a separate budget.
        :param description: Reserved caller label; currently excluded from diagnostics and report exports.
        :param require_complete: Reject relevant known loss; False explicitly permits a partial observation.
        :param since: Reusable mark from this watcher; its sequence intersects the immutable scope range.
        :param count: Positive number of distinct matching admissions required.
        :returns: Independent payload copies in Journal admission order.

        :raises EventTimeoutError: Insufficient distinct matches were admitted within the window.
        :raises IncompleteObservationError: Required observation history contains relevant known loss.
        :raises ObserverFailedError: A known terminal failure prevents a trustworthy result.
        :raises SyncRuntimeUnresponsiveError: The worker exceeds control or cancellation-acknowledgement budgets.
        :raises ConfigurationError: Wait arguments or the supplied mark are invalid.
        """
        return [
            event.payload
            for event in self._expect(matcher, count, timeout, description, require_complete, since)
        ]


class _PendingExpectation(PendingOperation):
    """Adapt the shared expectation engine to finite blocking-actor work slices.

    :ivar owner: Watcher supplying lifecycle checks, result reservations, and active-wait accounting.
    :ivar state: Shared independent-cursor expectation state advanced on the actor thread.
    :ivar poll_timeout: Suggested finite native receive budget before the next engine slice.
    """

    def __init__(self, owner, state: ExpectationState) -> None:
        """Wrap an already registered shared expectation for incremental actor execution.

        :param owner: Blocking watcher owning active-expectation accounting.
        :param state: Validated and bound shared expectation state.
        """
        self.owner, self.state = owner, state
        self.poll_timeout = 0.0

    def step(self):
        """Advance one bounded scan and finalize reservations when the wait terminates.

        :returns: A completion flag and detached matched envelopes when complete.
        :raises EventTimeoutError: The final eligible range contains insufficient matches.
        :raises BaseException: The engine reports cancellation, known failure, loss, or callback failure.
        """
        try:
            decision = self.state.step()
            self.poll_timeout = 0.0 if decision.scanned else 0.01
            if decision.status is ExpectationStatus.TIMEOUT:
                raise self.state.timeout_error()
            if decision.status is ExpectationStatus.MATCHED:
                self.state.finish()
                self.owner._active_expectations -= 1
                return True, self.state.results
            return False, None
        except BaseException as error:
            self.state.finish(error, cancelled=isinstance(error, CommandCancelled))
            self.owner._active_expectations -= 1
            raise


class SyncEventWatcher(SyncEventScope[T]):
    """Own one single-use blocking observation context and its continuously collecting worker.

    The actor owns Journal mutation, decoding, matching, and diagnostic collection.
    Report rendering runs in the creating caller; no asyncio loop drives this watcher.

    :ivar factory: Reusable blocking observer supplying immutable policies and adapter dependencies.
    :ivar source: Normalized existing source selected for this observation.
    :ivar decoder: Synchronous payload decoder invoked by the actor.
    :ivar access: Payload copying and accounted-size strategy.
    :ivar start_position: Requested native transport start policy.
    :ivar decoding: Unknown-type and decoding-error policy.
    :ivar identity: Optional run, worker, and test identifiers.
    :ivar _clock: Shared monotonic and UTC clock.
    :ivar observer_id: Stable unique identity of this observation.
    :ivar _runtime: Bounded single-owner blocking command actor.
    :ivar _journal: Canonical retained history mutated only by the actor.
    :ivar _diagnostics: Bounded collector owned by the actor while running.
    :ivar _diagnostics_proxy: Public access facade dispatching live reads to the actor.
    :ivar state: Current observation lifecycle state.
    :ivar cleanup_state: Not-started, complete, or failed resource-cleanup outcome.
    :ivar terminal_error: First known startup or continuous-observation failure.
    :ivar _session: Owned blocking transport session after successful open, or None.
    :ivar readiness: Resolved transport boundary and capabilities after successful open, or None.
    :ivar _entered: Whether this single-use context has already been entered.
    :ivar _scope_count: Lifetime number of explicitly created child scopes.
    :ivar _result_bytes: Accounted bytes reserved by unfinished matched results.
    :ivar _active_expectations: Number of registered unfinished expectations.
    :ivar _cleanup_error: Retained normalized resource-cleanup failure, if any.
    :ivar _stop_error: Retained actor-stop failure, if any.
    :ivar _finish_error: Retained cleanup-command dispatch or execution failure, if any.
    :ivar _cached_snapshot: Last detached Journal snapshot cached for access after stop, or None.
    :ivar _cached_diagnostics: Last detached collector snapshot cached for access after stop, or None.
    :ivar _cached_export: Last full diagnostic export cached before actor stop, or None.
    :ivar reporters: Watcher-local reporter instances bound from the factory prototypes.
    :ivar diagnostics: Detached diagnostic access facade.
    """

    def __init__(self, factory, source, decoder, access, start, decoding, identity) -> None:
        """Create lazy actor state, a bounded Journal, diagnostics, and the root scope.

        :param factory: Blocking observer supplying policies and resolved transport.
        :param source: Normalized source selection for this independent observation.
        :param decoder: Synchronous payload decoder selected by the factory.
        :param access: Payload isolation and size-accounting strategy.
        :param start: Requested native start position.
        :param decoding: Unknown-type and decoding-error policy.
        :param identity: Optional run, worker, and test identity.
        """
        self.factory, self.source, self.decoder, self.access = factory, source, decoder, access
        self.start_position, self.decoding, self.identity = start, decoding, identity
        self._clock = factory.clock
        self.observer_id = uuid4().hex
        self._runtime = ThreadRuntime(
            factory.lifecycle.sync_control_timeout, factory.lifecycle.cancellation_grace
        )
        self._journal = EventJournal(self.observer_id, factory.journal)
        self._diagnostics = DiagnosticCollector(
            self.observer_id,
            factory.diagnostic_policy,
            clock=self._clock,
            clone_payload=access.clone_for_user,
        )
        self._diagnostics_proxy = _SyncDiagnostics(self)
        self.state, self.cleanup_state = LifecycleState.CREATED, "not_started"
        self.terminal_error = None
        self._session: SyncTransportSession | None = None
        self.readiness: TransportReadiness | None = None
        self._entered = False
        self._scope_count = self._result_bytes = self._active_expectations = 0
        self._cleanup_error: BaseException | None = None
        self._stop_error: BaseException | None = None
        self._finish_error: BaseException | None = None
        self._cached_snapshot: JournalSnapshot | None = None
        self._cached_diagnostics: DiagnosticSnapshot | None = None
        self._cached_export: DiagnosticExport | None = None
        self.reporters = self._bind_reporters()
        super().__init__(self)

    @property
    def diagnostics(self):
        """Return the facade for owner-thread diagnostic reads and post-stop caches.

        :returns: The watcher-local diagnostic access facade.
        """
        return self._diagnostics_proxy

    def _bind_reporters(self):
        """Bind watcher-local reporters using aliases and the original transport display name.

        :returns: Successfully bound reporter instances; ordinary binding failures become safe diagnostic records.
        """
        return bind_observation_reporters(
            self.factory.reporters,
            self.factory.diagnostic_policy,
            self._diagnostics,
            transport=getattr(
                self.factory.transport, "transport_name", type(self.factory.transport).__name__
            ),
            source=self.source.name,
        )

    def _raise_closed(self):
        """Raise the known terminal failure or the ordinary closed-watcher error.

        :raises ObserverFailedError: A retained terminal failure takes precedence over closure.
        :raises ObserverClosedError: No ready runtime remains and no terminal failure is known.
        """
        if self.terminal_error is not None:
            raise ObserverFailedError(
                "Observer background operation failed",
                observer_id=self.observer_id,
                diagnostics=self._cached_diagnostics,
                cause=self.terminal_error,
            ) from self.terminal_error
        raise ObserverClosedError("Watcher is not open", observer_id=self.observer_id)

    def _check_open(self):
        """Check process ownership and inspect continuity before allowing actor work.

        :raises ProcessAffinityError: The watcher crossed its creating process boundary.
        :raises ObserverFailedError: A persistent session or Core failure is known.
        :raises ObserverClosedError: The lifecycle state is not READY.
        """
        if os.getpid() != self._runtime.pid:
            raise ProcessAffinityError("Watcher belongs to another process")
        if self._session is not None and self.state is LifecycleState.READY:
            failure = self._session.failure()
            if failure is not None:
                self._fail(failure)
        if self.terminal_error is not None:
            raise ObserverFailedError(
                "Observer background operation failed",
                observer_id=self.observer_id,
                diagnostics=self._diagnostics.snapshot(),
                cause=self.terminal_error,
            ) from self.terminal_error
        if self.state is not LifecycleState.READY:
            raise ObserverClosedError("Watcher is not ready or has closed", observer_id=self.observer_id)

    def _fail(self, error):
        """Retain the first terminal failure and record safe continuity metadata once.

        :param error: Original startup, transport, decoding, retention, or runtime failure.
        """
        if self.terminal_error is None:
            self.terminal_error = error
            self.state = LifecycleState.FAILED
            self._diagnostics.record(
                "terminal_failure",
                error_code=getattr(error, "code", "runtime_error"),
                error_type=type(error).__name__,
            )

    def _startup(self):
        """Open the owned blocking session and validate its independent READY contract.

        An immediate finite poll checks already-known failure before entry succeeds.
        The shared OpenContext carries the cooperative startup deadline to the adapter.

        :raises UnsupportedCapabilityError: The requested start, reader isolation, or settlement is unsupported.
        :raises ConfigurationError: The blocking interface exposes asynchronous callbacks or an awaitable open result.
        :raises ObserverStartupTimeoutError: Startup exceeds its absolute shared-clock deadline.
        :raises TransportError: The adapter cannot establish the requested ready subscription.
        :raises BaseException: Native startup or the initial continuity check fails.
        """
        self.state = LifecycleState.STARTING
        deadline = self._clock.monotonic() + self.factory.lifecycle.startup_timeout
        context = OpenContext(
            self.observer_id, deadline, self.factory.buffers, self.identity, self._diagnostics, self._clock
        )
        try:
            if self.start_position.mode.value not in self.factory.transport.capabilities.start_modes:
                raise UnsupportedCapabilityError("Transport does not support the requested start mode")
            session = self.factory.transport.open(self.source, self.start_position, context=context)
            if inspect.isawaitable(session):
                if inspect.iscoroutine(session):
                    session.close()
                raise ConfigurationError("Blocking transport open() returned an awaitable")
            # Own an allocated handle before validation so rollback can close it.
            self._session = session
            for method in ("poll", "failure", "close"):
                callback = getattr(session, method, None)
                if not callable(callback) or inspect.iscoroutinefunction(callback):
                    raise ConfigurationError(
                        "Blocking sessions require synchronous poll(), failure(), and close()"
                    )
            assert self._session is not None
            self.readiness = self._session.readiness
            capabilities = self.readiness.resolved_capabilities
            if self.start_position.mode.value not in capabilities.start_modes or not {
                "independent_reader",
                "dedicated_subscription",
            }.intersection(capabilities.isolation_modes):
                raise UnsupportedCapabilityError("Session cannot provide independent requested observation")
            if capabilities.supports_settlement:
                settle = getattr(self._session, "settle", None)
                if not callable(settle):
                    raise UnsupportedCapabilityError("Session advertises settlement without implementing it")
                if inspect.iscoroutinefunction(settle):
                    raise ConfigurationError("Blocking settlement requires synchronous settle()")
            if self._clock.monotonic() >= deadline:
                raise TimeoutError("Observer startup deadline expired")
            self.state = LifecycleState.READY
            self._poll(0)
            self._check_open()
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
        except BaseException as error:
            self.state, self.terminal_error = LifecycleState.FAILED, error
            self._diagnostics.record(
                "startup_failure", error_type=type(error).__name__, error_code=getattr(error, "code", None)
            )
            if isinstance(error, TimeoutError):
                raise ObserverStartupTimeoutError("Observer startup deadline expired", cause=error) from error
            raise

    def _poll(self, timeout):
        """Receive at most one delivery and commit admission before optional settlement.

        Known receive failures are recorded independently of business-message matching.
        Exceptions are retained as terminal failures for the next state check.

        :param timeout: Finite nonnegative maximum native receive wait in seconds.
        """
        if self.state is not LifecycleState.READY:
            self._runtime._stop.wait(timeout)
            return
        try:
            assert self._session is not None and self.readiness is not None
            error = self._session.failure()
            if error is not None:
                raise error
            raw = self._session.poll(timeout)
            if raw is not None:
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
                if self.readiness.resolved_capabilities.supports_settlement:
                    if raw.delivery_id is None:
                        raise TransportError("Settlement transport supplied no delivery token")
                    result = getattr(self._session, "settle")(raw.delivery_id, outcome)
                    if inspect.isawaitable(result):
                        if inspect.iscoroutine(result):
                            result.close()
                        raise ConfigurationError("Blocking session settle() returned an awaitable")
        except BaseException as error:
            self._fail(
                error
                if isinstance(error, EventObserverError)
                else TransportError("Blocking transport receive failed", cause=error)
            )

    def _copy_event(self, event):
        """Detach an event payload while preserving its immutable admission envelope.

        :param event: Canonical Journal event to isolate before exposing it to user code.
        :returns: An event with an independently copied payload.
        :raises PayloadIsolationError: The configured strategy cannot safely clone the payload.
        """
        try:
            return replace(event, payload=self.access.clone_for_user(event.payload))
        except PayloadIsolationError:
            raise
        except Exception as error:
            raise PayloadIsolationError("Payload isolation failed") from error

    def _bind(self, matcher):
        """Normalize a matcher against known decoder types and the shared copy strategy.

        :param matcher: Payload predicate or event matcher requiring schema-aware binding.
        :returns: A bound matcher for isolated callback arguments.
        :raises ConfigurationError: The matcher or its known schema configuration is invalid.
        """
        known = (
            (bytes, type(None))
            if isinstance(self.decoder, RawDecoder)
            else getattr(self.decoder, "known_message_types", None)
        )
        return bind_matcher(matcher, known, copy_event=self._copy_event)

    def _make_scope(self, parent, matcher, *, name, start):
        """Create one bounded child view without opening or seeking a transport reader.

        :param parent: Existing scope whose lower bound and filters must be preserved.
        :param matcher: Additional correlation matcher bound after inherited filters.
        :param name: Optional caller-supplied child label.
        :param start: Optional boundary narrowing the inherited range.
        :returns: A child scope sharing the watcher identity, Journal, and session.
        :raises ScopeResourceLimitError: The lifetime child-scope limit is exhausted.
        :raises ConfigurationError: The boundary lies in the future or matcher binding fails.
        :raises ObserverFailedError: Continuity failed before the scope could be created.
        """
        self._check_open()
        if self._scope_count >= self.factory.runtime_limits.max_scopes:
            raise ScopeResourceLimitError("Scope limit exhausted")
        start = start or ScopeStart.all_retained()
        lower = parent._lower_bound
        if start.mode is ScopeStartMode.FROM_NOW:
            lower = max(lower, self._journal.next_sequence)
        elif start.mode is ScopeStartMode.FROM_SEQUENCE:
            if start.sequence is None or start.sequence > self._journal.next_sequence:
                raise ConfigurationError("Scope sequence lies in the future")
            lower = max(lower, start.sequence)
        bound = self._bind(matcher)
        self._scope_count += 1
        return SyncEventScope(self, lower=lower, filters=(*parent._filters, bound), name=name)

    def _scope_snapshot(self, scope):
        """Copy one scope's definition, completeness, and retained expectation summaries.

        :param scope: Existing immutable scope whose diagnostic identity selects summaries.
        :returns: A detached ScopeSnapshot for the original scope range.
        """
        summaries = tuple(
            item for item in self._diagnostics.snapshot().expectations if item.scope_id == scope._scope_id
        )
        return ScopeSnapshot(
            self.observer_id,
            scope._scope_id,
            scope._lower_bound,
            self._journal.complete(scope._lower_bound),
            summaries,
            scope._name,
            len(scope._filters),
        )

    def _mark(self):
        """Record the next Journal sequence after checking that observation remains ready.

        :returns: A watcher-local mark with informational shared-clock timestamps.
        :raises ObserverFailedError: Continuity has already failed.
        :raises ObserverClosedError: The watcher is no longer ready.
        """
        self._check_open()
        return ObservationMark(
            self.observer_id, self._journal.next_sequence, self._clock.utcnow(), self._clock.monotonic()
        )

    def _check_expectation(self):
        """Prioritize command cancellation before known failure and watcher lifecycle.

        :raises CommandCancelled: The caller requested cancellation of this actual actor command.
        :raises ObserverFailedError: A persistent observation failure is known.
        :raises ObserverClosedError: The watcher no longer accepts expectations.
        """
        command = self._runtime.current_command
        if command is not None and command.cancelled.is_set():
            raise CommandCancelled()
        self._check_open()

    def _reserve_result(self, size):
        """Check and reserve bytes for one detached unfinished expectation result.

        :param size: Journal-accounted bytes belonging to the candidate result.
        :raises ExpectationResourceLimitError: Aggregate unfinished-result bytes exceed the runtime limit.
        """
        if self._result_bytes + size > self.factory.runtime_limits.max_result_bytes:
            raise ExpectationResourceLimitError("Pending result bytes budget exhausted")
        self._result_bytes += size

    def _release_result(self, size):
        """Release an expectation's accounted result bytes after success, failure, or cancellation.

        :param size: Bytes previously reserved by the expectation being finalized.
        """
        self._result_bytes -= size

    def _begin_expectation(self, scope, matcher, count, timeout, require_complete, since, began):
        """Validate and register shared expectation state without waiting for a match.

        :param scope: Immutable scope supplying the original identity, filters, and lower range.
        :param matcher: Matcher to bind against known decoder schemas.
        :param count: Positive number of distinct matching admissions required.
        :param timeout: Positive finite public admission-window duration in seconds.
        :param require_complete: Whether relevant retention loss must invalidate the result.
        :param since: Optional same-watcher mark narrowing this scope's range.
        :param began: Caller-captured monotonic start including command handoff time.
        :returns: Incremental work advanced by the actor between finite receive polls.
        :raises ConfigurationError: Wait arguments or mark ownership and position are invalid.
        :raises ExpectationResourceLimitError: Requested results or active registrations exceed policy.
        :raises ObserverFailedError: Continuity failed before registration.
        """
        self._check_open()
        limits = self.factory.runtime_limits
        lower = effective_lower_bound(
            observer_id=self.observer_id,
            lower_bound=scope._lower_bound,
            since=since,
            next_sequence=self._journal.next_sequence,
        )
        validate_expectation(count, timeout, require_complete, max_result_events=limits.max_result_events)
        if self._active_expectations >= limits.max_active_waiters:
            raise ExpectationResourceLimitError("Expectation resource limit reached")
        bound = self._bind(matcher)
        state = ExpectationState(
            observer_id=self.observer_id,
            expectation_id=uuid4().hex,
            scope_id=scope._scope_id,
            journal=self._journal,
            lower_bound=lower,
            filters=scope._filters,
            matcher=bound,
            count=count,
            timeout=timeout,
            began=began,
            deadline=began + timeout,
            require_complete=require_complete,
            scan_batch_events=limits.scan_batch_events,
            clock=self._clock,
            copy_event=self._copy_event,
            diagnostics=self._diagnostics,
            check_state=self._check_expectation,
            reserve_result=self._reserve_result,
            release_result=self._release_result,
        )
        self._active_expectations += 1
        return _PendingExpectation(self, state)

    def _dispatch(self, operation, *, budget=None, report=True, force_report=False):
        """Run one bounded actor command and deliver prepared automatic reports in the caller.

        Ordinary reporter failures do not replace the operation result. A report is
        prepared on the owning actor only when its previous command is acknowledged.

        :param operation: Owner-thread callback returning a value or incremental pending work.
        :param budget: Optional caller-control duration overriding the actor default.
        :param report: Whether this command may deliver newly prepared reports.
        :param force_report: Request failure/context diagnostics even without a new expectation.
        :returns: The actual completed actor-command result.
        :raises SyncRuntimeUnresponsiveError: Control or actual cancellation acknowledgement exceeds its budget.
        :raises BaseException: The command fails or the caller is interrupted.
        """
        error = None
        try:
            return self._runtime.call(operation, budget=budget)
        except BaseException as caught:
            error = caught
            raise
        finally:
            if report and self._runtime.running and self.reporters:
                try:
                    exported = self._runtime.call(
                        lambda: self._diagnostics.prepare_report(force=force_report or error is not None)
                    )
                    if exported is not None:
                        for reporter in self.reporters:
                            try:
                                reporter.emit(exported)
                            except Exception:
                                self._runtime.call(self._diagnostics.record_reporter_error)
                except Exception:
                    try:
                        self._runtime.call(self._diagnostics.record_reporter_error)
                    except Exception:
                        pass

    def _call(self, operation, *, report=True):
        """Dispatch one immediate actor operation with the default command budget.

        :param operation: Callback executed on the owner thread.
        :param report: Whether to deliver newly prepared automatic reports after completion.
        :returns: The actual completed operation value.
        :raises BaseException: Runtime dispatch, operation execution, or caller interruption fails.
        """
        return self._dispatch(operation, report=report)

    def __enter__(self) -> SyncEventWatcher[T]:
        """Start the worker and return only after its independent session confirms READY.

        Startup failure retains its original exception while bounded cleanup is attempted.

        :returns: This single-use ready watcher.
        :raises ConfigurationError: The context has already been entered.
        :raises EventObserverError: Caller affinity, native startup, or actor control prevents entry.
        :raises BaseException: Startup is interrupted or another startup failure occurs.
        """
        self._runtime.guard()
        if self._entered:
            raise ConfigurationError("A watch context can be entered only once")
        self._entered = True
        try:
            self._runtime.start(
                self._startup,
                self._poll,
                self._shutdown,
                budget=self.factory.lifecycle.startup_timeout + self.factory.lifecycle.sync_control_timeout,
            )
            return self
        except BaseException as error:
            if self._cached_diagnostics is not None and self.reporters:
                # Failed startup already finished on the worker. Rendering stays in the caller.
                try:
                    self._diagnostics.emit(self.reporters, force=True)
                except Exception:
                    pass
            try:
                self._runtime.stop()
            except BaseException as secondary:
                self._stop_error = secondary
                if isinstance(secondary, _INTERRUPTION_ERRORS):
                    raise
                error.add_note("Sync startup cleanup also failed within its resource budget")
            raise

    def snapshot(self):
        """Copy retained Journal state through the worker or return its post-stop cache.

        :returns: A detached JournalSnapshot, or None before a snapshot has been cached.
        :raises EventObserverError: Caller affinity or command control prevents the read.
        """
        self._runtime.guard()
        if not self._runtime.running:
            return self._cached_snapshot
        return self._call(lambda: self._journal.snapshot(self._copy_event), report=False)

    def _cleanup(self):
        """Close the owned session once and retain the resulting cleanup outcome.

        Successful cleanup is idempotent; a previously failed cleanup re-raises its
        retained typed failure without repeating native resource release.

        :raises ObserverCleanupError: Native cleanup fails or a retained cleanup failure already exists.
        :raises ObserverCleanupTimeoutError: The session cannot release resources by its cleanup deadline.
        """
        if self.cleanup_state == "complete":
            return
        if self._cleanup_error is not None:
            raise self._cleanup_error
        self.state = LifecycleState.STOPPING
        deadline = self._clock.monotonic() + self.factory.lifecycle.shutdown_timeout
        try:
            if self._session is not None:
                close = getattr(self._session, "close", None)
                if not callable(close) or inspect.iscoroutinefunction(close):
                    raise ObserverCleanupError("Blocking session has no synchronous close()")
                result = close(deadline=deadline)
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise ObserverCleanupError("Blocking session close() returned an awaitable")
            self.cleanup_state, self.state = "complete", LifecycleState.CLOSED
        except BaseException as error:
            normalized = (
                error
                if isinstance(error, (ObserverCleanupError, *_INTERRUPTION_ERRORS))
                else ObserverCleanupError("Observer resource cleanup failed", cause=error)
            )
            self._cleanup_error = normalized
            self.cleanup_state, self.state = "failed", LifecycleState.FAILED
            self._diagnostics.record(
                "cleanup_failure",
                error_type=type(error).__name__,
                error_code=getattr(error, "code", "cleanup_failure"),
            )
            if normalized is error:
                raise
            raise normalized from error
        finally:
            self._diagnostics.record("closed", cleanup_state=self.cleanup_state)
            self._cache()

    def _cache(self):
        """Retain detached Journal and diagnostic snapshots for reads after actor shutdown.

        Ordinary copy failures become safe diagnostic records.
        """
        try:
            self._cached_snapshot = self._journal.snapshot(self._copy_event)
            self._cached_diagnostics = self._diagnostics.snapshot()
        except Exception:
            self._diagnostics.record("diagnostic_cache_failure")

    def _shutdown(self):
        """Perform worker-owned cleanup and refresh detached caches even when cleanup fails.

        :raises ObserverCleanupError: The owned session cannot finish cleanup successfully.
        """
        try:
            self._cleanup()
        finally:
            self._cache()

    def _stop_runtime(self, primary_error):
        """Stop the actor while preserving primary failures and new caller interruptions.

        :param primary_error: Already propagating body, startup, or cleanup-command failure, or None.
        :raises BaseException: Stop fails without a primary error, or a new caller interruption takes priority.
        """
        try:
            self._runtime.stop()
        except BaseException as error:
            self._stop_error = error
            if primary_error is None or isinstance(error, _INTERRUPTION_ERRORS):
                raise
            primary_error.add_note("Sync runtime cleanup also failed; inspect the watcher's cleanup state")

    def _finish(self, exc_type=None, exc_value=None, traceback=None):
        """Finalize context diagnostics, native resources, full export cache, and actor shutdown.

        An application-body failure keeps priority over ordinary cleanup errors. A new
        caller interruption propagates after bounded cleanup has been attempted.

        :param exc_type: Body exception type, None for normal exit, or False for explicit close.
        :param exc_value: Original application-body exception, or None.
        :param traceback: Body traceback accepted for context-manager compatibility.
        :raises ObserverFailedError: Normal context exit finds a known terminal observation failure.
        :raises ObserverCleanupError: Resource cleanup fails without a primary application exception.
        :raises BaseException: Dispatch, stop, or a new caller interruption must propagate.
        """
        self._runtime.guard()
        if not self._runtime.running:
            if exc_type is not False and exc_value is None and self.terminal_error is not None:
                self._raise_closed()
            return
        primary = exc_value

        def finish():
            """Record any body failure and release owned resources on the actor thread.

            :raises ObserverCleanupError: Native cleanup fails.
            :raises ObserverFailedError: Normal context exit finds a known terminal failure after cleanup.
            """
            if exc_value is not None:
                self._diagnostics.record("context_failure", error_type=type(exc_value).__name__)
            cleanup_error = None
            try:
                self._cleanup()
            except _INTERRUPTION_ERRORS:
                raise
            except BaseException as error:
                cleanup_error = error
            if exc_type is not False and exc_value is None and self.terminal_error is not None:
                try:
                    self._raise_closed()
                except ObserverFailedError as error:
                    if cleanup_error is not None:
                        error.add_note("Observer cleanup also failed; see safe observer diagnostics")
                    raise
            if cleanup_error is not None:
                raise cleanup_error

        try:
            self._dispatch(
                finish,
                budget=self.factory.lifecycle.shutdown_timeout + self.factory.lifecycle.sync_control_timeout,
                force_report=exc_value is not None,
            )
        except BaseException as error:
            self._finish_error = error
            if exc_value is None or isinstance(error, _INTERRUPTION_ERRORS):
                primary = error
                raise
            exc_value.add_note("Observer cleanup command also failed; inspect the watcher's cleanup state")
        finally:
            try:
                if self._runtime.running:
                    try:
                        self._cached_export = self._runtime.call(self._diagnostics.export)
                    except Exception:
                        pass
            finally:
                self._stop_runtime(primary)

    def close(self):
        """Explicitly release resources without raising an already-known observation failure.

        :raises ObserverCleanupError: Native resource cleanup fails.
        :raises BaseException: Caller interruption or actor control prevents bounded closure.
        """
        self._finish(False)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Finish observation while preserving body-error and new-interruption priority.

        :param exc_type: Original application-body exception type, or None for normal exit.
        :param exc_value: Original application-body exception instance, or None.
        :param traceback: Original application-body traceback, or None.
        :returns: None, so application-body exceptions are never suppressed.
        :raises ObserverFailedError: Normal exit discovers a persistent observation failure.
        :raises ObserverCleanupError: Cleanup fails without a primary body exception.
        :raises BaseException: Actor control or caller interruption must propagate.
        """
        self._finish(exc_type, exc_value, traceback)
