"""Stable public errors. Messages from native causes are never copied automatically."""

from typing import Any


class EventObserverError(Exception):
    """Represent an operational or configuration failure with detached diagnostic references.

    :ivar code: Stable machine-readable identifier for this error category.
    :ivar sequence: Optional last relevant watcher-local Journal sequence.
    :ivar observer_id: Optional identity of the affected watcher.
    :ivar expectation_id: Optional identity of the affected expectation.
    :ivar diagnostics: Detached diagnostic snapshot or reference associated with the failure.
    :ivar cause: Original typed failure retained separately from the safe exception summary.
    """

    code = "event_observer_error"
    sequence: int | None = None

    def __init__(
        self,
        message: str | None = None,
        *,
        observer_id: str | None = None,
        expectation_id: str | None = None,
        diagnostics: Any = None,
        cause: BaseException | None = None,
        **details: Any,
    ) -> None:
        """Initialize a safe operational summary and attach caller-supplied diagnostic context.

        :param message: Safe human-readable summary; defaults to the error code with underscores replaced.
        :param observer_id: Identity of the affected watcher, when known.
        :param expectation_id: Identity of the affected expectation, when known.
        :param diagnostics: Detached snapshot or diagnostic reference.
        :param cause: Original failure object; exception chaining is established by the raising caller.
        :param details: Additional error-specific attributes to attach to this instance.
        """
        super().__init__(message or self.code.replace("_", " "))
        self.observer_id = observer_id
        self.expectation_id = expectation_id
        self.diagnostics = diagnostics
        self.cause = cause
        for key, value in details.items():
            setattr(self, key, value)


class ConfigurationError(EventObserverError):
    """Reject invalid or mutually inconsistent observer settings before useful work begins.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "configuration_error"


class EventAssertionError(AssertionError):
    """Represent an unsuccessful event assertion when sufficient observation data is available.

    :ivar code: Stable machine-readable identifier for this error category.
    :ivar sequence: Optional last relevant Journal sequence.
    :ivar observer_id: Optional identity of the watcher used by the assertion.
    :ivar expectation_id: Optional identity of the failed expectation.
    :ivar diagnostics: Detached diagnostics describing the unsuccessful assertion.
    """

    code = "event_assertion_error"
    sequence: int | None = None

    def __init__(
        self,
        message: str | None = None,
        *,
        observer_id: str | None = None,
        expectation_id: str | None = None,
        diagnostics: Any = None,
    ) -> None:
        """Initialize an assertion failure without classifying it as an operational interruption.

        :param message: Safe assertion summary; defaults to the readable error code.
        :param observer_id: Identity of the watcher used by the assertion.
        :param expectation_id: Identity of the failed expectation.
        :param diagnostics: Detached evidence explaining the assertion outcome.
        """
        super().__init__(message or self.code.replace("_", " "))
        self.observer_id = observer_id
        self.expectation_id = expectation_id
        self.diagnostics = diagnostics


class EventTimeoutError(EventAssertionError):
    """Report that the requested matching event count was not reached within its admission window.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "event_timeout"


class TransportError(EventObserverError):
    """Provide the base category for normalized transport-client failures.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "transport_error"


class EventDecodingError(EventObserverError):
    """Provide the base category for failures while turning a raw delivery into a payload.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "event_decoding_error"


class MissingOptionalDependencyError(ConfigurationError):
    """Report that a requested integration lacks its optional runtime dependency.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "missing_optional_dependency"


class UnsupportedCapabilityError(ConfigurationError):
    """Reject observation semantics that the selected adapter or session does not implement.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "unsupported_capability"


class MatcherConfigurationError(ConfigurationError):
    """Reject invalid matcher composition, field paths, or schema binding.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "matcher_configuration"


class DiagnosticConfigurationError(ConfigurationError):
    """Reject invalid diagnostic limits, disclosure settings, or redactor configuration.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "diagnostic_configuration"


class TransportConnectionError(TransportError):
    """Report failure to establish or maintain a native broker connection.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "transport_connection"


class TransportAuthenticationError(TransportError):
    """Report that the native broker rejected authentication.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "transport_authentication"


class TransportAuthorizationError(TransportError):
    """Report that broker permissions are insufficient for the requested reader.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "transport_authorization"


class TransportReadinessError(TransportError):
    """Report that reader preparation could not establish the promised readiness guarantees.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "transport_readiness"


class TransportSubscriptionError(TransportError):
    """Report invalid subscription setup, delivery metadata, or stream representation.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "transport_subscription"


class StartPositionUnavailableError(TransportError):
    """Reject a requested native start position whose required history is unavailable.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "start_position_unavailable"


class TopologyChangedError(TransportError):
    """Report a detected change to a session topology that was fixed at startup.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "topology_changed"


class TransportInterruptedError(TransportError):
    """Report a known interruption that prevents claiming continuous observation.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "transport_interrupted"


class TransportBackpressureError(TransportError):
    """Report that a delivery cannot fit safely into the bounded transport handoff.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "transport_backpressure"


class UnexpectedStreamEndError(TransportError):
    """Report unexpected termination of a receive stream intended to remain live.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "unexpected_stream_end"


class NullPayloadError(EventDecodingError):
    """Reject a null delivery for a decoder that requires message bytes.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "null_payload"


class InvalidPayloadError(EventDecodingError):
    """Report corrupt message bytes or a Protobuf message missing required fields.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "invalid_payload"


class UnknownMessageTypeError(EventDecodingError):
    """Identify an explicitly unknown message type eligible for the opt-in skip policy.

    :ivar code: Stable machine-readable identifier for this error category.
    :ivar reason: Optional stable discriminator failure reason, excluding the raw discriminator value.
    """

    code = "unknown_message_type"
    reason: str | None = None


class AmbiguousMessageTypeError(EventDecodingError):
    """Reject a type discriminator that identifies more than one candidate.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "ambiguous_message_type"


class TypeResolverError(EventDecodingError):
    """Report invalid type selection configuration or failure of a resolver callback.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "type_resolver"


class UnsupportedMessageTypeError(EventDecodingError):
    """Reject a resolver result that is not an allowed generated message class.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "unsupported_message_type"


class ObserverStartupTimeoutError(EventObserverError):
    """Report exhaustion of the overall observer startup deadline.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "observer_startup_timeout"


class ObserverFailedError(EventObserverError):
    """Expose a recorded terminal background failure to a waiter or normal context exit.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "observer_failed"


class ObserverClosedError(EventObserverError):
    """Reject an operation whose watcher is not ready or has already stopped.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "observer_closed"


class ObserverCleanupError(EventObserverError):
    """Report resource cleanup that failed or could not be completed.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "observer_cleanup"


class JournalOverflowError(EventObserverError):
    """Terminate observation when aggregate Journal limits are exceeded under FAIL.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "journal_overflow"


class EventTooLargeError(EventObserverError):
    """Reject a raw delivery before decoding because it exceeds the hard event-byte limit.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "event_too_large"


class IncompleteObservationError(EventObserverError):
    """Reject a strict expectation whose required Journal range has known or conservatively recorded loss.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "incomplete_observation"


class ExpectationResourceLimitError(EventObserverError):
    """Reject an expectation that exceeds waiter, count, result-byte, or search-state limits.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "expectation_resource_limit"


class ScopeResourceLimitError(EventObserverError):
    """Reject creation of more scope definitions than the watcher permits.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "scope_resource_limit"


class PayloadIsolationError(EventObserverError):
    """Report inability to detach or account for a payload safely.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "payload_isolation"


class InvalidMatcherResultError(EventObserverError):
    """Reject an asynchronous or malformed matcher or predicate result.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "invalid_matcher_result"


class MatcherEvaluationError(EventObserverError):
    """Report an evaluated matcher callback failure without failing unrelated waiters.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "matcher_evaluation"


class EventLoopMismatchError(EventObserverError):
    """Reject use of a loop-bound watcher from a different event loop.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "event_loop_mismatch"


class ThreadAffinityError(EventObserverError):
    """Reject synchronous-handle access outside its creating user thread.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "thread_affinity"


class ProcessAffinityError(EventObserverError):
    """Reject use of an active watcher from another process or after an unsafe fork.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "process_affinity"


class SyncAPIInAsyncContextError(EventObserverError):
    """Reject blocking facade calls made inside an active asyncio event loop.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "syncapi_in_async_context"


class ReentrantSyncCallError(EventObserverError):
    """Reject a blocking call from the runtime thread that would deadlock its own portal.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "reentrant_sync_call"


class SyncRuntimeUnresponsiveError(EventObserverError):
    """Report that a synchronous portal command or runtime shutdown exceeded its control guarantees.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "sync_runtime_unresponsive"


class DiagnosticExportError(EventObserverError):
    """Report failure of an explicit diagnostic export, including budget exhaustion.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "diagnostic_export"


class UnsupportedFieldPresenceError(MatcherConfigurationError):
    """Reject a presence check for a field without explicit supported presence semantics.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "unsupported_field_presence"


class ObserverCleanupTimeoutError(ObserverCleanupError):
    """Report exhaustion of the bounded observer resource-cleanup interval.

    :ivar code: Stable machine-readable identifier for this error category.
    """

    code = "observer_cleanup_timeout"


__all__ = [
    "EventObserverError",
    "ConfigurationError",
    "EventAssertionError",
    "EventTimeoutError",
    "TransportError",
    "EventDecodingError",
    "MissingOptionalDependencyError",
    "UnsupportedCapabilityError",
    "MatcherConfigurationError",
    "DiagnosticConfigurationError",
    "TransportConnectionError",
    "TransportAuthenticationError",
    "TransportAuthorizationError",
    "TransportReadinessError",
    "TransportSubscriptionError",
    "StartPositionUnavailableError",
    "TopologyChangedError",
    "TransportInterruptedError",
    "TransportBackpressureError",
    "UnexpectedStreamEndError",
    "NullPayloadError",
    "InvalidPayloadError",
    "UnknownMessageTypeError",
    "AmbiguousMessageTypeError",
    "TypeResolverError",
    "UnsupportedMessageTypeError",
    "ObserverStartupTimeoutError",
    "ObserverFailedError",
    "ObserverClosedError",
    "ObserverCleanupError",
    "JournalOverflowError",
    "EventTooLargeError",
    "IncompleteObservationError",
    "ExpectationResourceLimitError",
    "ScopeResourceLimitError",
    "PayloadIsolationError",
    "InvalidMatcherResultError",
    "MatcherEvaluationError",
    "EventLoopMismatchError",
    "ThreadAffinityError",
    "ProcessAffinityError",
    "SyncAPIInAsyncContextError",
    "ReentrantSyncCallError",
    "SyncRuntimeUnresponsiveError",
    "DiagnosticExportError",
    "UnsupportedFieldPresenceError",
    "ObserverCleanupTimeoutError",
]
