"""Stable public errors. Messages from native causes are never copied automatically."""

from typing import Any


class EventObserverError(Exception):
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
        super().__init__(message or self.code.replace("_", " "))
        self.observer_id = observer_id
        self.expectation_id = expectation_id
        self.diagnostics = diagnostics
        self.cause = cause
        for key, value in details.items():
            setattr(self, key, value)


class ConfigurationError(EventObserverError):
    code = "configuration_error"


class EventAssertionError(AssertionError):
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
        super().__init__(message or self.code.replace("_", " "))
        self.observer_id = observer_id
        self.expectation_id = expectation_id
        self.diagnostics = diagnostics


class EventTimeoutError(EventAssertionError):
    code = "event_timeout"


class TransportError(EventObserverError):
    code = "transport_error"


class EventDecodingError(EventObserverError):
    code = "event_decoding_error"


class MissingOptionalDependencyError(ConfigurationError):
    code = "missing_optional_dependency"


class UnsupportedCapabilityError(ConfigurationError):
    code = "unsupported_capability"


class MatcherConfigurationError(ConfigurationError):
    code = "matcher_configuration"


class DiagnosticConfigurationError(ConfigurationError):
    code = "diagnostic_configuration"


class TransportConnectionError(TransportError):
    code = "transport_connection"


class TransportAuthenticationError(TransportError):
    code = "transport_authentication"


class TransportAuthorizationError(TransportError):
    code = "transport_authorization"


class TransportReadinessError(TransportError):
    code = "transport_readiness"


class TransportSubscriptionError(TransportError):
    code = "transport_subscription"


class StartPositionUnavailableError(TransportError):
    code = "start_position_unavailable"


class TopologyChangedError(TransportError):
    code = "topology_changed"


class TransportInterruptedError(TransportError):
    code = "transport_interrupted"


class TransportBackpressureError(TransportError):
    code = "transport_backpressure"


class UnexpectedStreamEndError(TransportError):
    code = "unexpected_stream_end"


class NullPayloadError(EventDecodingError):
    code = "null_payload"


class InvalidPayloadError(EventDecodingError):
    code = "invalid_payload"


class UnknownMessageTypeError(EventDecodingError):
    code = "unknown_message_type"
    reason: str | None = None


class AmbiguousMessageTypeError(EventDecodingError):
    code = "ambiguous_message_type"


class TypeResolverError(EventDecodingError):
    code = "type_resolver"


class UnsupportedMessageTypeError(EventDecodingError):
    code = "unsupported_message_type"


class ObserverStartupTimeoutError(EventObserverError):
    code = "observer_startup_timeout"


class ObserverFailedError(EventObserverError):
    code = "observer_failed"


class ObserverClosedError(EventObserverError):
    code = "observer_closed"


class ObserverCleanupError(EventObserverError):
    code = "observer_cleanup"


class JournalOverflowError(EventObserverError):
    code = "journal_overflow"


class EventTooLargeError(EventObserverError):
    code = "event_too_large"


class IncompleteObservationError(EventObserverError):
    code = "incomplete_observation"


class ExpectationResourceLimitError(EventObserverError):
    code = "expectation_resource_limit"


class ScopeResourceLimitError(EventObserverError):
    code = "scope_resource_limit"


class PayloadIsolationError(EventObserverError):
    code = "payload_isolation"


class InvalidMatcherResultError(EventObserverError):
    code = "invalid_matcher_result"


class MatcherEvaluationError(EventObserverError):
    code = "matcher_evaluation"


class EventLoopMismatchError(EventObserverError):
    code = "event_loop_mismatch"


class ThreadAffinityError(EventObserverError):
    code = "thread_affinity"


class ProcessAffinityError(EventObserverError):
    code = "process_affinity"


class SyncAPIInAsyncContextError(EventObserverError):
    code = "syncapi_in_async_context"


class ReentrantSyncCallError(EventObserverError):
    code = "reentrant_sync_call"


class SyncRuntimeUnresponsiveError(EventObserverError):
    code = "sync_runtime_unresponsive"


class DiagnosticExportError(EventObserverError):
    code = "diagnostic_export"


class UnsupportedFieldPresenceError(MatcherConfigurationError):
    code = "unsupported_field_presence"


class ObserverCleanupTimeoutError(ObserverCleanupError):
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
