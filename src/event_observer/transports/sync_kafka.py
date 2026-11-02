"""Native blocking Kafka sessions using the shared serialized Consumer driver.

The driver performs native work on its one I/O worker. Blocking callers use a
condition-protected handoff; no coroutine scheduler or runtime loop is created.

:ivar __all__: Public native blocking Kafka adapter and session types.
"""

from __future__ import annotations

import threading
from typing import Any

from ..errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    ObserverCleanupTimeoutError,
    TransportError,
)
from ..models import (
    OpenContext,
    RawEvent,
    StartPosition,
    TransportCapabilities,
    TransportSource,
)
from . import kafka as _kafka
from .kafka import KafkaConfig, _KafkaNativeWorker, _capabilities
from .sync_base import validate_poll_timeout


def _load_native() -> Any:
    """Load the shared synchronous native Kafka client through its factory hook.

    :returns: The confluent_kafka module or a controlled shared native double.
    :raises ImportError: If the optional Kafka client is unavailable.
    """
    return _kafka._load_native()


class SyncKafkaAdapter:
    """Lazy factory for blocking readers using the same Kafka native algorithm.

    :ivar transport_name: Canonical report title matching the asynchronous adapter.
    :ivar config: Validated immutable Kafka connection and polling policy.
    :ivar capabilities: Independent partition-reader, replay, and envelope semantics.
    """

    transport_name = "KafkaAdapter"

    def __init__(self, config: KafkaConfig) -> None:
        """Store explicit configuration without importing or connecting a client.

        :param config: Immutable validated native connection and buffer settings.
        :returns: None; each open creates its own independent native worker.
        :raises ConfigurationError: If config is not a KafkaConfig.
        """
        if not isinstance(config, KafkaConfig):
            raise ConfigurationError("SyncKafkaAdapter requires KafkaConfig")
        self.config = config

    @property
    def capabilities(self) -> TransportCapabilities:
        """Return the shared Kafka capability contract without native imports.

        :returns: Supported replay, partition isolation, metadata, and flow control.
        """
        return _capabilities()

    def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> SyncKafkaSession:
        """Start the native worker and wait for its Fetch-confirmed READY boundary.

        :param source: Existing topic and optional explicit partition selection.
        :param start: Explicit requested replay policy and positions.
        :param context: Runtime clock, startup deadline, identity, and handoff limits.
        :returns: A fully READY blocking reader with one owned Consumer worker.
        :raises MissingOptionalDependencyError: If the optional Kafka client is unavailable.
        :raises TransportError: If native startup, positions, or Fetch readiness fail.
        :raises TimeoutError: If the startup deadline expires before readiness.
        """
        try:
            native = _load_native()
        except ImportError as error:
            raise MissingOptionalDependencyError(
                "Kafka requires the 'kafka' extra: pip install 'pytest-broker-observer[kafka]'"
            ) from error
        session = SyncKafkaSession(self.config, native, source, start, context)
        session._thread.start()
        try:
            with session._condition:
                remaining = max(0.0, context.startup_deadline - context.clock.monotonic())
                completed = session._condition.wait_for(
                    lambda: (
                        session._readiness is not None
                        or session._error is not None
                        or session._worker_finished
                    ),
                    timeout=remaining,
                )
                if session._error is not None:
                    raise session._error
                if (
                    not completed
                    or session._readiness is None
                    or context.clock.monotonic() >= context.startup_deadline
                ):
                    raise TimeoutError("Kafka startup deadline expired")
        except BaseException as primary:
            try:
                session.close(deadline=context.clock.monotonic() + 2.0)
            except BaseException as cleanup:
                setattr(primary, "cleanup_error", cleanup)
            raise
        return session


class SyncKafkaSession(_KafkaNativeWorker):
    """Condition-based bridge over the shared Kafka native worker algorithm."""

    def poll(self, timeout: float) -> RawEvent | None:
        """Read one bounded handoff event, prioritizing known native failure.

        :param timeout: Finite nonnegative maximum blocking time in seconds.
        :returns: One detached delivery, or None after an empty poll or intentional close.
        :raises TransportError: If the native worker has detected a terminal failure.
        :raises ConfigurationError: If timeout is invalid.
        """
        validate_poll_timeout(timeout)
        with self._condition:
            if self._error is not None:
                raise self._error
            if not self._queue and not self._stop.is_set() and not self._worker_finished:
                self._condition.wait(timeout=min(timeout, threading.TIMEOUT_MAX))
            if self._error is not None:
                raise self._error
            if self._stop.is_set() or not self._queue:
                return None
            raw = self._queue.popleft()
            self._condition.notify_all()
            return raw

    def failure(self) -> TransportError | None:
        """Inspect failure independently of handoff capacity or application waits.

        :returns: The first terminal native failure, or None when none is known.
        """
        with self._condition:
            return self._error

    def close(self, *, deadline: float) -> None:
        """Request stop and join Consumer cleanup on its owning native worker.

        :param deadline: Absolute cleanup deadline in the supplied clock domain.
        :returns: None after worker termination; repeated successful closure is harmless.
        :raises ObserverCleanupTimeoutError: If the worker outlives the cleanup budget.
        :raises ObserverCleanupError: If native Consumer cleanup failed.
        """
        self._stop.set()
        self._wake_owner()
        if self._thread.ident is not None:
            self._thread.join(timeout=max(0.0, deadline - self._context.clock.monotonic()))
        if self._thread.is_alive():
            raise ObserverCleanupTimeoutError("Kafka worker did not stop within the cleanup budget")
        if self._cleanup_error is not None:
            raise self._cleanup_error


__all__ = ["SyncKafkaAdapter", "SyncKafkaSession"]
