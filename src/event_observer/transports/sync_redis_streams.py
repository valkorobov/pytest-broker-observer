"""Native blocking Redis Streams readers with atomic continuity snapshots.

Each session owns one synchronous redis-py client on its I/O worker. Pure
cursor, identity, continuity, and envelope algorithms are shared with the async
driver. Blocking observation never creates a coroutine scheduler.

:ivar __all__: Public blocking Redis Streams adapter and session types.
"""

from __future__ import annotations

import importlib
import threading
from typing import Any
from uuid import uuid4

from ..errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    TransportError,
    UnsupportedCapabilityError,
)
from ..models import (
    OpenContext,
    RawEvent,
    StartPosition,
    TransportCapabilities,
    TransportSource,
)
from .redis_streams import (
    RedisStreamsConfig,
    _RedisStreamsReader,
    _capabilities,
    _client_options,
    _native_error,
    _raw_xread,
)
from .sync_base import validate_poll_timeout


def _load_native() -> Any:
    """Lazily import the synchronous redis-py client without connecting.

    :returns: The redis module providing blocking Redis and Pipeline clients.
    :raises ImportError: If the optional Redis dependency is unavailable.
    """
    return importlib.import_module("redis")


def _no_retry() -> Any:
    """Create the blocking SDK retry policy with no retries of known failures.

    :returns: A synchronous Retry object with NoBackoff and zero retries.
    :raises ImportError: If the optional retry API cannot be imported.
    """
    retry = importlib.import_module("redis.retry")
    backoff = importlib.import_module("redis.backoff")
    return retry.Retry(backoff.NoBackoff(), 0)


class SyncRedisStreamsAdapter:
    """Lazy factory of independent blocking cursors over existing stream keys.

    :ivar transport_name: Canonical report title shared with the asynchronous adapter.
    :ivar config: Validated immutable Redis URL, field mapping, and polling policy.
    :ivar capabilities: Independent latest, earliest, and exclusive native-ID semantics.
    """

    transport_name = "RedisStreamsAdapter"

    def __init__(self, config: RedisStreamsConfig) -> None:
        """Store configuration without importing or constructing a native client.

        :param config: Immutable validated connection and field mapping policy.
        :returns: None; native resources are created on each session's I/O worker.
        :raises ConfigurationError: If config is not RedisStreamsConfig.
        """
        if not isinstance(config, RedisStreamsConfig):
            raise ConfigurationError("SyncRedisStreamsAdapter requires RedisStreamsConfig")
        self.config = config

    @property
    def capabilities(self) -> TransportCapabilities:
        """Return the same native stream capabilities as the async driver.

        :returns: Supported independent starts, ordering, and envelope domains.
        """
        return _capabilities()

    def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> SyncRedisStreamsSession:
        """Start an independent I/O worker and confirm its initial atomic read.

        :param source: Existing typed stream key with no partition selection.
        :param start: Latest, earliest, or exclusive native-ID start policy.
        :param context: Runtime clock, startup deadline, identity, and handoff budgets.
        :returns: A READY blocking session owning one native connection pool.
        :raises UnsupportedCapabilityError: If partitions or an unsupported start are requested.
        :raises MissingOptionalDependencyError: If redis-py or its retry API is unavailable.
        :raises TransportError: If connection, metadata, identity, or readiness fails.
        :raises TimeoutError: If startup exceeds the supplied deadline.
        """
        if source.partitions is not None or start.mode.value not in self.capabilities.start_modes:
            raise UnsupportedCapabilityError(
                "Redis Streams supports latest, earliest, and stream-ID cursors without partitions"
            )
        if context.startup_deadline <= context.clock.monotonic():
            raise TimeoutError("Redis Streams startup deadline expired")
        try:
            native, retry = _load_native(), _no_retry()
        except ImportError as error:
            raise MissingOptionalDependencyError(
                "Redis Streams requires the 'redis' extra: pip install 'pytest-broker-observer[redis]'"
            ) from error
        session = SyncRedisStreamsSession(self.config, source, start, context, native, retry)
        session._thread.start()
        try:
            with session._condition:
                completed = session._condition.wait_for(
                    lambda: (
                        session._readiness is not None
                        or session._error is not None
                        or session._worker_finished
                    ),
                    timeout=max(0.0, context.startup_deadline - context.clock.monotonic()),
                )
                if session._error is not None:
                    raise session._error
                if (
                    not completed
                    or session._readiness is None
                    or context.clock.monotonic() >= context.startup_deadline
                ):
                    raise TimeoutError("Redis Streams startup deadline expired")
        except BaseException as primary:
            try:
                session.close(deadline=context.clock.monotonic() + 1.0)
            except BaseException as cleanup:
                setattr(primary, "cleanup_error", cleanup)
            raise
        return session


class SyncRedisStreamsSession(_RedisStreamsReader):
    """Thread-owned native Redis client with a bounded condition-based handoff.

    :ivar _native: Lazily imported synchronous Redis module or controlled native double.
    :ivar _retry: Synchronous native retry object configured for zero retries.
    :ivar _condition: Lock protecting handoff, readiness, and terminal state.
    :ivar _stop: Thread-safe intentional-stop signal independent of delivery capacity.
    :ivar _error: First terminal normalized native error, never replaced.
    :ivar _cleanup_error: Native cleanup error retaining its original cause and chain.
    :ivar _worker_finished: Whether native operations and final cleanup have finished.
    :ivar _thread: Dedicated I/O worker owning every native client operation.
    :ivar _client: Owned blocking client whose connection pool is closed by the worker.
    :ivar _buffer_bytes: Shared accounted handoff bytes, reset after native cleanup.
    """

    def __init__(
        self,
        config: RedisStreamsConfig,
        source: TransportSource,
        start: StartPosition,
        context: OpenContext,
        native: Any,
        retry: Any,
    ) -> None:
        """Allocate shared reader state and an unstarted native I/O worker.

        :param config: Immutable connection, polling, and field mapping policy.
        :param source: Existing stream selected for independent observation.
        :param start: Explicit latest, earliest, or exclusive native-ID replay policy.
        :param context: Runtime clock, deadlines, identity, and handoff limits.
        :param native: Synchronous Redis module or equivalent controlled double.
        :param retry: Synchronous native retry policy with zero retries.
        :returns: None; the adapter starts the worker after initialization.
        """
        super().__init__(config, source, start, context)
        self._native = native
        self._retry = retry
        self._condition = threading.Condition()
        self._stop = threading.Event()
        self._error: TransportError | None = None
        self._cleanup_error: ObserverCleanupError | None = None
        self._worker_finished = False
        self._thread = threading.Thread(
            target=self._run, name=f"event-observer-redis-streams-{uuid4().hex[:8]}", daemon=True
        )

    def _run(self) -> None:
        """Own startup, periodic atomic reads, failure publication, and native cleanup.

        :returns: None after closing the owned client and notifying lifecycle waiters.
        """
        try:
            options = _client_options(
                self._config, self._context, self._retry, single_connection_client=False
            )
            # Synchronous Redis reserves a connection eagerly when configured
            # as a single-connection client. Pipelines need that same pool slot,
            # so the I/O worker instead owns a pool with exactly one connection.
            self._client = self._native.Redis.from_url(self._config.url, **options)
            self._client.set_response_callback("XREAD", _raw_xread)
            with self._client.pipeline(transaction=True) as pipe:
                pipe.client_id()
                pipe.info("server")
                pipe.xinfo_stream(self._source.name)
                initial = pipe.execute()
            captured, start_id = self._prepare_start(initial)
            self._read_once()
            with self._condition:
                if not self._stop.is_set():
                    self._mark_ready(captured, start_id)
                self._condition.notify_all()
            while not self._stop.wait(min(self._config.poll_interval, threading.TIMEOUT_MAX)):
                self._read_once()
        except Exception as cause:
            error = _native_error(cause, startup=self._readiness is None)
            if error is not cause:
                error.__cause__ = cause
            self._fail(error)
        except BaseException as cause:
            error = _native_error(RuntimeError("Redis I/O worker was interrupted"), startup=False)
            error.cause = cause
            error.__cause__ = cause
            self._fail(error)
        finally:
            if self._client is not None:
                try:
                    self._client.close()
                except BaseException as cause:
                    error = ObserverCleanupError("Redis Streams cleanup failed", cause=cause)
                    error.__cause__ = cause
                    self._cleanup_error = error
            with self._condition:
                self._buffer.clear()
                self._buffer_bytes = 0
                self._worker_finished = True
                self._condition.notify_all()

    def _read_once(self) -> None:
        """Read one atomic identity/metadata/batch snapshot on the native owner.

        :returns: None after committing a validated bounded batch or observing closure.
        :raises TransportError: If the shared validation detects any native loss or invalid data.
        :raises Exception: If the native transaction fails; the worker normalizes its cause.
        """
        with self._client.pipeline(transaction=True) as pipe:
            pipe.client_id()
            pipe.info("server")
            pipe.xinfo_stream(self._source.name)
            pipe.xread({self._source.name: self._cursor}, count=self._read_count)
            response = pipe.execute()
        with self._condition:
            if not self._stop.is_set() and self._accept_response(response):
                self._condition.notify_all()

    def _fail(self, error: TransportError) -> None:
        """Publish the first known failure independently of the event buffer.

        :param error: Typed sanitized failure retaining its private native cause.
        :returns: None; closure and later failures do not replace the first outcome.
        """
        with self._condition:
            if not self._stop.is_set() and self._error is None:
                self._error = error
            self._condition.notify_all()

    def poll(self, timeout: float) -> RawEvent | None:
        """Return one detached entry within the finite relative wait budget.

        :param timeout: Finite nonnegative maximum wait in seconds; zero never waits.
        :returns: One RawEvent, or None after an empty poll or intentional closure.
        :raises TransportError: If any terminal failure is known, even with buffered entries.
        :raises ConfigurationError: If the relative timeout is invalid.
        """
        validate_poll_timeout(timeout)
        with self._condition:
            if self._error is not None:
                raise self._error
            if not self._buffer and not self._stop.is_set() and not self._worker_finished:
                self._condition.wait(timeout=min(timeout, threading.TIMEOUT_MAX))
            if self._error is not None:
                raise self._error
            if self._stop.is_set() or not self._buffer:
                return None
            raw, size = self._buffer.popleft()
            self._buffer_bytes -= size
            return raw

    def failure(self) -> TransportError | None:
        """Inspect failure without native I/O or waiting for buffer capacity.

        :returns: The first terminal transport error, or None when none is known.
        """
        with self._condition:
            return self._error

    def close(self, *, deadline: float) -> None:
        """Request intentional closure and join native cleanup within budget.

        :param deadline: Absolute cleanup deadline in the supplied runtime clock domain.
        :returns: None after the I/O worker and its owned client have terminated.
        :raises ObserverCleanupTimeoutError: If the native worker outlives the deadline.
        :raises ObserverCleanupError: If owned native client or pool cleanup fails.
        """
        self._stop.set()
        with self._condition:
            self._condition.notify_all()
        if self._thread.ident is not None:
            self._thread.join(timeout=self._remaining(deadline))
        if self._thread.is_alive():
            raise ObserverCleanupTimeoutError("Redis Streams cleanup deadline expired")
        if self._cleanup_error is not None:
            raise self._cleanup_error


__all__ = ["SyncRedisStreamsAdapter", "SyncRedisStreamsSession"]
