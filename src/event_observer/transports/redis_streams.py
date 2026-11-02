"""Independent Redis Streams readers with native cursors and bounded polling.

Every batch atomically pairs connection/server identity, stream metadata, and
nonblocking XREAD. The runtime never writes stream data, joins groups, or
acknowledges deliveries. Detected connection or continuity failures are terminal.

:ivar _MANAGED_OPTIONS: URL options reserved for binary decoding, zero retries,
    bounded sockets, callbacks, and ownership of the native connection.
:ivar _STREAM_ID: Native full-ID parser used before numeric pair comparison.
:ivar _UINT64_MAX: Largest allowed component of a native Redis stream ID.
:ivar __all__: Public Redis Streams configuration, adapter, and session types.
"""

from __future__ import annotations

import asyncio
import importlib
import math
import re
from collections import deque
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from ..errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    StartPositionUnavailableError,
    TransportAuthenticationError,
    TransportAuthorizationError,
    TransportBackpressureError,
    TransportConnectionError,
    TransportError,
    TransportInterruptedError,
    TransportReadinessError,
    TransportSubscriptionError,
    UnsupportedCapabilityError,
)
from ..journal import raw_size
from ..models import (
    ObservationBoundary,
    OpenContext,
    RawEvent,
    StartPosition,
    TransportCapabilities,
    TransportReadiness,
    TransportSource,
)


_MANAGED_OPTIONS = frozenset(
    {
        "decode_responses",
        "protocol",
        "encoding",
        "encoding_errors",
        "retry",
        "retry_on_error",
        "retry_on_timeout",
        "socket_timeout",
        "socket_connect_timeout",
        "max_connections",
        "single_connection_client",
        "connection_pool",
        "connection_class",
        "parser_class",
        "auto_close_connection_pool",
        "redis_connect_func",
        "credential_provider",
        "health_check_interval",
        "event_dispatcher",
        "maint_notifications_config",
    }
)
_STREAM_ID = re.compile(r"([0-9]+)-([0-9]+)\Z")
_UINT64_MAX = (1 << 64) - 1


@dataclass(frozen=True, slots=True)
class RedisStreamsConfig:
    """Immutable single-stream field mapping and bounded polling configuration.

    :ivar url: Explicit Redis/rediss/unix URL, excluded from repr and restricted from overriding managed
        options.
    :ivar payload_field: Nonempty UTF-8 native field containing body bytes.
    :ivar header_prefix: Nonempty UTF-8 prefix stripped from normalized header names.
    :ivar read_count: Positive maximum requested native batch size, clipped to handoff capacity.
    :ivar poll_interval: Positive finite delay between nonblocking polls, in seconds.
    :ivar socket_timeout: Positive finite native operation timeout, in seconds.
    """

    url: str = field(repr=False)
    payload_field: str = "payload"
    header_prefix: str = "header:"
    read_count: int = 100
    poll_interval: float = 0.05
    socket_timeout: float = 1.0

    def __post_init__(self) -> None:
        """Validate URL ownership constraints, field mapping, and finite polling budgets.

        :returns: None after validating the immutable configuration.
        :raises ConfigurationError: If the URL, managed query options, field names, or budgets are invalid.
        """
        if not isinstance(self.url, str) or not self.url or self.url != self.url.strip():
            raise ConfigurationError("Redis url must be a nonempty connection URL")
        try:
            parsed = urlsplit(self.url)
            valid = (
                parsed.scheme in {"redis", "rediss"}
                and bool(parsed.hostname)
                and (parsed.port is None or 1 <= parsed.port <= 65535)
            ) or (parsed.scheme == "unix" and bool(parsed.path))
            options = {name.lower() for name, _ in parse_qsl(parsed.query, keep_blank_values=True)}
        except ValueError:
            valid = False
            options = set()
        if not valid:
            raise ConfigurationError("Redis url must be a valid Redis connection URL")
        if options.intersection(_MANAGED_OPTIONS):
            raise ConfigurationError("Redis URL cannot override observer-managed connection options")
        for name in ("payload_field", "header_prefix"):
            value = getattr(self, name)
            try:
                valid_text = isinstance(value, str) and bool(value) and bool(value.encode("utf-8"))
            except UnicodeError:
                valid_text = False
            if not valid_text:
                raise ConfigurationError(f"{name} must be a nonempty UTF-8 string")
        if isinstance(self.read_count, bool) or not isinstance(self.read_count, int) or self.read_count <= 0:
            raise ConfigurationError("read_count must be a positive integer")
        for name in ("poll_interval", "socket_timeout"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (float, int))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ConfigurationError(f"{name} must be positive and finite")


def _capabilities() -> TransportCapabilities:
    """Describe independent stream cursors with explicit data-domain limitations.

    :returns: Latest, earliest, and exclusive native-ID capabilities without groups or settlement.
    """
    return TransportCapabilities(
        start_modes=frozenset({"latest", "earliest", "stream_id"}),
        isolation_modes=frozenset({"independent_reader"}),
        flow_control_modes=frozenset({"fetch_limit"}),
        ordering_scope="stream",
        supports_headers=True,
        supports_key=False,
        supports_null_payload=False,
        supports_duplicate_headers=False,
        supports_null_header_values=False,
    )


def _load_native() -> Any:
    """Import the optional asyncio Redis client only when opening a session.

    :returns: The redis.asyncio module.
    :raises ImportError: If the optional redis-py dependency is unavailable.
    """
    return importlib.import_module("redis.asyncio")


def _no_retry() -> Any:
    """Create a native retry policy that does not repeat a known failed read.

    :returns: A zero-attempt Retry object with NoBackoff.
    :raises ImportError: If the required optional retry API cannot be imported.
    """
    retry = importlib.import_module("redis.asyncio.retry")
    backoff = importlib.import_module("redis.backoff")
    return retry.Retry(backoff.NoBackoff(), 0)


def _raw_xread(response: Any, **_options: Any) -> Any:
    # redis-py's default XREAD parser turns each flat field sequence into a dict,
    # losing duplicate names before this adapter can validate them.
    """Preserve flat native entry fields so duplicate selected names remain detectable.

    :param response: Unconverted XREAD data from redis-py.
    :param _options: Unused response-callback parsing options.
    :returns: The unchanged raw XREAD response.
    """
    return response


def _id(value: Any) -> tuple[str, tuple[int, int]]:
    """Validate a full native ID and retain both unsigned 64-bit numeric components.

    :param value: Native value to validate before exposing it as a stream position.
    :returns: Canonical text ID together with its numeric comparison pair.
    :raises TransportSubscriptionError: If the native ID is malformed, non-ASCII, or out of range.
    """
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeError as error:
            raise TransportSubscriptionError("Redis returned an invalid stream ID") from error
    if not isinstance(value, str) or len(value) > 41:
        raise TransportSubscriptionError("Redis returned an invalid stream ID")
    match = _STREAM_ID.fullmatch(value)
    if match is None:
        raise TransportSubscriptionError("Redis returned an invalid stream ID")
    pair = (int(match[1]), int(match[2]))
    if any(part > _UINT64_MAX for part in pair):
        raise TransportSubscriptionError("Redis returned an invalid stream ID")
    return f"{pair[0]}-{pair[1]}", pair


@dataclass(frozen=True, slots=True)
class _StreamState:
    """Validated atomic source metadata used to detect loss and identity rewind.

    :ivar last_id: Canonical last-generated native ID, including an empty or deleted tail.
    :ivar last_pair: Numeric pair for last-generated ID comparison.
    :ivar added: Cumulative entries-added counter reported by the stream.
    :ivar deleted: Conservative removed-entry count derived from entries-added minus retained length.
    :ivar max_deleted: Largest deleted native ID observed by Redis.
    :ivar first_pair: First retained entry ID, or None when the typed stream is empty.
    """

    last_id: str
    last_pair: tuple[int, int]
    added: int
    deleted: int
    max_deleted: tuple[int, int]
    first_pair: tuple[int, int] | None


def _state(metadata: Any) -> _StreamState:
    """Decode required Redis 7+ counters and retained-boundary metadata.

    :param metadata: Native stream metadata from the same atomic observation transaction.
    :returns: A validated immutable stream-state value.
    :raises TransportSubscriptionError: If required counters, IDs, or first-entry metadata are absent or
        inconsistent.
    """
    if not isinstance(metadata, Mapping):
        raise TransportSubscriptionError("Redis stream metadata is unavailable")

    def get(name: str) -> Any:
        """Resolve one native metadata field in either text-key or byte-key form.

        :param name: Metadata field name, accepted in either text or ASCII byte form.
        :returns: The metadata value, or None if neither key representation is present.
        """
        return metadata.get(name, metadata.get(name.encode("ascii")))

    length, added = get("length"), get("entries-added")
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (length, added)):
        raise TransportSubscriptionError("Redis stream lacks required continuity counters")
    if added < length:
        raise TransportSubscriptionError("Redis stream continuity counters are inconsistent")
    last_id, last_pair = _id(get("last-generated-id"))
    _, max_deleted = _id(get("max-deleted-entry-id"))
    first_entry = get("first-entry")
    first_pair = None
    if length:
        if not isinstance(first_entry, (tuple, list)) or len(first_entry) != 2:
            raise TransportSubscriptionError("Redis stream first-entry metadata is unavailable")
        first_pair = _id(first_entry[0])[1]
    return _StreamState(last_id, last_pair, added, added - length, max_deleted, first_pair)


def _identity(client_id: Any, server: Any) -> tuple[int, str]:
    """Validate the connection ID and server process run_id from one transaction.

    :param client_id: Native connection identifier returned by CLIENT ID.
    :param server: INFO server fields containing the server process run_id.
    :returns: A native connection ID and canonical server run_id pair.
    :raises TransportReadinessError: If either identity value is unavailable or malformed.
    """
    if isinstance(client_id, bool) or not isinstance(client_id, int) or client_id < 1:
        raise TransportReadinessError("Redis connection identity is unavailable")
    run_id = server.get("run_id", server.get(b"run_id")) if isinstance(server, Mapping) else None
    if isinstance(run_id, bytes):
        try:
            run_id = run_id.decode("ascii")
        except UnicodeError:
            run_id = None
    if not isinstance(run_id, str) or re.fullmatch(r"[0-9a-f]{40}", run_id) is None:
        raise TransportReadinessError("Redis server identity is unavailable")
    return client_id, run_id


def _native_error(error: Exception, *, startup: bool) -> TransportError:
    """Classify native failures without including native messages in safe summaries.

    :param error: Failure to normalize or publish without copying its message into safe diagnostics.
    :param startup: Whether the failure occurred before READY rather than while polling.
    :returns: A typed authentication, authorization, subscription, connection, or interruption error.
    """
    if isinstance(error, TransportError):
        return error
    names = {cls.__name__ for cls in type(error).__mro__}
    if "AuthenticationError" in names:
        return TransportAuthenticationError("Redis authentication failed", cause=error)
    if names.intersection({"AuthorizationError", "NoPermissionError"}):
        return TransportAuthorizationError("Redis permission check failed", cause=error)
    if startup:
        if "ResponseError" in names:
            return TransportSubscriptionError("Redis source must be an existing readable stream", cause=error)
        return TransportConnectionError("Redis connection operation failed", cause=error)
    return TransportInterruptedError("Redis stream observation was interrupted", cause=error)


class RedisStreamsAdapter:
    """Lazy factory of independent cursors over existing typed stream keys.

    :ivar config: Validated immutable URL, field mapping, and polling policy.
    :ivar capabilities: Independent latest/earliest/native-ID reading without groups, keys, or null values.
    """

    def __init__(self, config: RedisStreamsConfig) -> None:
        """Store configuration without loading a native client or connecting.

        :param config: Validated immutable connection and delivery settings.
        :returns: None; clients are created inside the runtime loop during open.
        :raises ConfigurationError: If config is not a RedisStreamsConfig.
        """
        if not isinstance(config, RedisStreamsConfig):
            raise ConfigurationError("RedisStreamsAdapter requires RedisStreamsConfig")
        self.config = config

    @property
    def capabilities(self) -> TransportCapabilities:
        """Describe native stream starts and supported normalized values.

        :returns: Fresh immutable static capabilities for this adapter.
        """
        return _capabilities()

    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> RedisStreamsSession:
        """Verify the existing stream and return an independently READY cursor.

        :param source: Concrete existing source selected for this observation.
        :param start: Requested start policy and its transport-specific boundary.
        :param context: Runtime clock, startup deadline, observation identity, and buffer limits.
        :returns: A session owning its native client, poll task, and bounded handoff.
        :raises UnsupportedCapabilityError: If partitions or unsupported start policies are requested.
        :raises MissingOptionalDependencyError: If the Redis client or required retry API is unavailable.
        :raises TransportError: If source, identity, permissions, cursor availability, or readiness fails.
        :raises TimeoutError: If startup exceeds its deadline.
        """
        if source.partitions is not None or start.mode.value not in self.capabilities.start_modes:
            raise UnsupportedCapabilityError(
                "Redis Streams supports latest, earliest, and stream-ID cursors without partitions"
            )
        try:
            native, retry = _load_native(), _no_retry()
        except ImportError as error:
            raise MissingOptionalDependencyError(
                "Redis Streams requires the 'redis' extra: pip install 'pytest-broker-observer[redis]'"
            ) from error
        session = RedisStreamsSession(self.config, source, start, context)
        try:
            async with asyncio.timeout(session._remaining(context.startup_deadline)):
                await session._open(native, retry)
        except BaseException as primary:
            try:
                await session.close(deadline=context.clock.monotonic() + 1.0)
            except BaseException as cleanup:
                setattr(primary, "cleanup_error", cleanup)
            raise
        return session


class RedisStreamsSession:
    """One read-only polling client with atomic continuity checks and a bounded bridge.

    :ivar _config: Immutable URL, field mapping, and polling policy.
    :ivar _source: Existing stream key owned by the application or test harness.
    :ivar _start: Requested latest, earliest, or exclusive native-ID start.
    :ivar _context: Shared clock, deadlines, identity, diagnostics, and transfer limits.
    :ivar _client: Owned optional native client and its one-connection pool.
    :ivar _read_count: Requested batch count clipped to transport event capacity.
    :ivar _payload_field: UTF-8 encoded native payload field name.
    :ivar _header_prefix: UTF-8 encoded native header prefix.
    :ivar _cursor: Current exclusive native ID used by subsequent XREAD commands.
    :ivar _identity: Initial connection ID and server run_id; replacement is terminal.
    :ivar _previous: Previous validated atomic stream state used for continuity checks.
    :ivar _readiness: Resolved startup cursor and capability contract.
    :ivar _buffer: Deque of detached event/accounted-size pairs awaiting Core.
    :ivar _buffer_bytes: Accounted bytes currently retained in the handoff.
    :ivar _wake: Owner-loop event for new data, closure, or terminal failure.
    :ivar _failure: Shared terminal-error or intentional-close Future.
    :ivar _error: First terminal transport error retained for all waiters.
    :ivar _poll_task: Owned periodic nonblocking read/metadata task.
    :ivar _close_task: Single cleanup task joined by concurrent close callers.
    :ivar _closing: Whether closure has begun and the iterator must stop.
    :ivar _closed: Whether owned native cleanup completed successfully.
    :ivar _receive_started: Whether the single allowed receive iterator has been reserved.
    :ivar readiness: Resolved boundary and source metadata after successful startup.
    """

    def __init__(
        self, config: RedisStreamsConfig, source: TransportSource, start: StartPosition, context: OpenContext
    ) -> None:
        """Allocate owner-loop cursor, handoff, and failure state without connecting.

        :param config: Validated immutable connection and delivery settings.
        :param source: Concrete existing source selected for this observation.
        :param start: Requested start policy and its transport-specific boundary.
        :param context: Runtime clock, startup deadline, observation identity, and buffer limits.
        :returns: None; native resources are created by the later startup operation.
        """
        self._config, self._source, self._start, self._context = config, source, start, context
        self._client: Any = None
        self._read_count = min(config.read_count, context.buffers.max_events)
        self._payload_field = config.payload_field.encode("utf-8")
        self._header_prefix = config.header_prefix.encode("utf-8")
        self._cursor = "0-0"
        self._identity: tuple[int, str] | None = None
        self._previous: _StreamState | None = None
        self._readiness: TransportReadiness | None = None
        self._buffer: deque[tuple[RawEvent, int]] = deque()
        self._buffer_bytes = 0
        self._wake = asyncio.Event()
        self._failure: asyncio.Future[TransportError | None] = asyncio.get_running_loop().create_future()
        self._error: TransportError | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._closing = False
        self._closed = False
        self._receive_started = False

    def _remaining(self, deadline: float) -> float:
        """Calculate nonnegative remaining time in the owner runtime clock domain.

        :param deadline: Absolute cleanup deadline in the runtime monotonic clock domain.
        :returns: Seconds remaining until deadline, clamped to zero.
        """
        return max(0.0, deadline - self._context.clock.monotonic())

    async def _open(self, native: Any, retry: Any) -> None:
        """Resolve the initial cursor and verify an atomic read before publishing READY.

        :param native: Lazily loaded native client module or an equivalent controlled test double.
        :param retry: Native retry policy configured for zero retries.
        :returns: None once the native client, cursor, and poll task are ready.
        :raises StartPositionUnavailableError: If explicit history is below known baseline availability.
        :raises TransportError: If source, identity, permissions, or initial read validation fails.
        :raises TimeoutError: If startup runs out of time.
        """
        try:
            self._client = native.Redis.from_url(
                self._config.url,
                decode_responses=False,
                protocol=2,
                encoding="utf-8",
                encoding_errors="strict",
                retry=retry,
                retry_on_error=[],
                retry_on_timeout=False,
                socket_timeout=self._config.socket_timeout,
                socket_connect_timeout=min(
                    self._config.socket_timeout, self._remaining(self._context.startup_deadline)
                ),
                single_connection_client=True,
                max_connections=1,
                health_check_interval=0,
            )
            self._client.set_response_callback("XREAD", _raw_xread)
            # Direct commands would reserve the single client connection and
            # starve a pipeline using the same one-connection pool.
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.client_id()
                pipe.info("server")
                pipe.xinfo_stream(self._source.name)
                result = await pipe.execute()
            if not isinstance(result, (tuple, list)) or len(result) != 3:
                raise TransportReadinessError("Redis stream readiness response was invalid")
            self._identity = _identity(result[0], result[1])
            self._previous = _state(result[2])
            captured = self._context.clock.monotonic()
            if self._start.mode.value == "latest":
                self._cursor = self._previous.last_id
            elif self._start.mode.value == "stream_id":
                self._cursor, pair = _id(self._start.stream_id)
                first = self._previous.first_pair
                if pair < self._previous.max_deleted or (
                    self._previous.deleted
                    and pair < (first if first is not None else self._previous.last_pair)
                ):
                    raise StartPositionUnavailableError(
                        "Redis stream history after the requested cursor may be unavailable"
                    )
            start_id = self._cursor
            await self._read_once()
            self._readiness = TransportReadiness(
                ObservationBoundary(self._source.name, self._start.mode.value, {}, captured),
                _capabilities(),
                {"redis.start_id": start_id, "redis.client_id": self._identity[0]},
                self._context.clock.monotonic(),
            )
            self._poll_task = asyncio.create_task(self._poll(), name="event-observer-redis-streams-poll")
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise
        except Exception as error:
            normalized = _native_error(error, startup=True)
            if normalized is error:
                raise
            raise normalized from error

    @property
    def readiness(self) -> TransportReadiness:
        """Return confirmed native start metadata and reader capabilities.

        :returns: The resolved contract including redis.start_id and initial connection ID.
        :raises TransportReadinessError: If startup has not completed.
        """
        if self._readiness is None:
            raise TransportReadinessError("Redis Streams session is not ready")
        return self._readiness

    def _check_continuity(self, state: _StreamState) -> None:
        """Reject new deletion evidence or a rewind relative to the previous atomic state.

        :param state: Validated stream metadata describing the same batch of entries.
        :returns: None if the source remains conservatively continuous.
        :raises TransportInterruptedError: If counters, first/last IDs, or deletion watermarks reveal a
            source change.
        """
        previous = self._previous
        if previous is None:
            return
        if (
            state.added < previous.added
            or state.last_pair < previous.last_pair
            or state.max_deleted < previous.max_deleted
            or state.deleted < previous.deleted
            or (
                state.first_pair is not None
                and previous.first_pair is not None
                and state.first_pair < previous.first_pair
            )
        ):
            raise TransportInterruptedError("Redis stream identity or metadata rewound")
        if state.deleted > previous.deleted or state.max_deleted > previous.max_deleted:
            raise TransportInterruptedError("Redis stream history was deleted or trimmed")

    def _entries(self, response: Any, state: _StreamState) -> list[tuple[RawEvent, int]]:
        """Validate batch source, order, field domain, and complete envelope accounting.

        :param response: Native command response before lossy field-table conversion.
        :param state: Validated stream metadata describing the same batch of entries.
        :returns: Detached event/accounted-size pairs in strictly increasing native-ID order.
        :raises TransportSubscriptionError: If the batch shape or selected payload/header fields are
            malformed.
        :raises TransportInterruptedError: If native IDs violate cursor or batch metadata order.
        :raises TransportBackpressureError: If the native batch exceeds the configured read count.
        """
        if response is None or response == []:
            return []
        if not isinstance(response, (list, tuple)) or len(response) != 1:
            raise TransportSubscriptionError("Redis returned an invalid stream read response")
        stream = response[0]
        if not isinstance(stream, (tuple, list)) or len(stream) != 2:
            raise TransportSubscriptionError("Redis returned an invalid stream read response")
        source, entries = stream
        if source != self._source.name.encode("utf-8") or not isinstance(entries, (tuple, list)):
            raise TransportSubscriptionError("Redis returned an unexpected stream source")
        if len(entries) > self._read_count:
            raise TransportBackpressureError("Redis stream read exceeded its configured count")
        batch = []
        previous_id = _id(self._cursor)[1]
        for entry in entries:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                raise TransportSubscriptionError("Redis returned an invalid stream entry")
            entry_id, fields = entry
            text_id, pair = _id(entry_id)
            if pair <= previous_id or pair > state.last_pair:
                raise TransportInterruptedError("Redis stream delivery order is inconsistent")
            previous_id = pair
            if isinstance(fields, Mapping):
                pairs = fields.items()
            elif isinstance(fields, (list, tuple)) and len(fields) % 2 == 0:
                pairs = zip(fields[::2], fields[1::2])
            else:
                raise TransportSubscriptionError("Redis stream fields must be binary name/value pairs")
            payload = None
            headers = []
            seen = set()
            for name, value in pairs:
                if not isinstance(name, bytes):
                    raise TransportSubscriptionError("Redis stream field names must be bytes")
                if name == self._payload_field or name.startswith(self._header_prefix):
                    if name in seen:
                        raise TransportSubscriptionError(
                            "Redis stream contains duplicate payload or header fields"
                        )
                    seen.add(name)
                    if not isinstance(value, bytes):
                        raise TransportSubscriptionError("Redis stream payload and headers must be bytes")
                    if name == self._payload_field:
                        payload = value
                    else:
                        try:
                            header = name[len(self._header_prefix) :].decode("utf-8")
                        except UnicodeError as error:
                            raise TransportSubscriptionError(
                                "Redis header names must be valid UTF-8"
                            ) from error
                        headers.append((header, value))
            if payload is None:
                raise TransportSubscriptionError("Redis stream entry lacks the configured payload field")
            raw = RawEvent(
                payload, self._source.name, headers=tuple(headers), metadata={"redis.stream_id": text_id}
            )
            batch.append((raw, raw_size(raw)))
        return batch

    async def _read_once(self) -> None:
        """Atomically validate identity, stream continuity, and entries before handoff.

        :returns: None after advancing the cursor and queuing a complete bounded batch.
        :raises TransportInterruptedError: If source or connection/server identity changed.
        :raises TransportSubscriptionError: If the transaction response or entries are malformed.
        :raises TransportBackpressureError: If the complete batch cannot fit the bounded handoff.
        """
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.client_id()
            pipe.info("server")
            pipe.xinfo_stream(self._source.name)
            pipe.xread({self._source.name: self._cursor}, count=self._read_count)
            response = await pipe.execute()
        if not isinstance(response, (list, tuple)) or len(response) != 4:
            raise TransportSubscriptionError("Redis returned an invalid observation transaction")
        if _identity(response[0], response[1]) != self._identity:
            raise TransportInterruptedError("Redis connection or server identity changed")
        state = _state(response[2])
        self._check_continuity(state)
        batch = self._entries(response[3], state)
        total = sum(size for _, size in batch)
        if (
            len(self._buffer) + len(batch) > self._context.buffers.max_events
            or self._buffer_bytes + total > self._context.buffers.max_bytes
        ):
            raise TransportBackpressureError("Redis Streams handoff capacity exhausted")
        self._previous = state
        if batch:
            self._cursor = str(batch[-1][0].metadata["redis.stream_id"])
            self._buffer.extend(batch)
            self._buffer_bytes += total
            self._wake.set()

    def _fail(self, error: TransportError) -> None:
        """Publish the first terminal error through a path independent of receive capacity.

        :param error: Failure to normalize or publish without copying its message into safe diagnostics.
        :returns: None; intentional closure and later errors do not replace the first failure.
        """
        if self._closing or self._error is not None:
            return
        self._error = error
        if not self._failure.done():
            self._failure.set_result(error)
        self._wake.set()

    async def _poll(self) -> None:
        """Run bounded nonblocking reads periodically until cleanup or known failure.

        :returns: None after cleanup or publishing a terminal error.
        :raises asyncio.CancelledError: If the owned polling task is cancelled.
        """
        try:
            while not self._closing:
                await asyncio.sleep(self._config.poll_interval)
                await self._read_once()
        except asyncio.CancelledError:
            self._fail(TransportInterruptedError("Redis stream polling was cancelled"))
            raise
        except Exception as error:
            self._fail(_native_error(error, startup=False))

    def receive(self) -> AsyncIterator[RawEvent]:
        """Reserve this independent cursor's single asynchronous receive iterator.

        :returns: An asynchronous iterator of detached RawEvent values.
        :raises TransportSubscriptionError: If receive was already requested.
        """
        if self._receive_started:
            raise TransportSubscriptionError("A Redis Streams session permits one receive iterator")
        self._receive_started = True
        return self._receive()

    async def _receive(self) -> AsyncIterator[RawEvent]:
        """Drain the bounded handoff while prioritizing known failure and closure.

        :yields: Detached RawEvent values with native redis.stream_id metadata.
        :raises TransportError: If the poller has detected a terminal error.
        """
        while True:
            if self._error is not None:
                raise self._error
            if self._closing:
                return
            if self._buffer:
                raw, size = self._buffer.popleft()
                self._buffer_bytes -= size
                yield raw
            else:
                self._wake.clear()
                await self._wake.wait()

    async def wait_failure(self) -> TransportError | None:
        """Wait for terminal failure independently of available business entries.

        :returns: The first terminal transport error, or None after intentional cleanup.
        :raises asyncio.CancelledError: If only this monitoring waiter is cancelled.
        """
        return await asyncio.shield(self._failure)

    async def close(self, *, deadline: float) -> None:
        """Join one shielded cleanup operation for the client and poller.

        :param deadline: Absolute cleanup deadline in the runtime monotonic clock domain.
        :returns: None after owned tasks, client, and pool are released.
        :raises ObserverCleanupError: If native cleanup fails.
        :raises ObserverCleanupTimeoutError: If cleanup exceeds deadline.
        """
        if self._close_task is None:
            self._closing = True
            self._wake.set()
            self._close_task = asyncio.create_task(
                self._cleanup(deadline), name="event-observer-redis-streams-cleanup"
            )
        await asyncio.shield(self._close_task)

    async def _cleanup(self, deadline: float) -> None:
        """Cancel polling, close the owned pool, and clear the handoff within budget.

        :param deadline: Absolute cleanup deadline in the runtime monotonic clock domain.
        :returns: None after cleanup leaves the application stream and group state untouched.
        :raises ObserverCleanupError: If client/pool cleanup fails.
        :raises ObserverCleanupTimeoutError: If the lifecycle cleanup budget expires.
        """
        try:
            async with asyncio.timeout(self._remaining(deadline)):
                if self._poll_task is not None:
                    self._poll_task.cancel()
                    await asyncio.gather(self._poll_task, return_exceptions=True)
                if self._client is not None:
                    await self._client.aclose(close_connection_pool=True)
            self._closed = True
        except TimeoutError as error:
            raise ObserverCleanupTimeoutError("Redis Streams cleanup deadline expired") from error
        except Exception as error:
            raise ObserverCleanupError("Redis Streams cleanup failed") from error
        finally:
            self._buffer.clear()
            self._buffer_bytes = 0
            if not self._failure.done():
                self._failure.set_result(
                    None
                    if self._closed
                    else TransportInterruptedError("Redis Streams cleanup did not finish")
                )


__all__ = ["RedisStreamsAdapter", "RedisStreamsConfig", "RedisStreamsSession"]
