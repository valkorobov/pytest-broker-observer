"""Independent RabbitMQ exchange subscriptions with admission-time settlement.

Each watch owns an exclusive auto-delete classic queue and its native
connection. Application queues remain independent; no reconnect is attempted.
The optional aio-pika client is imported only when opening a session.

:ivar __all__: Public RabbitMQ configuration, adapter, and session types.
"""

from __future__ import annotations

import asyncio
import importlib
from collections import deque
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import uuid4

from ..errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
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
from .base import DeliveryOutcome

if TYPE_CHECKING:
    from .sync_rabbitmq import SyncRabbitMQAdapter


@dataclass(frozen=True, slots=True)
class RabbitMQConfig:
    """Immutable connection URL, exchange bindings, and bounded prefetch policy.

    :ivar url: Explicit AMQP/AMQPS URL, excluded from repr because it may contain credentials.
    :ivar binding_keys: Nonempty immutable binding-key tuple; defaults to all topic routes.
    :ivar prefetch_count: Positive AMQP prefetch from 1 through 65535, capped by handoff event capacity.
    """

    url: str = field(repr=False)
    binding_keys: tuple[str, ...] = ("#",)
    prefetch_count: int = 100

    def __post_init__(self) -> None:
        """Reject invalid AMQP URLs, binding keys, and unlimited or invalid prefetch.

        :returns: None after validating the immutable subscription settings.
        :raises ConfigurationError: If the URL, UTF-8 short-string keys, or prefetch are invalid.
        """
        if not isinstance(self.url, str) or not self.url or self.url != self.url.strip():
            raise ConfigurationError("RabbitMQ url must be a nonempty AMQP URL")
        try:
            parsed = urlsplit(self.url)
            valid = (
                parsed.scheme in {"amqp", "amqps"}
                and bool(parsed.hostname)
                and (parsed.port is None or 1 <= parsed.port <= 65535)
            )
        except ValueError:
            valid = False
        if not valid:
            raise ConfigurationError("RabbitMQ url must be a valid AMQP URL")
        if (
            not isinstance(self.binding_keys, tuple)
            or not self.binding_keys
            or any(not isinstance(key, str) for key in self.binding_keys)
        ):
            raise ConfigurationError("binding_keys must be a nonempty tuple of strings")
        try:
            keys_valid = all(len(key.encode("utf-8")) <= 255 for key in self.binding_keys)
        except UnicodeError:
            keys_valid = False
        if not keys_valid:
            raise ConfigurationError("RabbitMQ binding keys must fit an AMQP short string")
        if (
            isinstance(self.prefetch_count, bool)
            or not isinstance(self.prefetch_count, int)
            or not 1 <= self.prefetch_count <= 65535
        ):
            raise ConfigurationError("prefetch_count must be an integer from 1 to 65535")


def _capabilities() -> TransportCapabilities:
    """Describe latest-only isolated queue subscriptions and explicit settlement.

    :returns: Capabilities without replay, message keys, null bodies, or duplicate headers.
    """
    return TransportCapabilities(
        start_modes=frozenset({"latest"}),
        isolation_modes=frozenset({"dedicated_subscription"}),
        flow_control_modes=frozenset({"prefetch"}),
        ordering_scope="queue",
        supports_headers=True,
        supports_key=False,
        supports_settlement=True,
        supports_null_payload=False,
        supports_duplicate_headers=False,
    )


def _load_native() -> Any:
    """Import aio-pika only for a session that is being opened.

    :returns: The aio_pika module.
    :raises ImportError: If the optional RabbitMQ dependency is unavailable.
    """
    return importlib.import_module("aio_pika")


def _native_error(error: Exception, *, subscription: bool = False) -> TransportError:
    """Classify native failures while retaining local causes and safe summaries.

    :param error: Failure to normalize or publish without copying its message into safe diagnostics.
    :param subscription: Whether the failure occurred while preparing queue bindings or consumption.
    :returns: A normalized authentication, authorization, subscription, or connection error.
    """
    if isinstance(error, TransportError):
        return error
    names = {cls.__name__ for cls in type(error).__mro__}
    if names.intersection({"AuthenticationError", "ProbableAuthenticationError"}):
        return TransportAuthenticationError("RabbitMQ authentication failed", cause=error)
    if names.intersection(
        {"ChannelAccessRefused", "ConnectionNotAllowed", "ProbableAccessDeniedError"}
    ) or getattr(error, "reply_code", None) in {403, 530}:
        return TransportAuthorizationError("RabbitMQ permission check failed", cause=error)
    if subscription:
        return TransportSubscriptionError("RabbitMQ subscription preparation failed", cause=error)
    return TransportConnectionError("RabbitMQ connection operation failed", cause=error)


def _normalize_headers(headers: Any) -> tuple[tuple[str, bytes | None], ...]:
    """Detach the common flat AMQP header domain for both native clients.

    :param headers: Native AMQP field table, or None when no headers were supplied.
    :returns: Ordered string/byte-or-null pairs, with native text encoded as UTF-8.
    :raises TransportSubscriptionError: If a name or value is outside the supported flat domain.
    """
    if headers is None:
        return ()
    if not isinstance(headers, Mapping):
        raise TransportSubscriptionError("RabbitMQ headers must be a flat string-keyed table")
    result = []
    for name, value in headers.items():
        if not isinstance(name, str):
            raise TransportSubscriptionError("RabbitMQ header names must be strings")
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif isinstance(value, bytearray):
            value = bytes(value)
        elif value is not None and not isinstance(value, bytes):
            raise TransportSubscriptionError("RabbitMQ header values must be strings, bytes, or null")
        result.append((name, value))
    return tuple(result)


class RabbitMQAdapter:
    """Lazy factory of private subscriptions bound to an existing exchange.

    :ivar config: Validated immutable connection and binding settings.
    :ivar capabilities: Latest-only dedicated-subscription and settlement semantics.
    """

    def __init__(self, config: RabbitMQConfig) -> None:
        """Store subscription configuration without creating loop-bound native objects.

        :param config: Validated immutable connection and delivery settings.
        :returns: None; the existing exchange is verified only during open.
        :raises ConfigurationError: If config is not a RabbitMQConfig.
        """
        if not isinstance(config, RabbitMQConfig):
            raise ConfigurationError("RabbitMQAdapter requires RabbitMQConfig")
        self.config = config

    @property
    def capabilities(self) -> TransportCapabilities:
        """Return RabbitMQ's isolated subscription capabilities.

        :returns: Static semantics, including bounded prefetch and post-admission settlement.
        """
        return _capabilities()

    def sync_adapter(self) -> SyncRabbitMQAdapter:
        """Provide the equivalent worker-owned Pika driver without opening a connection.

        :returns: A lazy blocking adapter sharing this configuration and RabbitMQ semantics.
        """
        from .sync_rabbitmq import SyncRabbitMQAdapter

        return SyncRabbitMQAdapter(self.config)

    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> RabbitMQSession:
        """Create an isolated bound queue and return after consume-ok readiness.

        :param source: Concrete existing source selected for this observation.
        :param start: Requested start policy and its transport-specific boundary.
        :param context: Runtime clock, startup deadline, observation identity, and buffer limits.
        :returns: A READY RabbitMQSession owning only its subscription resources.
        :raises UnsupportedCapabilityError: If replay or partition selection was requested.
        :raises MissingOptionalDependencyError: If aio-pika is unavailable.
        :raises TransportError: If connection, exchange verification, or subscription readiness fails.
        :raises TimeoutError: If startup exceeds its shared-clock budget.
        """
        if start.mode.value != "latest" or source.partitions is not None:
            raise UnsupportedCapabilityError(
                "RabbitMQ supports latest exchange subscriptions without partitions"
            )
        try:
            native = _load_native()
        except ImportError as error:
            raise MissingOptionalDependencyError(
                "RabbitMQ requires the 'rabbitmq' extra: pip install 'pytest-broker-observer[rabbitmq]'"
            ) from error
        session = RabbitMQSession(self.config, source, context)
        try:
            async with asyncio.timeout(session._remaining(context.startup_deadline)):
                await session._open(native)
        except BaseException as primary:
            try:
                await session.close(deadline=context.clock.monotonic() + 1.0)
            except BaseException as cleanup:
                setattr(primary, "cleanup_error", cleanup)
            raise
        return session


class RabbitMQSession:
    """One runtime-owned queue, bounded pending-delivery set, and settlement path.

    :ivar _config: Immutable URL, binding keys, and prefetch policy.
    :ivar _source: Existing exchange selected for observation.
    :ivar _context: Shared clock, deadlines, identity, and transfer limits.
    :ivar _loop: Owner event loop for all native resources and signals.
    :ivar _connection: Owned native connection, with automatic reconnect disabled.
    :ivar _channel: Owned native channel for verification, QoS, and consumption.
    :ivar _underlay: Native channel view exposing the owned consumer registration.
    :ivar _queue: Server-named exclusive auto-delete observer queue.
    :ivar _consumer_tag: Unique consumer tag owned by this session.
    :ivar _consumer_started: Whether consume-ok completed and cancellation is applicable.
    :ivar _prefetch: Configured prefetch clipped to the handoff event capacity.
    :ivar _buffer: Detached events awaiting the single receive iterator.
    :ivar _pending: Delivery token to native message and accounted-size mapping until settlement.
    :ivar _settling: Tokens with an acknowledgement currently in progress.
    :ivar _pending_bytes: Accounted bytes of all pending native deliveries, including yielded events.
    :ivar _wake: Owner-loop wake event for receive and terminal state.
    :ivar _failure: Shared terminal failure or intentional-close signal.
    :ivar _error: First terminal error, preserved for all waiters.
    :ivar _readiness: Resolved contract after exchange, queue, bindings, and consume-ok.
    :ivar _monitor: Owned task detecting broker cancellation independently of business delivery.
    :ivar _close_task: Single tracked cleanup task joined by concurrent close callers.
    :ivar _closing: Whether cleanup has started and new deliveries must be ignored.
    :ivar _closed: Whether owned native resources were successfully released.
    :ivar _receive_started: Whether the single receive iterator has been reserved.
    :ivar readiness: Resolved dedicated-subscription contract after startup.
    """

    def __init__(self, config: RabbitMQConfig, source: TransportSource, context: OpenContext) -> None:
        """Allocate owner-loop delivery state without opening a native connection.

        :param config: Validated immutable connection and delivery settings.
        :param source: Concrete existing source selected for this observation.
        :param context: Runtime clock, startup deadline, observation identity, and buffer limits.
        :returns: None; the adapter subsequently prepares the subscription.
        """
        self._config = config
        self._source = source
        self._context = context
        self._loop = asyncio.get_running_loop()
        self._connection: Any = None
        self._channel: Any = None
        self._underlay: Any = None
        self._queue: Any = None
        self._consumer_tag = "observer-" + uuid4().hex
        self._consumer_started = False
        self._prefetch = min(config.prefetch_count, context.buffers.max_events)
        self._buffer: deque[RawEvent] = deque()
        self._pending: dict[str, tuple[Any, int]] = {}
        self._settling: set[str] = set()
        self._pending_bytes = 0
        self._wake = asyncio.Event()
        self._failure: asyncio.Future[TransportError | None] = self._loop.create_future()
        self._error: TransportError | None = None
        self._readiness: TransportReadiness | None = None
        self._monitor: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._closing = False
        self._closed = False
        self._receive_started = False

    def _remaining(self, deadline: float) -> float:
        """Calculate nonnegative remaining time in the shared monotonic domain.

        :param deadline: Absolute cleanup deadline in the runtime monotonic clock domain.
        :returns: Remaining seconds until deadline, clamped to zero.
        """
        return max(0.0, deadline - self._context.clock.monotonic())

    async def _open(self, native: Any) -> None:
        """Verify the exchange, bind an exclusive queue, and confirm its consumer.

        :param native: Lazily loaded native client module or an equivalent controlled test double.
        :returns: None once readiness and independent cancellation monitoring are installed.
        :raises TransportError: If connection, permissions, exchange lookup, or consume-ok fails.
        :raises TimeoutError: If an operation exhausts the startup budget.
        """
        subscription = False
        try:
            self._connection = await native.connect(
                self._config.url, timeout=self._remaining(self._context.startup_deadline)
            )
            self._connection.close_callbacks.add(self._on_connection_close)
            self._channel = await self._connection.channel(publisher_confirms=False)
            self._channel.close_callbacks.add(self._on_channel_close)
            self._underlay = await self._channel.get_underlay_channel()
            if not isinstance(getattr(self._underlay, "consumers", None), Mapping):
                raise TransportReadinessError("RabbitMQ client cannot monitor consumer cancellation")
            subscription = True
            exchange = await self._channel.get_exchange(self._source.name, ensure=True)
            await self._channel.set_qos(
                prefetch_count=self._prefetch, timeout=self._remaining(self._context.startup_deadline)
            )
            self._queue = await self._channel.declare_queue(
                "",
                durable=False,
                exclusive=True,
                auto_delete=True,
                arguments={"x-queue-type": "classic"},
                timeout=self._remaining(self._context.startup_deadline),
            )
            captured = self._context.clock.monotonic()
            for key in self._config.binding_keys:
                await self._queue.bind(
                    exchange, routing_key=key, timeout=self._remaining(self._context.startup_deadline)
                )
            # Bindings take effect independently. All bind acknowledgements and
            # consume-ok complete before the caller can perform its test action.
            tag = await self._queue.consume(
                self._on_message,
                no_ack=False,
                exclusive=True,
                consumer_tag=self._consumer_tag,
                timeout=self._remaining(self._context.startup_deadline),
            )
            self._consumer_started = True
            if tag != self._consumer_tag or tag not in self._underlay.consumers:
                raise TransportReadinessError("RabbitMQ consumer readiness was not confirmed")
            if self._error is not None:
                raise self._error
            if self._connection.is_closed or self._channel.is_closed:
                raise TransportReadinessError("RabbitMQ connection closed during startup")
            now = self._context.clock.monotonic()
            self._readiness = TransportReadiness(
                ObservationBoundary(self._source.name, "latest", {}, captured),
                _capabilities(),
                {"rabbitmq.prefetch_count": self._prefetch},
                now,
            )
            self._monitor = asyncio.create_task(
                self._watch_consumer(), name="event-observer-rabbitmq-consumer"
            )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise
        except Exception as error:
            normalized = _native_error(error, subscription=subscription)
            if normalized is error:
                raise
            raise normalized from error

    @property
    def readiness(self) -> TransportReadiness:
        """Expose confirmed queue-binding and consumer readiness.

        :returns: The resolved dedicated-subscription boundary and effective prefetch.
        :raises TransportReadinessError: If startup has not completed.
        """
        if self._readiness is None:
            raise TransportReadinessError("RabbitMQ session is not ready")
        return self._readiness

    def _fail(self, error: TransportError) -> None:
        """Retain one terminal failure and wake receive/monitoring waiters.

        :param error: Failure to normalize or publish without copying its message into safe diagnostics.
        :returns: None; intentional closure and later failures do not replace the original error.
        """
        if self._closing or self._error is not None:
            return
        self._error = error
        if not self._failure.done():
            self._failure.set_result(error)
        self._wake.set()

    def _on_connection_close(self, _sender: Any, error: BaseException | None = None) -> None:
        """Translate unexpected native connection closure into terminal failure.

        :param _sender: Native callback sender; unused by the observer.
        :param error: Failure to normalize or publish without copying its message into safe diagnostics.
        :returns: None after updating the independent failure signal.
        """
        self._fail(TransportInterruptedError("RabbitMQ connection closed unexpectedly", cause=error))

    def _on_channel_close(self, _sender: Any, error: BaseException | None = None) -> None:
        """Translate unexpected native channel closure into terminal failure.

        :param _sender: Native callback sender; unused by the observer.
        :param error: Failure to normalize or publish without copying its message into safe diagnostics.
        :returns: None after updating the independent failure signal.
        """
        self._fail(TransportInterruptedError("RabbitMQ channel closed unexpectedly", cause=error))

    async def _watch_consumer(self) -> None:
        # aiormq 6.9 removes broker-cancelled tags from this public registry but
        # exposes no cancel callback. Poll one owned tag, independent of receive.
        """Monitor the owned native tag for broker cancellation without waiting for data.

        :returns: None after cancellation or terminal failure; cancellation is propagated.
        :raises asyncio.CancelledError: If owned cleanup stops this monitor.
        """
        try:
            while not self._closing and self._error is None:
                if self._consumer_tag not in self._underlay.consumers:
                    self._fail(TransportSubscriptionError("RabbitMQ consumer was cancelled by the broker"))
                    return
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(TransportInterruptedError("RabbitMQ consumer monitoring failed", cause=error))

    @staticmethod
    def _headers(message: Any) -> tuple[tuple[str, bytes | None], ...]:
        """Detach the supported flat AMQP header domain without coercing unsupported values.

        :param message: Native delivery to detach and validate before handing it to Core.
        :returns: Ordered name/byte-or-null pairs; native text is encoded as UTF-8.
        :raises TransportSubscriptionError: If names or values are outside the supported flat header domain.
        """
        return _normalize_headers(message.headers)

    async def _on_message(self, message: Any) -> None:
        """Validate and reserve each native delivery until Core commits settlement.

        :param message: Native delivery to detach and validate before handing it to Core.
        :returns: None; malformed or oversized deliveries publish terminal failure instead of being dropped.
        """
        if self._closing or self._error is not None:
            return
        try:
            token = uuid4().hex
            routing_key = message.routing_key
            delivery_tag = message.delivery_tag
            if not isinstance(message.body, bytes) or not isinstance(routing_key, str):
                raise TransportSubscriptionError("RabbitMQ supplied invalid delivery metadata")
            if isinstance(delivery_tag, bool) or not isinstance(delivery_tag, int) or delivery_tag < 1:
                raise TransportSubscriptionError("RabbitMQ supplied an invalid delivery tag")
            raw = RawEvent(
                message.body,
                self._source.name,
                headers=self._headers(message),
                metadata={"rabbitmq.routing_key": routing_key, "rabbitmq.delivery_tag": delivery_tag},
                delivery_id=token,
            )
            size = raw_size(raw)
            if (
                len(self._pending) >= self._prefetch
                or self._pending_bytes + size > self._context.buffers.max_bytes
            ):
                raise TransportBackpressureError("RabbitMQ handoff capacity exhausted")
            self._pending[token] = (message, size)
            self._pending_bytes += size
            self._buffer.append(raw)
            self._wake.set()
        except Exception as error:
            self._fail(
                error
                if isinstance(error, TransportError)
                else TransportSubscriptionError("RabbitMQ delivery normalization failed", cause=error)
            )

    def receive(self) -> AsyncIterator[RawEvent]:
        """Reserve the single queue-delivery iterator.

        :returns: An asynchronous iterator of normalized deliveries.
        :raises TransportSubscriptionError: If receive was already reserved.
        """
        if self._receive_started:
            raise TransportSubscriptionError("A RabbitMQ session permits one receive iterator")
        self._receive_started = True
        return self._receive()

    async def _receive(self) -> AsyncIterator[RawEvent]:
        """Yield queued events while retaining native ownership until settlement.

        :yields: Detached RawEvent objects carrying opaque settlement tokens.
        :raises TransportError: If subscription monitoring or delivery normalization failed.
        """
        while True:
            if self._error is not None:
                raise self._error
            if self._closing:
                return
            if self._buffer:
                yield self._buffer.popleft()
            else:
                self._wake.clear()
                await self._wake.wait()

    async def wait_failure(self) -> TransportError | None:
        """Observe terminal failure even when the delivery handoff is full.

        :returns: The first terminal transport error, or None after intentional successful cleanup.
        :raises asyncio.CancelledError: If this monitoring waiter is cancelled; the shared signal remains
            live.
        """
        return await asyncio.shield(self._failure)

    async def settle(self, delivery_id: str, outcome: DeliveryOutcome) -> None:
        """Acknowledge exactly one token after Core commits its explicit safe outcome.

        :param delivery_id: Opaque session-local token returned with the original delivery.
        :param outcome: Committed admission, explicit filtering, or explicit Journal discard outcome.
        :returns: None after acknowledgement releases the delivery reservation.
        :raises TransportSubscriptionError: If outcome or token is invalid or already settling.
        :raises TransportInterruptedError: If acknowledgement fails or is cancelled.
        :raises TransportConnectionError: If cleanup has already started.
        """
        if not isinstance(outcome, DeliveryOutcome):
            raise TransportSubscriptionError("RabbitMQ requires an explicit safe delivery outcome")
        if self._error is not None:
            raise self._error
        if self._closing:
            raise TransportConnectionError("RabbitMQ session is closing")
        if delivery_id not in self._pending or delivery_id in self._settling:
            raise TransportSubscriptionError("Unknown or already settling RabbitMQ delivery token")
        message, size = self._pending[delivery_id]
        self._settling.add(delivery_id)
        try:
            await message.ack(multiple=False)
        except asyncio.CancelledError:
            self._fail(TransportInterruptedError("RabbitMQ delivery settlement was cancelled"))
            raise
        except Exception as error:
            normalized = TransportInterruptedError("RabbitMQ delivery settlement failed", cause=error)
            self._fail(normalized)
            raise normalized from error
        else:
            self._pending.pop(delivery_id)
            self._pending_bytes -= size
        finally:
            self._settling.discard(delivery_id)

    async def close(self, *, deadline: float) -> None:
        """Join one shielded cleanup task without allowing a caller to cancel it.

        :param deadline: Absolute cleanup deadline in the runtime monotonic clock domain.
        :returns: None after owned subscription resources close.
        :raises ObserverCleanupError: If owned queue deletion or connection cleanup fails.
        :raises ObserverCleanupTimeoutError: If queue deletion or connection cleanup exceeds its budget.
        """
        if self._close_task is None:
            self._closing = True
            self._wake.set()
            self._close_task = asyncio.create_task(
                self._cleanup(deadline), name="event-observer-rabbitmq-cleanup"
            )
        await asyncio.shield(self._close_task)

    async def _cleanup(self, deadline: float) -> None:
        """Cancel monitoring and release the owned queue, channel, and connection within budget.

        :param deadline: Absolute cleanup deadline in the runtime monotonic clock domain.
        :returns: None after successful cleanup; source exchanges and application queues are untouched.
        :raises ObserverCleanupError: If confirmed queue deletion or connection cleanup fails.
        :raises ObserverCleanupTimeoutError: If queue deletion or connection cleanup exceeds its budget.
        """
        if self._monitor is not None:
            self._monitor.cancel()
            await asyncio.gather(self._monitor, return_exceptions=True)
        try:
            queue_delete_error: Exception | None = None
            # Reserve time for connection close even if queue or channel RPCs stall.
            if self._consumer_started and self._queue is not None and not self._channel.is_closed:
                delete_queue = self._readiness is not None and self._error is None
                try:
                    async with asyncio.timeout(self._remaining(deadline) / 3):
                        if delete_queue:
                            # Automatic deletion can leave bindings routable after the queue stops.
                            await self._queue.delete(if_unused=False, if_empty=False)
                        else:
                            await self._queue.cancel(self._consumer_tag)
                except Exception as error:
                    if delete_queue:
                        queue_delete_error = error
            if self._channel is not None and not self._channel.is_closed:
                try:
                    async with asyncio.timeout(self._remaining(deadline) / 2):
                        await self._channel.close()
                except Exception:
                    pass
            if self._connection is not None:
                try:
                    async with asyncio.timeout(self._remaining(deadline)):
                        await self._connection.close()
                except TimeoutError as error:
                    raise ObserverCleanupTimeoutError(
                        "RabbitMQ connection cleanup deadline expired"
                    ) from error
                except Exception as error:
                    raise ObserverCleanupError("RabbitMQ connection cleanup failed") from error
            if isinstance(queue_delete_error, TimeoutError):
                raise ObserverCleanupTimeoutError(
                    "RabbitMQ queue deletion deadline expired", cause=queue_delete_error
                ) from queue_delete_error
            if queue_delete_error is not None:
                raise ObserverCleanupError("RabbitMQ queue deletion failed", cause=queue_delete_error) from (
                    queue_delete_error
                )
            self._closed = True
        except BaseException as error:
            if not self._failure.done():
                self._failure.set_result(
                    TransportInterruptedError("RabbitMQ resource cleanup did not finish", cause=error)
                )
            raise
        finally:
            if self._connection is not None:
                self._connection.close_callbacks.discard(self._on_connection_close)
            if self._channel is not None:
                self._channel.close_callbacks.discard(self._on_channel_close)
            self._buffer.clear()
            self._pending.clear()
            self._pending_bytes = 0
            if self._closed and not self._failure.done():
                self._failure.set_result(None)


__all__ = ["RabbitMQAdapter", "RabbitMQConfig", "RabbitMQSession"]
