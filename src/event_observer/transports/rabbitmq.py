"""Independent RabbitMQ exchange subscriptions with admission-time settlement.

Each watch owns a connection and a server-named exclusive classic queue. No
application queue is consumed and no automatic reconnection is attempted.
The aio-pika import is deferred until opening a session.
"""

from __future__ import annotations

import asyncio
import importlib
from collections import deque
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any
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


@dataclass(frozen=True, slots=True)
class RabbitMQConfig:
    url: str = field(repr=False)
    binding_keys: tuple[str, ...] = ("#",)
    prefetch_count: int = 100

    def __post_init__(self) -> None:
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
    return importlib.import_module("aio_pika")


def _native_error(error: Exception, *, subscription: bool = False) -> TransportError:
    """Classify native exceptions without displaying native messages or URLs."""
    if isinstance(error, TransportError):
        return error
    names = {cls.__name__ for cls in type(error).__mro__}
    if names.intersection({"AuthenticationError", "ProbableAuthenticationError"}):
        return TransportAuthenticationError("RabbitMQ authentication failed", cause=error)
    if names.intersection({"ChannelAccessRefused", "ConnectionNotAllowed"}):
        return TransportAuthorizationError("RabbitMQ permission check failed", cause=error)
    if subscription:
        return TransportSubscriptionError("RabbitMQ subscription preparation failed", cause=error)
    return TransportConnectionError("RabbitMQ connection operation failed", cause=error)


class RabbitMQAdapter:
    """Create a private queue bound to an existing exchange for every watch."""

    def __init__(self, config: RabbitMQConfig) -> None:
        if not isinstance(config, RabbitMQConfig):
            raise ConfigurationError("RabbitMQAdapter requires RabbitMQConfig")
        self.config = config

    @property
    def capabilities(self) -> TransportCapabilities:
        return _capabilities()

    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> RabbitMQSession:
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
    """One private subscription and an independently observable failure signal."""

    def __init__(self, config: RabbitMQConfig, source: TransportSource, context: OpenContext) -> None:
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
        return max(0.0, deadline - self._context.clock.monotonic())

    async def _open(self, native: Any) -> None:
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
        if self._readiness is None:
            raise TransportReadinessError("RabbitMQ session is not ready")
        return self._readiness

    def _fail(self, error: TransportError) -> None:
        if self._closing or self._error is not None:
            return
        self._error = error
        if not self._failure.done():
            self._failure.set_result(error)
        self._wake.set()

    def _on_connection_close(self, _sender: Any, error: BaseException | None = None) -> None:
        self._fail(TransportInterruptedError("RabbitMQ connection closed unexpectedly", cause=error))

    def _on_channel_close(self, _sender: Any, error: BaseException | None = None) -> None:
        self._fail(TransportInterruptedError("RabbitMQ channel closed unexpectedly", cause=error))

    async def _watch_consumer(self) -> None:
        # aiormq 6.9 removes broker-cancelled tags from this public registry but
        # exposes no cancel callback. Poll one owned tag, independent of receive.
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
        headers = message.headers
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
                # pamqp decodes AMQP byte-array table values as bytearray.
                value = bytes(value)
            elif value is not None and not isinstance(value, bytes):
                raise TransportSubscriptionError("RabbitMQ header values must be strings, bytes, or null")
            result.append((name, value))
        return tuple(result)

    async def _on_message(self, message: Any) -> None:
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
        if self._receive_started:
            raise TransportSubscriptionError("A RabbitMQ session permits one receive iterator")
        self._receive_started = True
        return self._receive()

    async def _receive(self) -> AsyncIterator[RawEvent]:
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
        return await asyncio.shield(self._failure)

    async def settle(self, delivery_id: str, outcome: DeliveryOutcome) -> None:
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
        if self._close_task is None:
            self._closing = True
            self._wake.set()
            self._close_task = asyncio.create_task(
                self._cleanup(deadline), name="event-observer-rabbitmq-cleanup"
            )
        await asyncio.shield(self._close_task)

    async def _cleanup(self, deadline: float) -> None:
        if self._monitor is not None:
            self._monitor.cancel()
            await asyncio.gather(self._monitor, return_exceptions=True)
        try:
            # Reserve time for closing the connection even if cancel/channel
            # RPCs fail or stall. Closing it destroys the exclusive queue.
            if self._consumer_started and self._queue is not None and not self._channel.is_closed:
                try:
                    async with asyncio.timeout(self._remaining(deadline) / 3):
                        await self._queue.cancel(self._consumer_tag)
                except Exception:
                    pass
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
