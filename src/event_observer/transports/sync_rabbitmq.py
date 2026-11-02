"""Worker-owned Pika RabbitMQ subscriptions with finite native I/O.

Pika's public process_data_events timer bounds polling, but its public callback
clock does not run inside blocking RPCs. The small native boundary below uses
Pika 1.4.x's transport timer to terminate only the owned connection when an RPC
exceeds its deadline. No asyncio runtime or auxiliary I/O thread is created.

:ivar _IO_SLICE: Maximum native polling interval between actor control opportunities.
:ivar _CONTROL_TIMEOUT: Finite allowance for acknowledgement output flushing.
:ivar __all__: Public blocking RabbitMQ adapter and session types.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
import importlib
import os
import threading
from typing import Any, TypeVar
from uuid import uuid4

from ..errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    ProcessAffinityError,
    ThreadAffinityError,
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
    TransportReadiness,
    TransportSource,
)
from .base import DeliveryOutcome
from .rabbitmq import RabbitMQConfig, _capabilities, _native_error, _normalize_headers
from .sync_base import validate_poll_timeout

_IO_SLICE = 0.05
_CONTROL_TIMEOUT = 1.0
T = TypeVar("T")


def _load_native() -> Any:
    """Load the optional blocking native client only when opening a reader.

    :returns: The Pika module without importing its asyncio adapter.
    :raises ImportError: If the RabbitMQ extra was not installed.
    """
    return importlib.import_module("pika")


class SyncRabbitMQAdapter:
    """Create independent blocking subscriptions to an existing exchange.

    :ivar config: Shared immutable RabbitMQ URL, binding, and prefetch settings.
    :ivar transport_name: Canonical label shared with the asynchronous adapter.
    :ivar capabilities: Latest-only dedicated subscription and explicit settlement semantics.
    """

    transport_name = "RabbitMQAdapter"

    def __init__(self, config: RabbitMQConfig) -> None:
        """Retain configuration without opening a connection or starting a thread.

        :param config: Validated immutable subscription settings.
        :returns: None after retaining the configuration.
        :raises ConfigurationError: If config is not a RabbitMQConfig.
        """
        if not isinstance(config, RabbitMQConfig):
            raise ConfigurationError("SyncRabbitMQAdapter requires RabbitMQConfig")
        self.config = config

    @property
    def capabilities(self):
        """Describe the same broker semantics as the asynchronous RabbitMQ driver.

        :returns: Static latest-only, header, prefetch, and settlement capabilities.
        """
        return _capabilities()

    def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> SyncRabbitMQSession:
        """Prepare an exclusive queue and return only after every binding and consume-ok.

        :param source: Existing exchange selected for isolated observation.
        :param start: Latest-only native observation boundary.
        :param context: Actor clock, startup deadline, and bounded handoff configuration.
        :returns: A READY reader whose native resources belong to the calling worker.
        :raises UnsupportedCapabilityError: If replay or source partitions are requested.
        :raises MissingOptionalDependencyError: If Pika is not installed.
        :raises TransportError: If connection or subscription preparation fails.
        :raises TimeoutError: If native startup exhausts its absolute deadline.
        """
        if start.mode.value != "latest" or source.partitions is not None:
            raise UnsupportedCapabilityError(
                "RabbitMQ supports latest exchange subscriptions without partitions"
            )
        try:
            native = _load_native()
        except ImportError as error:
            raise MissingOptionalDependencyError("Blocking RabbitMQ requires the 'rabbitmq' extra") from error
        session = SyncRabbitMQSession(self.config, source, context)
        try:
            session._open(native)
        except BaseException as primary:
            try:
                session.close(deadline=context.clock.monotonic() + 1.0)
            except BaseException as cleanup:
                setattr(primary, "cleanup_error", cleanup)
            raise
        return session


class SyncRabbitMQSession:
    """Own one native connection and bounded set of unsettled observer deliveries.

    :ivar _config: Immutable connection URL, binding keys, and prefetch policy.
    :ivar _source: Existing source exchange; this session never creates or deletes it.
    :ivar _context: Shared clock, startup deadline, and handoff limits.
    :ivar _owner: Thread identity owning every native operation.
    :ivar _pid: Process identity preventing inherited resource use after fork.
    :ivar _connection: Native Pika BlockingConnection, created inside open.
    :ivar _channel: Owned native channel used for queue creation and acknowledgement.
    :ivar _queue: Server-named exclusive auto-delete observer queue.
    :ivar _consumer_tag: Unique consumer registration confirmed during startup.
    :ivar _prefetch: Configured prefetch clipped to the event handoff capacity.
    :ivar _buffer: Detached deliveries awaiting actor polling.
    :ivar _pending: Delivery-token to native-tag/accounted-size reservations.
    :ivar _settling: Tokens whose already-admitted deliveries are being acknowledged.
    :ivar _pending_bytes: Accounted bytes reserved by outstanding native deliveries.
    :ivar _error: First terminal transport failure; later deliveries cannot replace it.
    :ivar _readiness: Immutable boundary installed after successful consume-ok.
    :ivar _closing: Whether intentional cleanup has begun.
    :ivar _closed: Whether owned native connection resources were released.
    :ivar readiness: Confirmed dedicated subscription contract.
    """

    def __init__(self, config: RabbitMQConfig, source: TransportSource, context: OpenContext) -> None:
        """Allocate worker-local delivery bookkeeping without native I/O.

        :param config: Immutable RabbitMQ settings shared with the asynchronous adapter.
        :param source: Existing exchange to observe through an owned temporary queue.
        :param context: Actor clock, lifecycle boundary, and transfer limits.
        :returns: None after creating empty bounded handoff state.
        """
        self._config = config
        self._source = source
        self._context = context
        self._owner = threading.get_ident()
        self._pid = os.getpid()
        self._connection: Any = None
        self._channel: Any = None
        self._queue: str | None = None
        self._consumer_tag = "observer-" + uuid4().hex
        self._prefetch = min(config.prefetch_count, context.buffers.max_events)
        self._buffer: deque[RawEvent] = deque()
        self._pending: dict[str, tuple[int, int]] = {}
        self._settling: set[str] = set()
        self._pending_bytes = 0
        self._error: TransportError | None = None
        self._readiness: TransportReadiness | None = None
        self._closing = False
        self._closed = False

    def _check_owner(self) -> None:
        """Reject native operations from a different process or thread.

        :returns: None when the calling worker owns this native session.
        :raises ProcessAffinityError: If an inherited session is used after fork.
        :raises ThreadAffinityError: If another thread attempts native I/O.
        """
        if os.getpid() != self._pid:
            raise ProcessAffinityError("RabbitMQ session belongs to another process")
        if threading.get_ident() != self._owner:
            raise ThreadAffinityError("RabbitMQ session belongs to its I/O worker")

    def _remaining(self, deadline: float) -> float:
        """Calculate a nonnegative relative budget in the actor clock domain.

        :param deadline: Absolute startup or cleanup deadline supplied by the actor.
        :returns: Remaining seconds, clamped to zero.
        """
        return max(0.0, deadline - self._context.clock.monotonic())

    def _bounded_call(self, operation: Callable[[], T], deadline: float) -> T:
        """Bound a blocking Pika RPC using its pinned native transport timer.

        Pika 1.4.x processes this timer inside BlockingConnection._flush_output;
        its public call_later instead queues callbacks until RPC dispatch ends.
        Expiry asks the owned transport to terminate normally through its native
        abort path, so the pending RPC unwinds with the original timeout cause.

        :param operation: Native channel/connection operation invoked on its owning worker.
        :param deadline: Absolute operation deadline in the shared actor clock domain.
        :returns: The native operation result before the deadline.
        :raises TimeoutError: If no operation budget remains or the native timer expires.
        :raises Exception: If the native operation fails before its deadline.
        """
        remaining = self._remaining(deadline)
        if remaining <= 0:
            raise TimeoutError("RabbitMQ operation deadline expired")
        impl = self._connection._impl
        expired = False
        timeout = TimeoutError("RabbitMQ operation deadline expired")

        def expire() -> None:
            """Abort the owned native stream when its RPC deadline is reached.

            :returns: None after initiating nonblocking socket termination.
            """
            nonlocal expired
            expired = True
            impl._terminate_stream(timeout)

        timer = impl._adapter_call_later(remaining, expire)
        try:
            result = operation()
            if expired or self._remaining(deadline) <= 0:
                raise timeout
            return result
        finally:
            if not self._connection.is_closed:
                impl._adapter_remove_timeout(timer)

    def _open(self, native: Any) -> None:
        """Open the native connection and prepare the complete isolated subscription.

        :param native: Lazily imported Pika module or a controlled native test double.
        :returns: None after the complete subscription has confirmed readiness.
        :raises TransportError: If authentication, exchange lookup, bindings, or consume-ok fails.
        :raises TimeoutError: If startup exceeds its shared-clock deadline.
        """
        subscription = False
        try:
            remaining = self._remaining(self._context.startup_deadline)
            if remaining <= 0:
                raise TimeoutError("RabbitMQ startup deadline expired")
            parameters = native.URLParameters(self._config.url)
            parameters.connection_attempts = 1
            parameters.retry_delay = 0
            parameters.socket_timeout = min(1.0, remaining)
            parameters.stack_timeout = remaining
            parameters.blocked_connection_timeout = min(_CONTROL_TIMEOUT, remaining)
            self._connection = native.BlockingConnection(parameters)
            impl = self._connection._impl
            if not all(
                callable(getattr(impl, name, None))
                for name in ("_adapter_call_later", "_adapter_remove_timeout", "_terminate_stream")
            ):
                raise TransportReadinessError("RabbitMQ client cannot enforce RPC deadlines")
            deadline = self._context.startup_deadline
            self._channel = self._bounded_call(self._connection.channel, deadline)
            subscription = True
            self._bounded_call(
                lambda: self._channel.exchange_declare(exchange=self._source.name, passive=True), deadline
            )
            self._bounded_call(lambda: self._channel.basic_qos(prefetch_count=self._prefetch), deadline)
            queue = self._bounded_call(
                lambda: self._channel.queue_declare(
                    queue="",
                    durable=False,
                    exclusive=True,
                    auto_delete=True,
                    arguments={"x-queue-type": "classic"},
                ),
                deadline,
            )
            self._queue = queue.method.queue
            if not isinstance(self._queue, str) or not self._queue:
                raise TransportReadinessError("RabbitMQ server did not name the observer queue")
            captured = self._context.clock.monotonic()
            for key in self._config.binding_keys:
                self._bounded_call(
                    lambda key=key: self._channel.queue_bind(
                        queue=self._queue, exchange=self._source.name, routing_key=key
                    ),
                    deadline,
                )
            self._channel.add_on_cancel_callback(self._on_cancel)
            tag = self._bounded_call(
                lambda: self._channel.basic_consume(
                    queue=self._queue,
                    on_message_callback=self._on_message,
                    auto_ack=False,
                    exclusive=True,
                    consumer_tag=self._consumer_tag,
                ),
                deadline,
            )
            # BlockingChannel may queue a broker cancellation while Consume-ok
            # is being awaited. Dispatch it before publishing readiness.
            self._bounded_call(lambda: self._connection.process_data_events(time_limit=0), deadline)
            if tag != self._consumer_tag or self.failure() is not None:
                raise self._error or TransportReadinessError("RabbitMQ consume-ok was not confirmed")
            now = self._context.clock.monotonic()
            self._readiness = TransportReadiness(
                ObservationBoundary(self._source.name, "latest", {}, captured),
                _capabilities(),
                {"rabbitmq.prefetch_count": self._prefetch},
                now,
            )
        except TimeoutError:
            raise
        except Exception as error:
            normalized = _native_error(error, subscription=subscription)
            if normalized is error:
                raise
            raise normalized from error

    @property
    def readiness(self) -> TransportReadiness:
        """Return the immutable subscription boundary after complete native readiness.

        :returns: The owned queue's latest boundary and effective prefetch metadata.
        :raises TransportReadinessError: If subscription setup has not completed.
        """
        if self._readiness is None:
            raise TransportReadinessError("RabbitMQ session is not ready")
        return self._readiness

    def _fail(self, error: TransportError) -> None:
        """Retain the first terminal failure independently of delivery-buffer capacity.

        :param error: Safe typed transport failure with its original local cause.
        :returns: None; intentional cleanup and later failures do not overwrite an existing failure.
        """
        if not self._closing and self._error is None:
            self._error = error

    def _on_cancel(self, frame: Any) -> None:
        """Observe broker cancellation without waiting for another business delivery.

        :param frame: Native Basic.Cancel method frame for an owned consumer.
        :returns: None after recording terminal subscription loss.
        """
        if getattr(getattr(frame, "method", None), "consumer_tag", None) == self._consumer_tag:
            self._fail(TransportSubscriptionError("RabbitMQ consumer was cancelled by the broker"))

    def _on_message(self, channel: Any, method: Any, properties: Any, body: bytes) -> None:
        """Detach and reserve one delivery until Core commits its safe settlement outcome.

        :param channel: Native callback channel, already owned by this worker.
        :param method: Delivery metadata containing routing key and native delivery tag.
        :param properties: Native AMQP properties containing the flat header table.
        :param body: Raw body; null or nonbyte bodies are unsupported.
        :returns: None after enqueueing a bounded raw delivery or recording terminal failure.
        """
        if self._closing or self._error is not None:
            return
        try:
            tag, routing_key = method.delivery_tag, method.routing_key
            if not isinstance(body, bytes) or not isinstance(routing_key, str):
                raise TransportSubscriptionError("RabbitMQ supplied invalid delivery metadata")
            if isinstance(tag, bool) or not isinstance(tag, int) or tag < 1:
                raise TransportSubscriptionError("RabbitMQ supplied an invalid delivery tag")
            raw = RawEvent(
                body,
                self._source.name,
                headers=_normalize_headers(properties.headers),
                metadata={
                    "rabbitmq.routing_key": routing_key,
                    "rabbitmq.delivery_tag": tag,
                },
                delivery_id=uuid4().hex,
            )
            size = raw_size(raw)
            settling_bytes = sum(self._pending[token][1] for token in self._settling)
            if len(self._pending) - len(self._settling) >= self._prefetch or (
                self._pending_bytes - settling_bytes + size > self._context.buffers.max_bytes
            ):
                raise TransportBackpressureError("RabbitMQ handoff capacity exhausted")
            token = raw.delivery_id
            assert token is not None
            self._pending[token] = (tag, size)
            self._pending_bytes += size
            self._buffer.append(raw)
        except Exception as error:
            self._fail(
                error
                if isinstance(error, TransportError)
                else TransportSubscriptionError("RabbitMQ delivery normalization failed", cause=error)
            )

    def failure(self) -> TransportError | None:
        """Inspect terminal state without waiting for a free delivery-buffer slot.

        :returns: The first terminal error, or None while healthy or intentionally closed.
        :raises ProcessAffinityError: If native state is inspected from another process.
        :raises ThreadAffinityError: If native state is inspected from another thread.
        """
        self._check_owner()
        if self._error is None and not self._closing:
            if self._connection is not None and self._connection.is_closed:
                self._fail(TransportInterruptedError("RabbitMQ connection closed unexpectedly"))
            elif self._channel is not None and self._channel.is_closed:
                cause = getattr(self._channel, "_closing_reason", None)
                self._fail(
                    _native_error(cause, subscription=True)
                    if isinstance(cause, Exception)
                    else TransportInterruptedError("RabbitMQ channel closed unexpectedly")
                )
        return self._error

    def poll(self, timeout: float) -> RawEvent | None:
        """Pump bounded native I/O and return one detached event, prioritizing known failure.

        :param timeout: Finite nonnegative read allowance; zero pumps without waiting.
        :returns: One queued event, or None after an empty poll or intentional close.
        :raises ConfigurationError: If timeout is negative, boolean, or nonfinite.
        :raises TransportError: If connection, consumer, decoding, or handoff state has failed.
        """
        self._check_owner()
        validate_poll_timeout(timeout)
        if (error := self.failure()) is not None:
            raise error
        if self._closing or self._closed:
            return None
        try:
            self._connection.process_data_events(time_limit=0 if self._buffer else min(timeout, _IO_SLICE))
        except Exception as error:
            self._fail(_native_error(error))
        if (error := self.failure()) is not None:
            raise error
        return self._buffer.popleft() if self._buffer else None

    def settle(self, delivery_id: str, outcome: DeliveryOutcome) -> None:
        """Acknowledge one already-committed delivery and release its transfer reservation.

        :param delivery_id: Opaque token accompanying the actor's original RawEvent.
        :param outcome: Explicit safe admission, filtering, or configured discard outcome.
        :returns: None after native acknowledgement output has been flushed.
        :raises TransportSubscriptionError: If the outcome or delivery token is invalid.
        :raises TransportError: If the session failed or acknowledgement output cannot be flushed.
        """
        self._check_owner()
        if not isinstance(outcome, DeliveryOutcome):
            raise TransportSubscriptionError("RabbitMQ requires an explicit safe delivery outcome")
        if (error := self.failure()) is not None:
            raise error
        if self._closing:
            raise TransportConnectionError("RabbitMQ session is closing")
        if delivery_id not in self._pending or delivery_id in self._settling:
            raise TransportSubscriptionError("Unknown or already settling RabbitMQ delivery token")
        tag, size = self._pending[delivery_id]
        self._settling.add(delivery_id)
        try:
            self._bounded_call(
                lambda: self._channel.basic_ack(delivery_tag=tag, multiple=False),
                self._context.clock.monotonic() + _CONTROL_TIMEOUT,
            )
        except Exception as error:
            normalized = TransportInterruptedError("RabbitMQ delivery settlement failed", cause=error)
            self._fail(normalized)
            raise normalized from error
        else:
            self._pending.pop(delivery_id)
            self._pending_bytes -= size
        finally:
            self._settling.discard(delivery_id)

    def _abort(self, cause: Exception) -> None:
        """Force only the owned native stream closed without another blocking protocol RPC.

        :param cause: Timeout or cleanup failure retained by native connection termination.
        :returns: None after native stream termination has been driven to completion.
        :raises ObserverCleanupError: If Pika does not confirm owned socket termination.
        """
        if self._connection is None or self._connection.is_closed:
            return
        self._connection._impl._terminate_stream(cause)
        try:
            self._connection.process_data_events(time_limit=0)
        except Exception:
            pass
        if not self._connection.is_closed:
            raise ObserverCleanupError("RabbitMQ owned connection abort was not confirmed", cause=cause)

    def close(self, *, deadline: float) -> None:
        """Close only the owned connection within its absolute deadline, aborting on timeout.

        Closing the connection destroys the exclusive observer queue and performs
        no exchange deletion or application-queue operation. Pending deliveries
        are never acknowledged by this cleanup path.

        :param deadline: Absolute cleanup deadline in the shared actor clock domain.
        :returns: None after native resources are released; repeated successful close is harmless.
        :raises ObserverCleanupTimeoutError: If protocol cleanup exceeds its deadline.
        :raises ObserverCleanupError: If protocol cleanup or forced socket release fails.
        """
        self._check_owner()
        if self._closed:
            return
        self._closing = True
        try:
            if self._connection is not None and not self._connection.is_closed:
                self._bounded_call(self._connection.close, deadline)
        except TimeoutError as error:
            self._abort(error)
            raise ObserverCleanupTimeoutError(
                "RabbitMQ connection cleanup deadline expired", cause=error
            ) from error
        except Exception as error:
            self._abort(error)
            raise ObserverCleanupError("RabbitMQ connection cleanup failed", cause=error) from error
        finally:
            if self._connection is None or self._connection.is_closed:
                self._closed = True
                self._buffer.clear()
                self._pending.clear()
                self._settling.clear()
                self._pending_bytes = 0


__all__ = ["SyncRabbitMQAdapter", "SyncRabbitMQSession"]
