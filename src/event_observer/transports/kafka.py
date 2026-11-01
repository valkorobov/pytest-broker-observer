"""Independent Kafka readers with explicit boundaries and a bounded handoff.

The native client is imported only when opening a session. Every Consumer call,
including construction and destruction, runs on that session's worker thread.
The Python bridge conservatively holds one record at a time, so its configured
byte budget also covers the producer-side record awaiting delivery. Native
librdkafka queues have a separate, explicitly configured budget.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import math
import threading
from collections import deque
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from ..errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    ObserverCleanupError,
    ObserverCleanupTimeoutError,
    StartPositionUnavailableError,
    TopologyChangedError,
    TransportAuthenticationError,
    TransportAuthorizationError,
    TransportBackpressureError,
    TransportConnectionError,
    TransportError,
    TransportReadinessError,
    TransportSubscriptionError,
)
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
        "bootstrap.servers",
        "group.id",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "auto.offset.reset",
        "allow.auto.create.topics",
        "enable.partition.eof",
        "isolation.level",
        "error_cb",
        "stats_cb",
        "statistics.interval.ms",
        "security.protocol",
        "sasl.mechanism",
        "sasl.mechanisms",
        "sasl.username",
        "sasl.password",
        "ssl.ca.location",
        "ssl.certificate.location",
        "ssl.key.location",
        "ssl.key.password",
        "queued.max.messages.kbytes",
        "queued.min.messages",
        "fetch.max.bytes",
        "max.partition.fetch.bytes",
        "fetch.wait.max.ms",
        "enable.ssl.certificate.verification",
        "ssl.endpoint.identification.algorithm",
    }
)


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    bootstrap_servers: str | tuple[str, ...]
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = field(default=None, repr=False)
    sasl_password: str | None = field(default=None, repr=False)
    ssl_ca_location: str | None = None
    ssl_certificate_location: str | None = None
    ssl_key_location: str | None = field(default=None, repr=False)
    ssl_key_password: str | None = field(default=None, repr=False)
    isolation_level: str = "read_committed"
    topology_check_interval: float = 1.0
    poll_timeout: float = 0.05
    metadata_timeout: float = 1.0
    native_queue_bytes: int = 16 * 1024 * 1024
    native_fetch_bytes: int = 1024 * 1024
    client_options: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        servers = self.bootstrap_servers
        if not isinstance(servers, (str, tuple)) or not servers:
            raise ConfigurationError("bootstrap_servers must be a nonempty string or tuple")
        addresses = (servers,) if isinstance(servers, str) else servers
        if any(not isinstance(address, str) or not address.strip() for address in addresses):
            raise ConfigurationError("bootstrap_servers contains an empty or invalid address")
        if self.security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise ConfigurationError("Unsupported Kafka security_protocol")
        if self.isolation_level not in {"read_committed", "read_uncommitted"}:
            raise ConfigurationError("Unsupported Kafka isolation_level")
        for name in ("topology_check_interval", "poll_timeout", "metadata_timeout"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ConfigurationError(f"{name} must be positive and finite")
        for name in ("native_queue_bytes", "native_fetch_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ConfigurationError(f"{name} must be a positive integer")
        if self.native_queue_bytes < 1024 or self.native_fetch_bytes > self.native_queue_bytes:
            raise ConfigurationError(
                "Native fetch budget must fit the native queue budget (at least 1024 bytes)"
            )
        sasl = self.security_protocol.startswith("SASL_")
        if sasl and not self.sasl_mechanism:
            raise ConfigurationError("SASL security requires sasl_mechanism")
        if not sasl and any(
            value is not None for value in (self.sasl_mechanism, self.sasl_username, self.sasl_password)
        ):
            raise ConfigurationError("SASL settings require a SASL security_protocol")
        if sasl and self.sasl_mechanism in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}:
            if self.sasl_username is None or self.sasl_password is None:
                raise ConfigurationError("The selected SASL mechanism requires username and password")
        tls_settings = (
            self.ssl_ca_location,
            self.ssl_certificate_location,
            self.ssl_key_location,
            self.ssl_key_password,
        )
        if any(value is not None for value in tls_settings) and not self.security_protocol.endswith("SSL"):
            raise ConfigurationError("TLS settings require SSL or SASL_SSL")
        if bool(self.ssl_certificate_location) != bool(self.ssl_key_location):
            raise ConfigurationError("TLS client certificate and key must be configured together")
        if self.ssl_key_password is not None and self.ssl_key_location is None:
            raise ConfigurationError("ssl_key_password requires ssl_key_location")
        options = dict(self.client_options)
        if _MANAGED_OPTIONS.intersection(options):
            raise ConfigurationError("client_options cannot override observer-managed Kafka settings")
        if any(
            not isinstance(key, str) or not isinstance(value, (str, int, float, bool))
            for key, value in options.items()
        ):
            raise ConfigurationError("client_options must contain scalar native configuration values")
        object.__setattr__(self, "client_options", MappingProxyType(options))


def _capabilities() -> TransportCapabilities:
    return TransportCapabilities(
        start_modes=frozenset({"latest", "earliest", "offset", "timestamp"}),
        isolation_modes=frozenset({"independent_reader"}),
        flow_control_modes=frozenset({"fetch_limit", "pause_resume"}),
        ordering_scope="partition",
        supports_headers=True,
        supports_key=True,
        supports_source_timestamps=True,
        supports_position_tracking=True,
        supports_topology_detection=True,
        supports_settlement=False,
    )


def _load_native() -> Any:
    return importlib.import_module("confluent_kafka")


def _load_admin() -> Any:
    return importlib.import_module("confluent_kafka.admin")


class KafkaAdapter:
    """Factory of independent manually assigned Kafka sessions."""

    def __init__(self, config: KafkaConfig) -> None:
        if not isinstance(config, KafkaConfig):
            raise ConfigurationError("KafkaAdapter requires KafkaConfig")
        self.config = config

    @property
    def capabilities(self) -> TransportCapabilities:
        return _capabilities()

    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> KafkaSession:
        try:
            native = _load_native()
        except ImportError as exc:
            raise MissingOptionalDependencyError(
                "Kafka requires the 'kafka' extra: pip install 'pytest-broker-observer[kafka]'"
            ) from exc
        session = KafkaSession(self.config, native, source, start, context)
        session._thread.start()
        try:
            remaining = max(0.0, context.startup_deadline - context.clock.monotonic())
            await asyncio.wait_for(asyncio.shield(session._ready), timeout=remaining)
        except BaseException as primary:
            try:
                await session.close(deadline=context.clock.monotonic() + 2.0)
            except BaseException as cleanup:
                # Startup/cancellation remains the primary failure.
                setattr(primary, "cleanup_error", cleanup)
            raise
        return session


class KafkaSession:
    """One worker, one receive iterator and an independent failure signal."""

    def __init__(
        self,
        config: KafkaConfig,
        native: Any,
        source: TransportSource,
        start: StartPosition,
        context: OpenContext,
    ) -> None:
        self._config = config
        self._native = native
        self._source = source
        self._start = start
        self._context = context
        self._loop = asyncio.get_running_loop()
        self._ready: asyncio.Future[None] = self._loop.create_future()
        self._failure: asyncio.Future[TransportError | None] = self._loop.create_future()
        self._finished: asyncio.Future[None] = self._loop.create_future()
        self._data_ready = asyncio.Event()
        self._condition = threading.Condition()
        self._queue: deque[RawEvent] = deque()
        self._stop = threading.Event()
        self._wake_pending = False
        self._error: TransportError | None = None
        self._cleanup_error: BaseException | None = None
        self._receive_started = False
        self._readiness: TransportReadiness | None = None
        self._fetch_confirmed: set[int] = set()
        self._partitions: tuple[int, ...] = ()
        self._topology: tuple[int, ...] = ()
        self._highs: dict[int, int] = {}
        self._consumer: Any = None
        self._admin: Any = None
        self._thread = threading.Thread(
            target=self._run, name=f"event-observer-kafka-{uuid4().hex[:8]}", daemon=True
        )

    @property
    def readiness(self) -> TransportReadiness:
        if self._readiness is None:
            raise TransportReadinessError("Kafka session is not ready")
        return self._readiness

    async def wait_failure(self) -> TransportError | None:
        return await asyncio.shield(self._failure)

    def receive(self) -> AsyncIterator[RawEvent]:
        if self._receive_started:
            raise TransportSubscriptionError("A Kafka session permits one receive iterator")
        self._receive_started = True
        return self._receive()

    async def _receive(self) -> AsyncIterator[RawEvent]:
        while True:
            with self._condition:
                if self._error is not None:
                    raise self._error
                if self._queue:
                    event = self._queue.popleft()
                    self._condition.notify_all()
                elif self._stop.is_set():
                    return
                else:
                    self._data_ready.clear()
                    event = None
            if event is not None:
                yield event
            else:
                await self._data_ready.wait()

    async def close(self, *, deadline: float) -> None:
        self._stop.set()
        with self._condition:
            self._condition.notify_all()
        self._wake_loop()
        remaining = max(0.0, deadline - self._context.clock.monotonic())
        try:
            await asyncio.wait_for(asyncio.shield(self._finished), timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise ObserverCleanupTimeoutError("Kafka worker did not stop within the cleanup budget") from exc
        # The completion callback can reach the loop just before the worker's
        # final Python frame returns. Confirm thread termination as well.
        while self._thread.is_alive():
            remaining = deadline - self._context.clock.monotonic()
            if remaining <= 0:
                raise ObserverCleanupTimeoutError("Kafka worker did not terminate within the cleanup budget")
            await asyncio.sleep(min(0.001, remaining))
        self._thread.join(timeout=0)
        if self._cleanup_error is not None:
            raise self._cleanup_error

    def _wake_loop(self) -> None:
        with self._condition:
            if self._wake_pending:
                return
            self._wake_pending = True
        try:
            self._loop.call_soon_threadsafe(self._notify)
        except RuntimeError:
            pass

    def _notify(self) -> None:
        with self._condition:
            self._wake_pending = False
        self._data_ready.set()
        if self._stop.is_set() and not self._ready.done():
            # The outer startup timeout/cancellation owns its outcome. A late
            # exception on its already-cancelled shield would otherwise be
            # reported as an unhandled Future exception on Python 3.14.
            self._ready.cancel()
        if self._error is not None:
            if not self._failure.done():
                self._failure.set_result(self._error)
            if not self._ready.done():
                self._ready.set_exception(self._error)
        if self._readiness is not None and not self._ready.done():
            self._ready.set_result(None)
        if not self._thread.is_alive() or getattr(self, "_worker_finished", False):
            if not self._failure.done():
                self._failure.set_result(None)
            if not self._ready.done():
                self._ready.cancel()
            if not self._finished.done():
                self._finished.set_result(None)

    def _fail(self, error: TransportError) -> None:
        with self._condition:
            if self._error is None:
                self._error = error
            self._condition.notify_all()
        self._wake_loop()

    def _remaining(self) -> float:
        remaining = self._context.startup_deadline - self._context.clock.monotonic()
        if remaining <= 0 or self._stop.is_set():
            raise TransportReadinessError("Kafka fetch readiness was not confirmed within the startup budget")
        return min(self._config.metadata_timeout, remaining)

    def _native_config(self) -> dict[str, Any]:
        config = self._config
        result: dict[str, Any] = dict(config.client_options)
        result.update(
            {
                "bootstrap.servers": config.bootstrap_servers
                if isinstance(config.bootstrap_servers, str)
                else ",".join(config.bootstrap_servers),
                "group.id": f"event-observer-{self._context.observer_id}-{uuid4().hex}",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "error",
                "allow.auto.create.topics": False,
                "enable.partition.eof": True,
                "isolation.level": config.isolation_level,
                "security.protocol": config.security_protocol,
                "queued.max.messages.kbytes": max(1, config.native_queue_bytes // 1024),
                "queued.min.messages": 1,
                "fetch.max.bytes": config.native_fetch_bytes,
                "max.partition.fetch.bytes": config.native_fetch_bytes,
                "fetch.wait.max.ms": 100,
                "statistics.interval.ms": 100,
                "error_cb": self._error_callback,
                "stats_cb": self._statistics_callback,
            }
        )
        for field_name, native_name in (
            ("sasl_mechanism", "sasl.mechanism"),
            ("sasl_username", "sasl.username"),
            ("sasl_password", "sasl.password"),
            ("ssl_ca_location", "ssl.ca.location"),
            ("ssl_certificate_location", "ssl.certificate.location"),
            ("ssl_key_location", "ssl.key.location"),
            ("ssl_key_password", "ssl.key.password"),
        ):
            value = getattr(config, field_name)
            if value is not None:
                result[native_name] = value
        return result

    def _normalize(self, native_error: Any) -> TransportError:
        code = native_error.code() if callable(getattr(native_error, "code", None)) else None
        names = {
            TransportAuthenticationError: ("_AUTHENTICATION", "SASL_AUTHENTICATION_FAILED"),
            TransportAuthorizationError: (
                "TOPIC_AUTHORIZATION_FAILED",
                "GROUP_AUTHORIZATION_FAILED",
                "CLUSTER_AUTHORIZATION_FAILED",
            ),
            StartPositionUnavailableError: ("OFFSET_OUT_OF_RANGE", "_AUTO_OFFSET_RESET"),
            TransportSubscriptionError: ("UNKNOWN_TOPIC_OR_PART", "_UNKNOWN_TOPIC", "_UNKNOWN_PARTITION"),
        }
        for error_type, constants in names.items():
            if any(code == getattr(self._native.KafkaError, name, object()) for name in constants):
                return error_type(f"Kafka operation failed ({error_type.__name__}, code={code})")
        return TransportConnectionError(f"Kafka reader operation failed (code={code})")

    def _error_callback(self, error: Any) -> None:
        # Callback details may contain server-supplied strings or credentials.
        self._fail(self._normalize(error))

    def _statistics_callback(self, payload: str) -> None:
        # hi_offset/ls_offset are updated by Fetch responses. Unlike a metadata
        # request, a successful Fetch confirms the Read path, including a reader
        # waiting beyond the LSO of an existing open transaction.
        try:
            partitions = (
                json.loads(payload).get("topics", {}).get(self._source.name, {}).get("partitions", {})
            )
            for partition in self._partitions:
                info = partitions.get(str(partition), {})
                if (
                    info.get("fetch_state") == "active"
                    and info.get("hi_offset", -1) >= 0
                    and info.get("ls_offset", -1) >= 0
                ):
                    self._fetch_confirmed.add(partition)
        except (ValueError, TypeError, AttributeError):
            # Malformed optional statistics cannot manufacture readiness.
            return

    def _metadata(self, timeout: float) -> tuple[int, ...]:
        metadata = self._consumer.list_topics(topic=self._source.name, timeout=timeout)
        topic = metadata.topics.get(self._source.name)
        if topic is None:
            raise TransportSubscriptionError("Requested Kafka topic is unavailable")
        if topic.error is not None:
            raise self._normalize(topic.error)
        if not topic.partitions:
            raise TransportSubscriptionError("Requested Kafka topic has no partitions")
        for partition in topic.partitions.values():
            if partition.error is not None:
                raise self._normalize(partition.error)
        return tuple(sorted(topic.partitions))

    def _resolve(self) -> tuple[list[Any], dict[int, int]]:
        self._topology = self._metadata(self._remaining())
        requested = self._source.partitions
        self._partitions = tuple(sorted(requested)) if requested is not None else self._topology
        if not self._partitions or not set(self._partitions).issubset(self._topology):
            raise TransportSubscriptionError("Requested Kafka partitions are unavailable")
        mode = getattr(self._start.mode, "value", self._start.mode)
        if mode == "offset" and set(self._start.offsets) != set(self._partitions):
            raise StartPositionUnavailableError(
                "Explicit offsets must cover exactly the selected Kafka partitions"
            )
        watermarks: dict[int, tuple[int, int]] = {}
        for partition in self._partitions:
            result = self._consumer.get_watermark_offsets(
                self._native.TopicPartition(self._source.name, partition),
                timeout=self._remaining(),
                cached=False,
            )
            if result is None:
                raise TransportReadinessError("Could not resolve Kafka watermarks")
            watermarks[partition] = result
            self._highs[partition] = result[1]
        if self._config.isolation_level == "read_committed":
            # Consumer watermark queries honor isolation.level and return LSO.
            # Resolve the physical high explicitly without changing reader
            # isolation or consuming uncommitted records.
            self._highs = self._physical_highs()
            watermarks = {
                partition: (low, self._highs[partition]) for partition, (low, _) in watermarks.items()
            }
        timestamp_offsets = {}
        if mode == "timestamp":
            values = self._consumer.offsets_for_times(
                [
                    self._native.TopicPartition(self._source.name, partition, self._start.timestamp_ms)
                    for partition in self._partitions
                ],
                timeout=self._remaining(),
            )
            for value in values:
                if value.error is not None:
                    raise self._normalize(value.error)
                timestamp_offsets[value.partition] = value.offset
        positions = {}
        for partition, (low, high) in watermarks.items():
            if mode == "latest":
                position = high
            elif mode == "earliest":
                position = low
            elif mode == "offset":
                position = self._start.offsets[partition]
            elif mode == "timestamp":
                position = timestamp_offsets.get(partition, -1)
                if position < 0:
                    if self._start.require_existing:
                        raise StartPositionUnavailableError(
                            "No Kafka record exists at the requested timestamp in every partition"
                        )
                    position = high
            else:
                raise StartPositionUnavailableError("Unsupported Kafka start position")
            if position < low or position > high:
                raise StartPositionUnavailableError("Requested Kafka offset is outside the available range")
            positions[partition] = position
        return [
            self._native.TopicPartition(self._source.name, partition, position)
            for partition, position in positions.items()
        ], positions

    def _physical_highs(self) -> dict[int, int]:
        module = _load_admin()
        # AdminClient shares security/network settings; consumer-only options
        # must not leak into its configuration or produce ignored-option logs.
        common_prefixes = (
            "bootstrap.",
            "client.",
            "security.",
            "sasl.",
            "ssl.",
            "socket.",
            "broker.",
            "reconnect.",
            "connections.",
            "metadata.",
            "api.",
        )
        config = {
            name: value
            for name, value in self._native_config().items()
            if name.startswith(common_prefixes) or name in {"debug", "log_level"}
        }
        admin = module.AdminClient(config)
        self._admin = admin
        try:
            requests = {
                self._native.TopicPartition(self._source.name, partition): module.OffsetSpec.latest()
                for partition in self._partitions
            }
            futures = admin.list_offsets(
                requests,
                isolation_level=self._native.IsolationLevel.READ_UNCOMMITTED,
                request_timeout=self._remaining(),
            )
            highs = {}
            for partition, future in futures.items():
                value = future.result(timeout=self._remaining()).offset
                if value < 0:
                    raise TransportReadinessError("Could not resolve physical Kafka high watermarks")
                highs[partition.partition] = value
            if set(highs) != set(self._partitions):
                raise TransportReadinessError("Kafka high watermark resolution omitted selected partitions")
            return highs
        finally:
            # confluent-kafka AdminClient exposes no public close(). Releasing
            # the final reference explicitly here destroys its native handle
            # on this worker, after its bounded requests have completed.
            self._admin = None
            del admin

    def _message(self, message: Any) -> RawEvent | None:
        error = message.error()
        if error is not None:
            if error.code() == self._native.KafkaError._PARTITION_EOF:
                self._fetch_confirmed.add(message.partition())
                return None
            raise self._normalize(error)
        self._fetch_confirmed.add(message.partition())
        timestamp_kind, timestamp_ms = message.timestamp()
        raw = RawEvent(
            payload=message.value(),
            source=message.topic(),
            headers=tuple(message.headers() or ()),
            key=message.key(),
            metadata={
                "kafka.partition": message.partition(),
                "kafka.offset": message.offset(),
                "kafka.timestamp_ms": timestamp_ms,
                "kafka.timestamp_kind": timestamp_kind,
            },
        )
        # Include all user-controlled envelope components before handing off.
        size = len(raw.payload or b"") + len(raw.key or b"") + len(raw.source.encode("utf-8")) + 128
        size += sum(len(name.encode("utf-8")) + len(value or b"") + 32 for name, value in raw.headers)
        for name, value in raw.metadata.items():
            size += 32 + len(name.encode("utf-8"))
            size += len(value) if isinstance(value, bytes) else len(str(value).encode("utf-8"))
        if size > self._context.buffers.max_bytes:
            raise TransportBackpressureError("Kafka record exceeds the transport handoff byte budget")
        return raw

    def _run(self) -> None:
        try:
            self._consumer = self._native.Consumer(self._native_config())
            assignment, positions = self._resolve()
            captured = self._context.clock.monotonic()
            # Probe the Fetch path using the same reader at its captured high
            # watermarks, then restore the requested numeric positions. Records
            # published during this probe are read again from those positions;
            # no business record is dropped from the actual observation. This
            # avoids filling the handoff before Core can receive the session.
            probes = [
                self._native.TopicPartition(self._source.name, partition, self._highs[partition])
                for partition in self._partitions
            ]
            self._consumer.assign(probes)
            while not set(self._partitions).issubset(self._fetch_confirmed):
                self._remaining()
                message = self._consumer.poll(self._config.poll_timeout)
                if self._error is not None:
                    raise self._error
                if message is not None:
                    error = message.error()
                    if error is not None and error.code() != self._native.KafkaError._PARTITION_EOF:
                        raise self._normalize(error)
                    self._fetch_confirmed.add(message.partition())
            self._consumer.assign(assignment)
            self._readiness = TransportReadiness(
                boundary=ObservationBoundary(
                    source=self._source.name,
                    start_mode=getattr(self._start.mode, "value", self._start.mode),
                    positions=positions,
                    captured_monotonic=captured,
                ),
                resolved_capabilities=_capabilities(),
                source_metadata={
                    "kafka.partition_count": len(self._partitions),
                    "kafka.native_queue_bytes": self._config.native_queue_bytes,
                    "kafka.native_fetch_bytes": self._config.native_fetch_bytes,
                    "kafka.isolation_level": self._config.isolation_level,
                },
                ready_monotonic=self._context.clock.monotonic(),
            )
            self._wake_loop()
            next_topology = self._context.clock.monotonic() + self._config.topology_check_interval
            paused = False
            while not self._stop.is_set() and self._error is None:
                now = self._context.clock.monotonic()
                if now >= next_topology:
                    current = self._metadata(self._config.metadata_timeout)
                    if current != self._topology:
                        raise TopologyChangedError(
                            "Kafka topic partition topology changed during observation"
                        )
                    next_topology = self._context.clock.monotonic() + self._config.topology_check_interval
                with self._condition:
                    has_pending = bool(self._queue)
                if has_pending:
                    if not paused:
                        self._consumer.pause(assignment)
                        paused = True
                    with self._condition:
                        if self._queue and not self._stop.is_set():
                            self._condition.wait(
                                timeout=min(
                                    self._config.poll_timeout,
                                    max(0.001, next_topology - self._context.clock.monotonic()),
                                )
                            )
                    continue
                if paused:
                    self._consumer.resume(assignment)
                    paused = False
                message = self._consumer.poll(self._config.poll_timeout)
                if self._error is not None:
                    raise self._error
                if message is not None:
                    raw = self._message(message)
                    message = None  # Release the native message before any backpressure wait.
                    if raw is not None:
                        with self._condition:
                            self._queue.append(raw)
                        raw = None
                        self._wake_loop()
        except BaseException as exc:
            if not self._stop.is_set():
                if isinstance(exc, TransportError):
                    error = exc
                else:
                    native_error = exc.args[0] if exc.args else None
                    error = self._normalize(native_error)
                self._fail(error)
        finally:
            if self._consumer is not None:
                try:
                    self._consumer.close()
                except BaseException:
                    self._cleanup_error = ObserverCleanupError("Kafka native client cleanup failed")
            with self._condition:
                self._queue.clear()
            self._worker_finished = True
            self._wake_loop()


__all__ = ["KafkaAdapter", "KafkaConfig", "KafkaSession"]
