"""Independent Kafka readers with explicit boundaries and a bounded handoff.

Native imports are lazy. Every Consumer operation, including construction and
cleanup, runs on its session's worker thread. The Python bridge conservatively
holds one record at a time; native librdkafka queues have separate budgets.

:ivar _MANAGED_OPTIONS: Native settings reserved for observer ownership, safety,
    readiness callbacks, security, and queue budgets.
:ivar __all__: Public Kafka configuration, adapter, and session types.
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
from typing import TYPE_CHECKING, Any
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


if TYPE_CHECKING:
    from .sync_kafka import SyncKafkaAdapter


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
    """Immutable Kafka connection, security, polling, and native-buffer policy.

    :ivar bootstrap_servers: Bootstrap address string or immutable tuple of addresses.
    :ivar security_protocol: Explicit PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL mode.
    :ivar sasl_mechanism: SASL mechanism when SASL is enabled.
    :ivar sasl_username: Optional SASL username, excluded from repr.
    :ivar sasl_password: Optional SASL secret, excluded from repr.
    :ivar ssl_ca_location: CA certificate path used by TLS.
    :ivar ssl_certificate_location: Optional TLS client certificate path.
    :ivar ssl_key_location: TLS private-key path, excluded from repr.
    :ivar ssl_key_password: TLS private-key password, excluded from repr.
    :ivar isolation_level: Reader transaction isolation; defaults to read_committed.
    :ivar topology_check_interval: Positive seconds between partition-topology checks.
    :ivar poll_timeout: Positive maximum duration of a worker poll call.
    :ivar metadata_timeout: Per-request metadata budget, capped by startup remaining time.
    :ivar native_queue_bytes: Native queue budget, separate from Python handoff accounting.
    :ivar native_fetch_bytes: Native fetch budget no greater than the native queue budget.
    :ivar client_options: Detached immutable native scalar settings that cannot override managed options.
    """

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
        """Validate related security settings and freeze caller-supplied native options.

        :returns: None after the immutable policy is validated.
        :raises ConfigurationError: If addresses, security combinations, limits, or native options are
            invalid.
        """
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
    """Describe Kafka's independent partition readers and explicit replay policies.

    :returns: A fresh immutable capability value without creating native resources.
    """
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
    """Import the Kafka client only when opening an observation.

    :returns: The confluent_kafka module.
    :raises ImportError: If the optional Kafka client cannot be imported.
    """
    return importlib.import_module("confluent_kafka")


def _load_admin() -> Any:
    """Import the independent AdminClient API used for physical high watermarks.

    :returns: The confluent_kafka.admin module.
    :raises ImportError: If the installed Kafka client lacks its admin module.
    """
    return importlib.import_module("confluent_kafka.admin")


class KafkaAdapter:
    """Lazy factory for manually assigned Kafka reader sessions.

    :ivar transport_name: Canonical report title shared by both runtime adapters.
    :ivar config: Validated immutable Kafka configuration.
    :ivar capabilities: Supported partition isolation, replay, metadata, and flow-control semantics.
    """

    transport_name = "KafkaAdapter"

    def __init__(self, config: KafkaConfig) -> None:
        """Store an explicit policy without importing a client or starting a worker.

        :param config: Validated immutable connection and delivery settings.
        :returns: None; native resources are deferred until open.
        :raises ConfigurationError: If config is not a KafkaConfig.
        """
        if not isinstance(config, KafkaConfig):
            raise ConfigurationError("KafkaAdapter requires KafkaConfig")
        self.config = config

    @property
    def capabilities(self) -> TransportCapabilities:
        """Return static Kafka semantics without contacting the broker.

        :returns: Independent-reader capabilities with partition ordering and explicit replay.
        """
        return _capabilities()

    def sync_adapter(self) -> SyncKafkaAdapter:
        """Build the native blocking adapter without opening a connection.

        :returns: An independent blocking adapter sharing this immutable configuration.
        """
        from .sync_kafka import SyncKafkaAdapter

        return SyncKafkaAdapter(self.config)

    async def open(
        self, source: TransportSource, start: StartPosition, *, context: OpenContext
    ) -> KafkaSession:
        """Start one native worker and wait for its Fetch-confirmed READY boundary.

        :param source: Concrete existing source selected for this observation.
        :param start: Requested start policy and its transport-specific boundary.
        :param context: Runtime clock, startup deadline, observation identity, and buffer limits.
        :returns: A READY KafkaSession with its own Consumer and handoff.
        :raises MissingOptionalDependencyError: If the Kafka client is unavailable.
        :raises TransportError: If boundary resolution or Fetch readiness fails.
        :raises TimeoutError: If the startup budget is exhausted.
        :raises asyncio.CancelledError: If startup is cancelled; rollback still attempts worker cleanup.
        """
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
            # Startup has one owner. Cancelling this wait must cancel its
            # single-use signal before a competing late worker notification.
            # The independent session failure signal remains untouched.
            await asyncio.wait_for(session._ready, timeout=remaining)
            if context.clock.monotonic() >= context.startup_deadline:
                raise TimeoutError("Kafka startup deadline expired")
        except BaseException as primary:
            try:
                await session.close(deadline=context.clock.monotonic() + 2.0)
            except BaseException as cleanup:
                # Startup/cancellation remains the primary failure.
                setattr(primary, "cleanup_error", cleanup)
            raise
        return session


class _KafkaNativeWorker:
    """Shared native Kafka driver used by blocking and asynchronous session bridges.

    :ivar _config: Immutable connection, security, polling, and buffer settings.
    :ivar _native: Lazily imported Kafka client module or controlled native double.
    :ivar _source: Selected topic and optional explicit partition set.
    :ivar _start: Requested replay policy before numeric boundary resolution.
    :ivar _context: Owner clock, startup deadline, observation identity, and handoff limits.
    :ivar _condition: Thread condition protecting bridge queue and notification bookkeeping.
    :ivar _queue: Conservative one-record Python handoff deque.
    :ivar _stop: Thread-safe stop request observed by polling and backpressure waits.
    :ivar _error: First terminal transport error, retained without replacement.
    :ivar _cleanup_error: Native cleanup error with its original Python cause.
    :ivar _readiness: Resolved READY contract, or None before successful startup.
    :ivar _fetch_confirmed: Partitions for which the same reader confirmed a Fetch path.
    :ivar _partitions: Concrete selected partitions.
    :ivar _topology: Complete initial topic partition set used for static topology checks.
    :ivar _highs: Captured physical numeric high watermarks by partition.
    :ivar _consumer: Worker-owned native Consumer, never operated from either runtime bridge.
    :ivar _admin: Temporary worker-owned AdminClient during physical-high resolution.
    :ivar _thread: Dedicated worker serializing all native operations.
    :ivar _worker_finished: Completion marker published after final worker resource cleanup.
    :ivar readiness: Resolved boundary and capabilities after successful startup.
    """

    def __init__(
        self,
        config: KafkaConfig,
        native: Any,
        source: TransportSource,
        start: StartPosition,
        context: OpenContext,
    ) -> None:
        """Allocate an unstarted native worker and its bounded thread-safe handoff.

        :param config: Validated immutable connection and delivery settings.
        :param native: Lazily loaded native client module or an equivalent controlled test double.
        :param source: Concrete existing source selected for this observation.
        :param start: Requested start policy and its transport-specific boundary.
        :param context: Runtime clock, startup deadline, observation identity, and buffer limits.
        :returns: None; the chosen runtime bridge starts this worker after allocating its signals.
        """
        self._config = config
        self._native = native
        self._source = source
        self._start = start
        self._context = context
        self._condition = threading.Condition()
        self._queue: deque[RawEvent] = deque()
        self._stop = threading.Event()
        self._worker_finished = False
        self._error: TransportError | None = None
        self._cleanup_error: BaseException | None = None
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

    def _wake_owner(self) -> None:
        """Notify blocking bridge waiters after publishing worker state.

        :returns: None; asynchronous bridges override this hook to wake their owner loop.
        """
        with self._condition:
            self._condition.notify_all()

    @property
    def readiness(self) -> TransportReadiness:
        """Return Fetch-confirmed boundary and session metadata.

        :returns: Resolved readiness for the selected topic and partitions.
        :raises TransportReadinessError: If startup is not complete.
        """
        if self._readiness is None:
            raise TransportReadinessError("Kafka session is not ready")
        return self._readiness

    def _fail(self, error: TransportError) -> None:
        """Retain the first terminal worker error and wake all affected waiters.

        :param error: Failure to normalize or publish without copying its message into safe diagnostics.
        :returns: None; later errors do not replace the original failure.
        """
        with self._condition:
            if self._error is None:
                self._error = error
            self._condition.notify_all()
        self._wake_owner()

    def _remaining(self) -> float:
        """Bound a native startup request by remaining shared-clock time.

        :returns: Positive seconds capped by metadata_timeout.
        :raises TransportReadinessError: If startup expired or a stop request was observed.
        """
        remaining = self._context.startup_deadline - self._context.clock.monotonic()
        if remaining <= 0 or self._stop.is_set():
            raise TransportReadinessError("Kafka fetch readiness was not confirmed within the startup budget")
        return min(self._config.metadata_timeout, remaining)

    def _native_config(self) -> dict[str, Any]:
        """Build managed Consumer settings without mutating caller configuration.

        :returns: Native settings with auto commit/store disabled and observer-owned callbacks.
        """
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
        """Translate a native numeric error code into a safe stable error type.

        :param native_error: Native client error whose numeric code determines the public error type.
        :returns: A typed transport error whose summary omits native error text.
        """
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
        """Record an asynchronous native client error on the worker path.

        :param error: Native Kafka error delivered by librdkafka's error callback.
        :returns: None after publishing the normalized failure.
        """
        self._fail(self._normalize(error))

    def _statistics_callback(self, payload: str) -> None:
        # hi_offset/ls_offset are updated by Fetch responses. Unlike a metadata
        # request, a successful Fetch confirms the Read path, including a reader
        # waiting beyond the LSO of an existing open transaction.
        """Accept successful Fetch statistics as readiness evidence for selected partitions.

        :param payload: librdkafka statistics JSON; native values are not copied into safe summaries.
        :returns: None; malformed optional statistics are ignored.
        """
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

    def _metadata(self, client: Any, timeout: float) -> tuple[int, ...]:
        """Resolve and validate the complete topic partition set.

        :param client: Native Consumer or AdminClient whose metadata is being resolved.
        :param timeout: Finite operation budget in seconds.
        :returns: Sorted partition numbers reported by valid native metadata.
        :raises TransportSubscriptionError: If the topic or its partition metadata is unavailable.
        :raises TransportError: If native topic or partition metadata reports a client error.
        """
        metadata = client.list_topics(topic=self._source.name, timeout=timeout)
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
        """Resolve an explicit numeric start boundary for every selected partition.

        :returns: Native assignments and their partition-to-offset mapping.
        :raises StartPositionUnavailableError: If offsets or required timestamp positions are unavailable.
        :raises TransportSubscriptionError: If requested partitions do not belong to the topic.
        :raises TransportError: If native metadata or boundary resolution fails.
        """
        self._topology = self._metadata(self._consumer, self._remaining())
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
        """Resolve physical highs independently of the consuming reader's isolation.

        :returns: Physical numeric high watermarks from READ_UNCOMMITTED ListOffsets.
        :raises TransportReadinessError: If highs are invalid or selected partitions are omitted.
        :raises Exception: If the bounded native admin request fails; the worker normalizes it.
        """
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
        config["allow.auto.create.topics"] = False
        admin = module.AdminClient(config)
        self._admin = admin
        try:
            # The AdminClient has its own metadata cache. Resolve the leader
            # route before ListOffsets, whose internal refresh can outlast our
            # per-request timeout on a fresh client.
            partitions = self._metadata(admin, self._remaining())
            if not set(self._partitions).issubset(partitions):
                raise TransportSubscriptionError("Requested Kafka partitions are unavailable")
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
        """Detach one native record and enforce the complete envelope byte budget.

        :param message: Native delivery to detach and validate before handing it to Core.
        :returns: A normalized event, or None for partition-EOF readiness signals.
        :raises TransportError: If the delivery contains a native error.
        :raises TransportBackpressureError: If the detached envelope exceeds the handoff byte budget.
        """
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
        """Own native startup, Fetch probing, polling, topology checks, and final cleanup.

        :returns: None after publishing worker completion; both runtime bridges retain original native
            failure causes.
        """
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
            self._wake_owner()
            next_topology = self._context.clock.monotonic() + self._config.topology_check_interval
            paused = False
            while not self._stop.is_set() and self._error is None:
                now = self._context.clock.monotonic()
                if now >= next_topology:
                    current = self._metadata(self._consumer, self._config.metadata_timeout)
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
                        self._wake_owner()
        except BaseException as exc:
            if not self._stop.is_set():
                if isinstance(exc, TransportError):
                    error = exc
                else:
                    native_error = exc.args[0] if exc.args else None
                    error = self._normalize(native_error)
                    error.cause = exc
                    error.__cause__ = exc
                self._fail(error)
        finally:
            if self._consumer is not None:
                try:
                    self._consumer.close()
                except BaseException as exc:
                    error = ObserverCleanupError("Kafka native client cleanup failed", cause=exc)
                    error.__cause__ = exc
                    self._cleanup_error = error
            with self._condition:
                self._queue.clear()
            self._worker_finished = True
            self._wake_owner()


class KafkaSession(_KafkaNativeWorker):
    """One serialized Kafka worker, bounded bridge, and independent failure signal.

    :ivar _config: Immutable connection, security, polling, and buffer settings.
    :ivar _native: Lazily imported Kafka client module or controlled native double.
    :ivar _source: Selected topic and optional explicit partition set.
    :ivar _start: Requested replay policy before numeric boundary resolution.
    :ivar _context: Owner clock, startup deadline, observation identity, and handoff limits.
    :ivar _loop: Async owner loop receiving worker notifications.
    :ivar _ready: Owner-loop Future completed after readiness or startup failure.
    :ivar _failure: Shared terminal-error signal, shielded from individual waiter cancellation.
    :ivar _finished: Owner-loop Future completed when the worker finishes.
    :ivar _data_ready: Owner-loop event used to wake the receive iterator.
    :ivar _condition: Thread condition protecting bridge queue and notification bookkeeping.
    :ivar _queue: Conservative one-record Python handoff deque.
    :ivar _stop: Thread-safe stop request observed by polling and backpressure waits.
    :ivar _wake_pending: Whether a worker-to-loop wake callback is already reserved.
    :ivar _error: First terminal transport error, retained without replacement.
    :ivar _cleanup_error: Native cleanup error with its original Python cause.
    :ivar _receive_started: Whether the single allowed receive iterator was requested.
    :ivar _readiness: Resolved READY contract, or None before successful startup.
    :ivar _fetch_confirmed: Partitions for which the same reader confirmed a Fetch path.
    :ivar _partitions: Concrete selected partitions.
    :ivar _topology: Complete initial topic partition set used for static topology checks.
    :ivar _highs: Captured physical numeric high watermarks by partition.
    :ivar _consumer: Worker-owned native Consumer, never operated from the owner loop.
    :ivar _admin: Temporary worker-owned AdminClient during physical-high resolution.
    :ivar _thread: Dedicated worker serializing all native operations.
    :ivar _worker_finished: Completion marker published after final worker resource cleanup.
    :ivar readiness: Resolved boundary and capabilities after successful startup.
    """

    def __init__(
        self,
        config: KafkaConfig,
        native: Any,
        source: TransportSource,
        start: StartPosition,
        context: OpenContext,
    ) -> None:
        """Allocate the shared native driver and its asynchronous owner signals.

        :param config: Immutable connection, polling, and native buffer policy.
        :param native: Lazily loaded native Kafka module or controlled double.
        :param source: Existing topic and optional explicit partitions.
        :param start: Requested native replay policy.
        :param context: Runtime clock, startup deadline, identity, and handoff limits.
        :returns: None; the adapter starts the allocated native worker.
        """
        super().__init__(config, native, source, start, context)
        self._loop = asyncio.get_running_loop()
        self._ready: asyncio.Future[None] = self._loop.create_future()
        self._failure: asyncio.Future[TransportError | None] = self._loop.create_future()
        self._finished: asyncio.Future[None] = self._loop.create_future()
        self._data_ready = asyncio.Event()
        self._wake_pending = False
        self._receive_started = False

    async def wait_failure(self) -> TransportError | None:
        """Await worker failure without cancelling the shared signal.

        :returns: The first terminal transport error, or None after intentional worker closure.
        :raises asyncio.CancelledError: If only the caller's monitoring wait is cancelled.
        """
        return await asyncio.shield(self._failure)

    def receive(self) -> AsyncIterator[RawEvent]:
        """Reserve the only receive iterator for this independent reader.

        :returns: An asynchronous iterator of normalized RawEvent values.
        :raises TransportSubscriptionError: If receive was already requested.
        """
        if self._receive_started:
            raise TransportSubscriptionError("A Kafka session permits one receive iterator")
        self._receive_started = True
        return self._receive()

    async def _receive(self) -> AsyncIterator[RawEvent]:
        """Drain the bridge while coordinating worker capacity notifications.

        :yields: Detached RawEvent deliveries in observed partition-read order.
        :raises TransportError: If the worker reports a terminal error.
        """
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
        """Request stop and confirm both native cleanup and thread termination.

        :param deadline: Absolute cleanup deadline in the runtime monotonic clock domain.
        :returns: None once the worker has terminated.
        :raises ObserverCleanupTimeoutError: If cleanup or thread join exceeds deadline.
        :raises ObserverCleanupError: If native Consumer.close failed; its cause remains inspectable.
        """
        self._stop.set()
        with self._condition:
            self._condition.notify_all()
        self._wake_owner()
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

    def _wake_owner(self) -> None:
        """Reserve at most one thread-safe wake callback for the owner loop.

        :returns: None; a closed owner loop is left for outer cleanup accounting.
        """
        with self._condition:
            if self._wake_pending:
                return
            self._wake_pending = True
        try:
            self._loop.call_soon_threadsafe(self._notify)
        except RuntimeError:
            pass

    def _notify(self) -> None:
        """Publish queued data, readiness, failure, or completion to owner-loop waiters.

        :returns: None after coalesced notifications are delivered.
        """
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


__all__ = ["KafkaAdapter", "KafkaConfig", "KafkaSession"]
