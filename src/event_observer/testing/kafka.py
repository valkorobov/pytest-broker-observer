"""Real Kafka contract harness with independent publisher and admin paths.

Construction imports no native client and opens no connection. Only unique
topics recorded as owned by this harness are eligible for publication/deletion.

:ivar __all__: Public Kafka publisher/admin harness type.
"""

from __future__ import annotations

import asyncio
import importlib
import math
import re
from typing import Any
from uuid import uuid4

from ..errors import ConfigurationError, MissingOptionalDependencyError, TransportError
from ..models import TransportSource
from ..transports.kafka import KafkaAdapter, KafkaConfig
from .harness import PublishReceipt


class KafkaAdapterHarness:
    """Own unique test topics and independently confirmed Kafka publications.

    :ivar adapter: Lazy Kafka adapter used by the contracts.
    :ivar _servers: Normalized bootstrap address string for native test clients.
    :ivar _partitions: Partition count used for every created test topic.
    :ivar _timeout: Finite admin, producer, and cleanup operation budget.
    :ivar _topics: Names owned by this harness, registered before creation can succeed.
    :ivar _native: Lazily imported confluent_kafka module.
    :ivar _admin_module: Lazily imported confluent_kafka.admin module.
    :ivar _admin: Independent native admin client used for owned-topic operations.
    :ivar _producer: Independent native producer, created on first publication.
    :ivar _closed: Whether cleanup has forbidden new source creation/publication.
    """

    def __init__(
        self,
        bootstrap_servers: str | tuple[str, ...],
        *,
        partitions: int = 1,
        timeout: float = 15.0,
    ) -> None:
        """Validate fixture budgets and create a lazy adapter without native resources.

        :param bootstrap_servers: Explicit Kafka bootstrap address string or tuple of addresses.
        :param partitions: Positive number of partitions in each owned test topic.
        :param timeout: Finite operation budget in seconds.
        :returns: None; producer/admin clients are created only on demand.
        :raises ConfigurationError: If partitions, timeout, or bootstrap configuration are invalid.
        """
        if isinstance(partitions, bool) or not isinstance(partitions, int) or partitions <= 0:
            raise ConfigurationError("Harness partitions must be a positive integer")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ConfigurationError("Harness timeout must be positive and finite")
        config = KafkaConfig(bootstrap_servers=bootstrap_servers)
        self.adapter = KafkaAdapter(config)
        self._servers = (
            bootstrap_servers if isinstance(bootstrap_servers, str) else ",".join(bootstrap_servers)
        )
        self._partitions = partitions
        self._timeout = timeout
        self._topics: set[str] = set()
        self._native: Any = None
        self._admin_module: Any = None
        self._admin: Any = None
        self._producer: Any = None
        self._closed = False

    def _clients(self) -> None:
        """Lazily import native APIs and initialize the independent admin client.

        :returns: None once required admin APIs are available.
        :raises MissingOptionalDependencyError: If the Kafka optional dependency is unavailable.
        """
        if self._native is None:
            try:
                self._native = importlib.import_module("confluent_kafka")
                self._admin_module = importlib.import_module("confluent_kafka.admin")
            except ImportError as exc:
                raise MissingOptionalDependencyError("Kafka harness requires the 'kafka' extra") from exc
        if self._admin is None:
            self._admin = self._admin_module.AdminClient({"bootstrap.servers": self._servers})

    def _ensure_open(self) -> None:
        """Reject publication or source creation after harness cleanup.

        :returns: None while the fixture is still open.
        :raises ConfigurationError: If cleanup has already closed the harness.
        """
        if self._closed:
            raise ConfigurationError("Kafka harness is already cleaned up")

    async def create_source(self, *, case_id: str) -> TransportSource:
        """Register and create a unique topic through the independent admin path.

        :param case_id: Nonempty test label used in a unique owned source name.
        :returns: A source naming the successfully created owned topic.
        :raises ConfigurationError: If case_id is invalid or cleanup already ran.
        :raises TransportError: If the broker does not confirm topic creation.
        """
        self._ensure_open()
        if not isinstance(case_id, str) or not case_id:
            raise ConfigurationError("Harness case_id must be a nonempty string")
        label = re.sub(r"[^A-Za-z0-9._-]+", "-", case_id)[:100]
        topic = f"observer-contract-{label}-{uuid4().hex}"
        # Register before the request: timeout can leave a partially created topic.
        self._topics.add(topic)
        await asyncio.to_thread(self._create_topic, topic)
        return TransportSource(topic)

    def _create_topic(self, topic: str) -> None:
        """Create one registered test topic using bounded native admin requests.

        :param topic: Unique topic name already registered as owned by this harness.
        :returns: None after the broker confirms creation.
        :raises TransportError: If topic creation fails or times out.
        """
        self._clients()
        try:
            futures = self._admin.create_topics(
                [self._admin_module.NewTopic(topic, num_partitions=self._partitions, replication_factor=1)],
                operation_timeout=self._timeout,
                request_timeout=self._timeout,
            )
            futures[topic].result(timeout=self._timeout)
        except Exception as exc:
            raise TransportError("Kafka harness could not create its test source") from exc

    async def publish(
        self,
        source: TransportSource,
        payload: bytes | None,
        *,
        key: bytes | None = None,
        headers: tuple[tuple[str, bytes | None], ...] = (),
    ) -> PublishReceipt:
        """Publish to an owned topic using a separate producer execution path.

        :param source: Concrete existing source selected for this observation.
        :param payload: Raw message body; null is accepted only when the adapter advertises it.
        :param key: Optional message key, rejected by transports that do not support one.
        :param headers: Ordered normalized header pairs, subject to the transport capabilities.
        :returns: A confirmed receipt with the broker partition and offset.
        :raises ConfigurationError: If the topic is not owned or the fixture is closed.
        :raises TransportError: If publication is not confirmed.
        """
        self._ensure_open()
        if source.name not in self._topics:
            raise ConfigurationError("Kafka harness can publish only to its own test sources")
        return await asyncio.to_thread(self._publish, source, payload, key, headers)

    def _publish(self, source: TransportSource, payload, key, headers) -> PublishReceipt:
        """Produce one partition-zero record and wait for its delivery callback.

        :param source: Concrete existing source selected for this observation.
        :param payload: Raw message body; null is accepted only when the adapter advertises it.
        :param key: Optional message key, rejected by transports that do not support one.
        :param headers: Ordered normalized header pairs, subject to the transport capabilities.
        :returns: A receipt containing the confirmed native partition and offset.
        :raises TransportError: If flushing, delivery, or native producer operations fail.
        """
        self._clients()
        try:
            if self._producer is None:
                self._producer = self._native.Producer(
                    {
                        "bootstrap.servers": self._servers,
                        "message.timeout.ms": max(1, int(self._timeout * 1000)),
                    }
                )
            delivery: list[tuple[Any, Any]] = []
            self._producer.produce(
                source.name,
                value=payload,
                key=key,
                headers=list(headers),
                partition=0,
                on_delivery=lambda error, message: delivery.append((error, message)),
            )
            pending = self._producer.flush(self._timeout)
            if pending or len(delivery) != 1 or delivery[0][0] is not None:
                raise TransportError("Kafka harness publication was not confirmed by the broker")
            message = delivery[0][1]
            return PublishReceipt(
                metadata={"kafka.partition": message.partition(), "kafka.offset": message.offset()}
            )
        except TransportError:
            raise
        except Exception as exc:
            raise TransportError("Kafka harness could not publish its test record") from exc

    async def cleanup(self) -> None:
        """Run blocking producer/topic cleanup outside the caller's event loop.

        :returns: None after owned resources are drained and deleted.
        :raises TransportError: If any owned resource cannot be cleaned up.
        """
        await asyncio.to_thread(self._cleanup)

    def _cleanup(self) -> None:
        """Drain the independent producer and delete registered topics only.

        :returns: None after releasing clients and closing the harness.
        :raises TransportError: If draining or deletion leaves an owned resource unresolved.
        """
        failures: list[Exception] = []
        try:
            if self._producer is not None:
                try:
                    if self._producer.flush(self._timeout):
                        failures.append(
                            TransportError("Kafka harness producer cleanup did not drain pending records")
                        )
                except Exception as exc:
                    failures.append(exc)
            if self._topics:
                self._clients()
                topics = tuple(self._topics)
                try:
                    futures = self._admin.delete_topics(
                        list(topics),
                        operation_timeout=self._timeout,
                        request_timeout=self._timeout,
                    )
                    for topic, future in futures.items():
                        try:
                            future.result(timeout=self._timeout)
                            self._topics.discard(topic)
                        except Exception as exc:
                            native_error = exc.args[0] if exc.args else None
                            code_getter = getattr(native_error, "code", None)
                            code = code_getter() if callable(code_getter) else None
                            if code == self._native.KafkaError.UNKNOWN_TOPIC_OR_PART:
                                self._topics.discard(topic)
                            else:
                                failures.append(exc)
                except Exception as exc:
                    failures.append(exc)
        finally:
            self._producer = None
            self._admin = None
            self._closed = True
        if failures:
            raise TransportError("Kafka harness cleanup could not remove all owned resources") from failures[
                0
            ]


__all__ = ["KafkaAdapterHarness"]
