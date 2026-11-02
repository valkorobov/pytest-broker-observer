"""Independent RabbitMQ publisher/admin harness for owned topic exchanges.

Each operation creates and closes its own native connection, supporting
separate asyncio.run calls without sharing clients tied to closed loops.

:ivar __all__: Public RabbitMQ publisher/admin harness type.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import importlib
import math
import re
from typing import Any, AsyncGenerator
from uuid import uuid4

from ..errors import (
    ConfigurationError,
    MissingOptionalDependencyError,
    TransportError,
    UnsupportedCapabilityError,
)
from ..models import RawEvent, TransportSource
from ..transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig
from .harness import PublishReceipt


def _load_client() -> tuple[Any, Any]:
    """Load the optional publisher APIs without importing them during construction.

    :returns: The aio-pika and aiormq modules needed for broker-confirmation checks.
    :raises MissingOptionalDependencyError: If the RabbitMQ extra is unavailable.
    """
    try:
        return importlib.import_module("aio_pika"), importlib.import_module("aiormq")
    except ImportError as exc:
        raise MissingOptionalDependencyError("RabbitMQ harness requires the 'rabbitmq' extra") from exc


class RabbitMQAdapterHarness:
    """Create owned exchanges and publish through independent confirmed connections.

    :ivar adapter: Lazy dedicated-subscription adapter used by contracts.
    :ivar _url: Explicit native connection URL, retained only for fixture operations.
    :ivar _timeout: Positive finite connection/operation/cleanup budget.
    :ivar _exchanges: Names owned by the fixture, registered before declaration can succeed.
    :ivar _closed: Whether cleanup has forbidden new creation/publication.
    """

    def __init__(self, url: str, *, timeout: float = 15.0) -> None:
        """Validate fixture budgets and store explicit connection settings.

        :param url: Explicit broker connection URL; never discovered from environment variables.
        :param timeout: Finite operation budget in seconds.
        :returns: None; native clients remain unimported until an operation.
        :raises ConfigurationError: If timeout or connection configuration is invalid.
        """
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ConfigurationError("Harness timeout must be positive and finite")
        self.adapter = RabbitMQAdapter(RabbitMQConfig(url=url))
        self._url = url
        self._timeout = timeout
        self._exchanges: set[str] = set()
        self._closed = False

    def _ensure_open(self) -> None:
        """Reject new fixture operations after cleanup has started.

        :returns: None while source creation/publication remains allowed.
        :raises ConfigurationError: If the harness has been cleaned up.
        """
        if self._closed:
            raise ConfigurationError("RabbitMQ harness is already cleaned up")

    @asynccontextmanager
    async def _connection(self) -> AsyncGenerator[tuple[Any, Any, Any], None]:
        """Own one fresh native connection for a bounded publisher/admin operation.

        :yields: Native aio-pika module, aiormq module, and a fresh connected client.
        :raises TransportError: If cleanup fails without another primary exception.
        :raises TimeoutError: If the operation exceeds the fixture timeout.
        """
        native, amqp = _load_client()
        connection = None
        primary: BaseException | None = None
        try:
            async with asyncio.timeout(self._timeout):
                connection = await native.connect(self._url, timeout=self._timeout)
                yield native, amqp, connection
        except BaseException as exc:
            primary = exc
            raise
        finally:
            if connection is not None:
                try:
                    await asyncio.wait_for(connection.close(), timeout=self._timeout)
                except Exception as exc:
                    if primary is None:
                        raise TransportError("RabbitMQ harness connection cleanup failed") from exc
                    primary.add_note("RabbitMQ harness connection cleanup also failed")

    async def create_source(self, *, case_id: str) -> TransportSource:
        """Create and register a unique non-durable topic exchange.

        :param case_id: Nonempty test label used in a unique owned source name.
        :returns: A source naming the independently created exchange.
        :raises ConfigurationError: If the case label is invalid or cleanup already ran.
        :raises TransportError: If the exchange cannot be declared.
        """
        self._ensure_open()
        if not isinstance(case_id, str) or not case_id:
            raise ConfigurationError("Harness case_id must be a nonempty string")
        label = re.sub(r"[^A-Za-z0-9._-]+", "-", case_id)[:100]
        name = f"observer-contract-{label}-{uuid4().hex}"
        # Record ownership before declaration: a timeout may still create it.
        self._exchanges.add(name)
        try:
            async with self._connection() as (native, _, connection):
                channel = await connection.channel()
                await channel.declare_exchange(
                    name,
                    type=native.ExchangeType.TOPIC,
                    durable=False,
                    auto_delete=False,
                    timeout=self._timeout,
                )
        except (ConfigurationError, TransportError):
            raise
        except Exception as exc:
            raise TransportError("RabbitMQ harness could not create its test source") from exc
        return TransportSource(name)

    async def publish(
        self,
        source: TransportSource,
        payload: bytes | None,
        *,
        key: bytes | None = None,
        headers: tuple[tuple[str, bytes | None], ...] = (),
        routing_key: str = "events",
    ) -> PublishReceipt:
        """Publish one supported body/header set and require broker Basic.Ack.

        :param source: Concrete existing source selected for this observation.
        :param payload: Raw message body; null is accepted only when the adapter advertises it.
        :param key: Optional message key, rejected by transports that do not support one.
        :param headers: Ordered normalized header pairs, subject to the transport capabilities.
        :param routing_key: UTF-8 routing key used when publishing to the owned topic exchange.
        :returns: A confirmed receipt containing the routing key.
        :raises ConfigurationError: If the exchange is unowned or native names/keys are invalid.
        :raises UnsupportedCapabilityError: If null bodies, separate keys, or duplicate headers are
            supplied.
        :raises TransportError: If the broker does not confirm publication.
        """
        self._ensure_open()
        if source.name not in self._exchanges:
            raise ConfigurationError("RabbitMQ harness can publish only to its own test sources")
        if payload is None:
            raise UnsupportedCapabilityError("RabbitMQ messages cannot represent null payloads")
        if key is not None:
            raise UnsupportedCapabilityError("RabbitMQ messages do not expose a separate message key")
        raw = RawEvent(payload, source.name, headers=headers)
        if len({name for name, _ in raw.headers}) != len(raw.headers):
            raise UnsupportedCapabilityError("RabbitMQ field tables cannot preserve duplicate header names")
        wire_headers: dict[str, bytearray | None] = {}
        for name, value in raw.headers:
            try:
                encoded_name = name.encode("utf-8")
            except UnicodeError as exc:
                raise ConfigurationError("RabbitMQ header names must be valid UTF-8 strings") from exc
            # pamqp truncates field names beyond 128 characters and encodes
            # them as short strings, whose encoded length cannot exceed 255.
            if len(name) > 128 or len(encoded_name) > 255:
                raise ConfigurationError("RabbitMQ header names must fit the native field-name limits")
            # pamqp field tables support binary values as bytearray, not bytes.
            # Preserve arbitrary binary data without forcing text decoding.
            wire_headers[name] = bytearray(value) if value is not None else None
        try:
            valid_routing_key = isinstance(routing_key, str) and len(routing_key.encode("utf-8")) <= 255
        except UnicodeError:
            valid_routing_key = False
        if not valid_routing_key:
            raise ConfigurationError("RabbitMQ routing_key must fit a valid UTF-8 short string")
        try:
            async with self._connection() as (native, amqp, connection):
                channel = await connection.channel(publisher_confirms=True)
                exchange = await channel.get_exchange(source.name, ensure=False)
                confirmation = await exchange.publish(
                    native.Message(body=raw.payload, headers=wire_headers),
                    routing_key=routing_key,
                    mandatory=False,
                    timeout=self._timeout,
                )
                if not isinstance(confirmation, amqp.spec.Basic.Ack):
                    raise TransportError("RabbitMQ harness publication was not confirmed by the broker")
        except (ConfigurationError, TransportError):
            raise
        except Exception as exc:
            raise TransportError("RabbitMQ harness could not publish its test record") from exc
        return PublishReceipt(metadata={"rabbitmq.routing_key": routing_key})

    async def cleanup(self) -> None:
        """Delete only registered exchanges using fresh native channels.

        :returns: None after all owned exchange names are removed.
        :raises TransportError: If connection cleanup or an owned exchange deletion fails.
        """
        self._closed = True
        if not self._exchanges:
            return
        failures: list[Exception] = []
        try:
            async with self._connection() as (_, amqp, connection):
                for name in tuple(sorted(self._exchanges)):
                    channel = await connection.channel()
                    try:
                        exchange = await channel.get_exchange(name, ensure=False)
                        await exchange.delete(if_unused=False, timeout=self._timeout)
                        self._exchanges.discard(name)
                    except amqp.exceptions.ChannelNotFoundEntity:
                        self._exchanges.discard(name)
                    except Exception as exc:
                        failures.append(exc)
        except (MissingOptionalDependencyError, TransportError):
            raise
        except Exception as exc:
            raise TransportError("RabbitMQ harness cleanup could not remove its owned exchanges") from exc
        if failures:
            raise TransportError(
                "RabbitMQ harness cleanup could not remove all owned exchanges"
            ) from failures[0]


__all__ = ["RabbitMQAdapterHarness"]
