"""Independent RabbitMQ publisher/admin harness for explicitly owned exchanges.

Each operation opens and closes its own connection, so a synchronous test can
call create_source(), publish(), and cleanup() through separate asyncio.run()
invocations without sharing loop-bound clients.
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
    try:
        return importlib.import_module("aio_pika"), importlib.import_module("aiormq")
    except ImportError as exc:
        raise MissingOptionalDependencyError("RabbitMQ harness requires the 'rabbitmq' extra") from exc


class RabbitMQAdapterHarness:
    def __init__(self, url: str, *, timeout: float = 15.0) -> None:
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
        if self._closed:
            raise ConfigurationError("RabbitMQ harness is already cleaned up")

    @asynccontextmanager
    async def _connection(self) -> AsyncGenerator[tuple[Any, Any, Any], None]:
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
