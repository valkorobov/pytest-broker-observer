"""Independent Redis Streams test publisher/admin with per-operation clients.

The harness owns only its generated stream keys. It never creates or joins a
consumer group. XADD receipts report the returned entry ID, without promising
replication, persistence, or exactly-once publication.
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
from ..transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig
from .harness import PublishReceipt


_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")


def _load_client() -> tuple[Any, Any]:
    try:
        native = importlib.import_module("redis.asyncio")
        retry = importlib.import_module("redis.asyncio.retry").Retry
        backoff = importlib.import_module("redis.backoff").NoBackoff
    except ImportError as exc:
        raise MissingOptionalDependencyError("Redis Streams harness requires the 'redis' extra") from exc
    return native, retry(backoff(), 0)


def _stream_id(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeError as exc:
            raise TransportError("Redis XADD returned an invalid stream ID") from exc
    if not isinstance(value, str) or len(value) > 41:
        raise TransportError("Redis XADD did not return a stream ID")
    match = _ID.fullmatch(value)
    if match is None or any(int(part) > 2**64 - 1 for part in match.groups()) or value == "0-0":
        raise TransportError("Redis XADD returned an invalid stream ID")
    return value


class RedisStreamsAdapterHarness:
    def __init__(self, url: str, *, timeout: float = 15.0) -> None:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ConfigurationError("Harness timeout must be positive and finite")
        self.adapter = RedisStreamsAdapter(RedisStreamsConfig(url=url))
        self._url = url
        self._timeout = timeout
        self._streams: set[str] = set()
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise ConfigurationError("Redis Streams harness is already cleaned up")

    @asynccontextmanager
    async def _client(self) -> AsyncGenerator[Any, None]:
        native, retry = _load_client()
        client = None
        primary: BaseException | None = None
        try:
            async with asyncio.timeout(self._timeout):
                client = native.from_url(
                    self._url,
                    decode_responses=False,
                    protocol=2,
                    retry=retry,
                    socket_timeout=self._timeout,
                    socket_connect_timeout=self._timeout,
                )
                yield client
        except BaseException as exc:
            primary = exc
            raise
        finally:
            if client is not None:
                try:
                    await asyncio.wait_for(client.aclose(), timeout=self._timeout)
                except Exception as exc:
                    if primary is None:
                        raise TransportError("Redis Streams harness client cleanup failed") from exc
                    primary.add_note("Redis Streams harness client cleanup also failed")

    async def create_source(self, *, case_id: str) -> TransportSource:
        self._ensure_open()
        if not isinstance(case_id, str) or not case_id:
            raise ConfigurationError("Harness case_id must be a nonempty string")
        label = re.sub(r"[^A-Za-z0-9._-]+", "-", case_id)[:100]
        name = f"observer-stream-{label}-{uuid4().hex}"
        self._streams.add(name)
        try:
            async with self._client() as client:
                seed = _stream_id(await client.xadd(name, {b"payload": b""}))
                removed = await client.xdel(name, seed)
                if isinstance(removed, bool) or removed != 1:
                    raise TransportError("Redis Streams harness could not initialize an empty typed stream")
        except (ConfigurationError, TransportError):
            raise
        except Exception as exc:
            raise TransportError("Redis Streams harness could not create its test source") from exc
        return TransportSource(name)

    async def publish(
        self,
        source: TransportSource,
        payload: bytes | None,
        *,
        key: bytes | None = None,
        headers: tuple[tuple[str, bytes | None], ...] = (),
    ) -> PublishReceipt:
        self._ensure_open()
        if source.name not in self._streams:
            raise ConfigurationError("Redis Streams harness can publish only to its own test sources")
        if payload is None:
            raise UnsupportedCapabilityError("Redis Streams entries cannot represent null payloads")
        if key is not None:
            raise UnsupportedCapabilityError("Redis Streams entries do not expose a separate message key")
        raw = RawEvent(payload, source.name, headers=headers)
        if len({name for name, _ in raw.headers}) != len(raw.headers):
            raise UnsupportedCapabilityError(
                "Redis Streams field maps cannot preserve duplicate header names"
            )
        fields = {b"payload": raw.payload}
        for name, value in raw.headers:
            if value is None:
                raise UnsupportedCapabilityError("Redis Streams headers cannot represent null values")
            try:
                encoded_name = name.encode("utf-8")
            except UnicodeError as exc:
                raise ConfigurationError("Redis Streams header names must be valid UTF-8") from exc
            fields[b"header:" + encoded_name] = value
        try:
            async with self._client() as client:
                # Avoid recreating an owned stream that disappeared after startup.
                stream_id = _stream_id(await client.xadd(source.name, fields, nomkstream=True))
        except (ConfigurationError, TransportError):
            raise
        except Exception as exc:
            raise TransportError("Redis Streams harness could not append its test entry") from exc
        return PublishReceipt(metadata={"redis.stream_id": stream_id})

    async def cleanup(self) -> None:
        self._closed = True
        if not self._streams:
            return
        failures: list[Exception] = []
        try:
            async with self._client() as client:
                for name in tuple(sorted(self._streams)):
                    try:
                        await client.delete(name)
                        self._streams.discard(name)
                    except Exception as exc:
                        failures.append(exc)
        except (ConfigurationError, TransportError):
            raise
        except Exception as exc:
            raise TransportError("Redis Streams harness cleanup could not remove its owned streams") from exc
        if failures:
            raise TransportError(
                "Redis Streams harness cleanup could not remove all owned streams"
            ) from failures[0]


__all__ = ["RedisStreamsAdapterHarness"]
