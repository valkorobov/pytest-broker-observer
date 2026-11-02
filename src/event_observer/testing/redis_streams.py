"""Independent Redis Streams publisher/admin harness with per-operation clients.

Only generated keys are owned. XADD receipts report command acceptance and
the returned entry ID, without claiming durability or exactly-once delivery.
The harness never creates or joins consumer groups.

:ivar _ID: Canonical full-ID parser for successful XADD receipts, excluding 0-0.
:ivar __all__: Public Redis Streams publisher/admin harness type.
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
    """Load optional asyncio Redis APIs and a zero-attempt retry policy.

    :returns: The native client module and an independent zero-retry policy.
    :raises MissingOptionalDependencyError: If the Redis extra is unavailable.
    """
    try:
        native = importlib.import_module("redis.asyncio")
        retry = importlib.import_module("redis.asyncio.retry").Retry
        backoff = importlib.import_module("redis.backoff").NoBackoff
    except ImportError as exc:
        raise MissingOptionalDependencyError("Redis Streams harness requires the 'redis' extra") from exc
    return native, retry(backoff(), 0)


def _stream_id(value: Any) -> str:
    """Validate that XADD returned a canonical nonzero full native stream ID.

    :param value: Native value to validate before exposing it as a stream position.
    :returns: The ASCII stream-ID string for the broker-confirmed append.
    :raises TransportError: If the receipt is malformed, noncanonical, zero, or out of uint64 range.
    """
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
    """Own unique typed test streams and independently confirmed XADD publications.

    :ivar adapter: Lazy independent-reader adapter used by contracts.
    :ivar _url: Explicit native Redis URL used by per-operation clients.
    :ivar _timeout: Positive finite connection, operation, and cleanup budget.
    :ivar _streams: Fixture-owned stream names recorded before initialization can succeed.
    :ivar _closed: Whether cleanup has forbidden new creation/publication.
    """

    def __init__(self, url: str, *, timeout: float = 15.0) -> None:
        """Validate fixture policy and create a lazy adapter without a native connection.

        :param url: Explicit broker connection URL; never discovered from environment variables.
        :param timeout: Finite operation budget in seconds.
        :returns: None; each operation later creates its own native client.
        :raises ConfigurationError: If timeout or Redis configuration is invalid.
        """
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
        """Reject new publisher/admin work after cleanup starts.

        :returns: None while this fixture remains open.
        :raises ConfigurationError: If cleanup has already run.
        """
        if self._closed:
            raise ConfigurationError("Redis Streams harness is already cleaned up")

    @asynccontextmanager
    async def _client(self) -> AsyncGenerator[Any, None]:
        """Own one fresh native client for a bounded publisher/admin operation.

        :yields: A binary-response native Redis client with zero configured retries.
        :raises TransportError: If native cleanup fails without an existing primary exception.
        :raises TimeoutError: If the operation exceeds the fixture timeout.
        """
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
        """Leave a unique existing empty typed stream using an owned XADD/XDEL seed.

        :param case_id: Nonempty test label used in a unique owned source name.
        :returns: A source naming the fixture-owned stream.
        :raises ConfigurationError: If the case label is invalid or the fixture is closed.
        :raises TransportError: If seed creation or seed deletion is not confirmed.
        """
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
        """Append supported fields without recreating a disappeared owned stream.

        :param source: Concrete existing source selected for this observation.
        :param payload: Raw message body; null is accepted only when the adapter advertises it.
        :param key: Optional message key, rejected by transports that do not support one.
        :param headers: Ordered normalized header pairs, subject to the transport capabilities.
        :returns: A receipt containing the server-generated redis.stream_id.
        :raises ConfigurationError: If the stream is unowned or header names are invalid UTF-8.
        :raises UnsupportedCapabilityError: If null bodies/headers, duplicate headers, or keys are supplied.
        :raises TransportError: If XADD does not return a valid confirmed stream ID.
        """
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
        """Delete only recorded fixture-owned stream keys and close its native client.

        :returns: None after all owned stream names are removed.
        :raises TransportError: If any owned key or native client cannot be cleaned up.
        """
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
