"""Common bounded assertions for native broker fault tests."""

import asyncio
from contextlib import asynccontextmanager, contextmanager
import socket
import time
from urllib.parse import quote, urlsplit, urlunsplit

import pytest

from event_observer.config import TransportBufferLimits
from event_observer.models import OpenContext, StartPosition


IO_TIMEOUT = 6.0


def deadline(seconds: float = IO_TIMEOUT) -> float:
    """Supply one absolute monotonic deadline for a test operation."""
    return time.monotonic() + seconds


def _context(*, seconds: float, slots: int) -> OpenContext:
    """Use a small handoff so that its saturation can be verified directly."""
    return OpenContext("native-fault", deadline(seconds), TransportBufferLimits(max_events=slots))


@contextmanager
def sync_session(adapter, source, *, slots: int = 1, seconds: float = IO_TIMEOUT):
    """Open and clean one thread-owned native subscription."""
    session = adapter.sync_adapter().open(
        source, StartPosition.latest(), context=_context(seconds=seconds, slots=slots)
    )
    try:
        assert session.readiness.boundary.source == source.name
        yield session
    finally:
        session.close(deadline=deadline())


@asynccontextmanager
async def async_session(adapter, source, *, slots: int = 1, seconds: float = IO_TIMEOUT):
    """Open and clean one loop-owned native subscription."""
    session = await adapter.open(
        source, StartPosition.latest(), context=_context(seconds=seconds, slots=slots)
    )
    try:
        assert session.readiness.boundary.source == source.name
        yield session
    finally:
        await session.close(deadline=deadline())


def check_startup_failure(adapter, source, mode: str, expected) -> None:
    """Require the expected typed failure within a shared finite startup allowance."""
    elapsed_limit = deadline(6)

    async def run_async():
        async with async_session(adapter, source, seconds=2):
            pass

    with pytest.raises(expected) as caught:
        if mode == "async":
            asyncio.run(run_async())
        else:
            with sync_session(adapter, source, seconds=2):
                pass
    assert time.monotonic() < elapsed_limit
    assert getattr(caught.value, "cause", caught.value.__cause__) is not None


@contextmanager
def unavailable_url(url: str):
    """Reserve a loopback port without listening so the failure cannot reach another service."""
    parts = urlsplit(url)
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        user_info = parts.netloc.rpartition("@")[0]
        authority = f"{user_info}@127.0.0.1:{endpoint.getsockname()[1]}"
        yield urlunsplit(parts._replace(netloc=authority))


def url_with_credentials(url: str, username: str, password: str) -> str:
    """Send test credentials to the same verified loopback endpoint."""
    parts = urlsplit(url)
    authority = f"{quote(username, safe='')}:{quote(password, safe='')}@{parts.hostname}:{parts.port}"
    return urlunsplit(parts._replace(netloc=authority))


def poll_until_record(session):
    """Drive a native blocking reader until a record actually reaches the handoff."""
    until = deadline()
    while (remaining := until - time.monotonic()) > 0:
        if (record := session.poll(min(0.05, remaining))) is not None:
            return record
    pytest.fail("No native broker record reached the handoff within the budget")


def drive_blocking_failure(session, expected):
    """Pump blocking I/O and verify that no delivery wins over a known failure."""
    until = deadline()
    while (remaining := until - time.monotonic()) > 0:
        try:
            record = session.poll(min(0.05, remaining))
        except expected as error:
            assert session.failure() is error
            with pytest.raises(expected) as again:
                session.poll(0)
            assert again.value is error
            return error
        assert record is None, "Delivery escaped after the fault barrier"
    pytest.fail("No blocking terminal failure reached the session within the budget")
