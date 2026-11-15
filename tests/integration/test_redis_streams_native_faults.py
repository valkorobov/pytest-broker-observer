"""Redis Streams connection and continuity faults on one private Redis server."""

import asyncio
from contextlib import aclosing, contextmanager
import os
import time
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from event_observer.errors import (
    TransportAuthenticationError,
    TransportAuthorizationError,
    TransportConnectionError,
    TransportInterruptedError,
)
from event_observer.testing import RedisStreamsAdapterHarness
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig
from tests.integration.containers.common import (
    paused_owned_broker,
    restart_owned_broker,
    verify_owned_broker_ports,
    wait_until_ready,
)
from tests.integration.containers.fault_checks import (
    IO_TIMEOUT,
    async_session,
    check_startup_failure,
    drive_blocking_failure,
    poll_until_record,
    sync_session,
    unavailable_url,
    url_with_credentials,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        os.environ.get("EVENT_OBSERVER_FAULTS") != "redis-streams",
        reason="Run the dedicated isolated Redis Streams fault profile",
    ),
]


@pytest.fixture(scope="module")
def redis_control():
    """Allow Redis administration only against the verified isolated container."""
    import redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    url = os.environ["REDIS_URL"]
    endpoint = urlsplit(url)
    assert endpoint.scheme == "redis" and endpoint.hostname == "127.0.0.1" and endpoint.port
    assert endpoint.path == "/0" and not (endpoint.query or endpoint.fragment)
    verify_owned_broker_ports("redis-streams", {6379: endpoint.port})
    with redis.Redis.from_url(
        url, socket_timeout=2, socket_connect_timeout=2, retry=Retry(NoBackoff(), 0)
    ) as control:
        assert control.ping()
        yield url, control


@pytest.fixture
def redis_source(redis_control):
    harness = RedisStreamsAdapterHarness(redis_control[0], timeout=IO_TIMEOUT)
    try:
        source = asyncio.run(harness.create_source(case_id="stage4-redis-streams"))
        yield harness, source
    finally:
        asyncio.run(harness.cleanup())


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_startup_unavailable(mode, redis_control, redis_source):
    _, source = redis_source
    with unavailable_url(redis_control[0]) as url:
        check_startup_failure(
            RedisStreamsAdapter(RedisStreamsConfig(url)),
            source,
            mode,
            (TransportConnectionError, TimeoutError),
        )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_startup_rejects_invalid_credentials(mode, redis_control, redis_source):
    _, source = redis_source
    invalid = url_with_credentials(redis_control[0], "default", os.environ["EVENT_OBSERVER_FAULT_PASSWORD"])
    check_startup_failure(
        RedisStreamsAdapter(RedisStreamsConfig(invalid)), source, mode, TransportAuthenticationError
    )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_startup_rejects_missing_permissions(mode, redis_control, redis_source):
    url, control = redis_control
    _, source = redis_source
    username, password = f"fault-{uuid4().hex}", os.environ["EVENT_OBSERVER_FAULT_PASSWORD"]
    # Keep native connection identification and the transaction valid, but deny XINFO.
    assert control.execute_command(
        "ACL",
        "SETUSER",
        username,
        "on",
        ">" + password,
        "~" + source.name,
        "+ping",
        "+client|id",
        "+multi",
        "+exec",
        "+info",
    )
    try:
        restricted = RedisStreamsAdapter(RedisStreamsConfig(url_with_credentials(url, username, password)))
        check_startup_failure(restricted, source, mode, TransportAuthorizationError)

        def gone(_timeout):
            return None if any(client["user"] == username for client in control.client_list()) else True

        wait_until_ready("Redis denied reader cleanup", gone, timeout=IO_TIMEOUT)
    finally:
        assert control.acl_deluser(username) == 1


def _kill_reader(control, session) -> None:
    """Disconnect only the native observer client confirmed in its READY boundary."""
    identifier = session.readiness.source_metadata["redis.client_id"]
    assert isinstance(identifier, int) and not isinstance(identifier, bool)
    assert identifier != control.client_id()
    assert control.execute_command("CLIENT", "KILL", "ID", identifier) == 1


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_disconnect_when_handoff_is_full(mode, redis_control, redis_source):
    _, control = redis_control
    harness, source = redis_source

    async def run_async():
        async with async_session(harness.adapter, source) as session:
            assert (await harness.publish(source, b"pending-redis-delivery")).accepted
            await asyncio.wait_for(session._wake.wait(), IO_TIMEOUT)
            assert len(session._buffer) == 1 and session._buffer[0][0].payload == b"pending-redis-delivery"
            _kill_reader(control, session)
            error = await asyncio.wait_for(session.wait_failure(), IO_TIMEOUT)
            assert isinstance(error, TransportInterruptedError)
            async with aclosing(session.receive()) as records:
                with pytest.raises(TransportInterruptedError) as caught:
                    await anext(records)
                assert caught.value is error

    if mode == "async":
        asyncio.run(run_async())
    else:
        with sync_session(harness.adapter, source) as session:
            assert asyncio.run(harness.publish(source, b"pending-redis-delivery")).accepted
            with session._condition:
                assert session._condition.wait_for(lambda: bool(session._buffer), timeout=IO_TIMEOUT)
                assert (
                    len(session._buffer) == 1 and session._buffer[0][0].payload == b"pending-redis-delivery"
                )
            _kill_reader(control, session)
            with session._condition:
                assert session._condition.wait_for(lambda: session._error is not None, timeout=IO_TIMEOUT)
            drive_blocking_failure(session, TransportInterruptedError)


def _exercise_loss(mode, adapter, source, inject):
    """Await the first terminal signal after a fault and reject subsequent delivery."""

    async def run_async():
        async with async_session(adapter, source) as session:
            with inject():
                error = await asyncio.wait_for(session.wait_failure(), IO_TIMEOUT)
                assert isinstance(error, TransportInterruptedError)
                async with aclosing(session.receive()) as records:
                    with pytest.raises(TransportInterruptedError) as caught:
                        await anext(records)
                    assert caught.value is error
                return error

    if mode == "async":
        return asyncio.run(run_async())
    with sync_session(adapter, source) as session:
        with inject():
            return drive_blocking_failure(session, TransportInterruptedError)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_blackhole_after_ready(mode, redis_control, redis_source):
    import redis

    _, source = redis_source
    adapter = RedisStreamsAdapter(RedisStreamsConfig(redis_control[0], socket_timeout=0.4))

    @contextmanager
    def blackhole():
        with paused_owned_broker("redis-streams"):
            blackholed_at = time.monotonic()
            yield
            assert time.monotonic() - blackholed_at < 3

    error = _exercise_loss(mode, adapter, source, blackhole)
    assert isinstance(error.cause, redis.exceptions.TimeoutError)
    assert redis_control[1].ping()


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_restart_after_ready(mode, redis_control, redis_source):
    import redis

    _, control = redis_control
    harness, source = redis_source
    run_id = control.info("server")["run_id"]

    def probe(timeout):
        with redis.Redis.from_url(
            redis_control[0], socket_timeout=timeout, socket_connect_timeout=timeout
        ) as server:
            try:
                return server.info("server").get("run_id")
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                return None

    @contextmanager
    def restart():
        try:
            restart_owned_broker("redis-streams")
            yield
        finally:
            assert wait_until_ready("Redis after restart", probe, timeout=30) != run_id

    _exercise_loss(mode, harness.adapter, source, restart)
    # Restore just this test's nonpersistent stream and establish a new boundary.
    assert control.xadd(source.name, {"payload": "new-server"})
    with sync_session(harness.adapter, source) as session:
        assert asyncio.run(harness.publish(source, b"fresh-subscription")).accepted
        assert poll_until_record(session).payload == b"fresh-subscription"
