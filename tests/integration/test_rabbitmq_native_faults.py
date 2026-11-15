"""RabbitMQ faults on the dedicated Testcontainers fixture and its own subscriptions."""

import asyncio
from base64 import b64encode
from contextlib import aclosing, contextmanager
import json
import os
import re
import time
from urllib.error import URLError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.errors import (
    EventTimeoutError,
    ObserverCleanupTimeoutError,
    TransportAuthenticationError,
    TransportAuthorizationError,
    TransportConnectionError,
    TransportInterruptedError,
)
from event_observer.testing import RabbitMQAdapterHarness
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig
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
    deadline,
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
        os.environ.get("EVENT_OBSERVER_FAULTS") != "rabbitmq",
        reason="Run the dedicated isolated RabbitMQ fault profile",
    ),
]


@pytest.fixture(scope="module")
def rabbit_control():
    """Verify the exact AMQP/admin ports before issuing requests to the private node."""
    url = os.environ["RABBITMQ_URL"]
    amqp = urlsplit(url)
    management = urlsplit(os.environ["RABBITMQ_MANAGEMENT_URL"])
    assert management.hostname == amqp.hostname == "127.0.0.1"
    assert management.scheme == "http" and not any(
        (management.username, management.password, management.path, management.query, management.fragment)
    )
    assert amqp.port is not None and management.port is not None
    assert amqp.username is not None and amqp.password is not None and amqp.path == "/"
    verify_owned_broker_ports("rabbitmq", {5672: amqp.port, 15672: management.port})
    credentials = b64encode(f"{unquote(amqp.username)}:{unquote(amqp.password)}".encode()).decode()
    opener = build_opener(ProxyHandler({}))

    def request(path, *, method="GET", data=None, timeout=5):
        body = None if data is None else json.dumps(data).encode()
        operation = Request(
            management.geturl() + path,
            data=body,
            method=method,
            headers={"Authorization": f"Basic {credentials}", "Content-Type": "application/json"},
        )
        with opener.open(operation, timeout=timeout) as response:
            return json.load(response) if method == "GET" else None

    return url, request


@pytest.fixture
def rabbit_source(rabbit_control):
    harness = RabbitMQAdapterHarness(rabbit_control[0], timeout=IO_TIMEOUT)
    try:
        source = asyncio.run(harness.create_source(case_id="stage4-rabbitmq"))
        yield harness, source
    finally:
        asyncio.run(harness.cleanup())


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_startup_unavailable(mode, rabbit_control, rabbit_source):
    _, source = rabbit_source
    with unavailable_url(rabbit_control[0]) as url:
        check_startup_failure(
            RabbitMQAdapter(RabbitMQConfig(url)), source, mode, (TransportConnectionError, TimeoutError)
        )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_startup_rejects_invalid_credentials(mode, rabbit_control, rabbit_source):
    url, _ = rabbit_control
    _, source = rabbit_source
    assert (username := urlsplit(url).username) is not None
    invalid = url_with_credentials(url, unquote(username), os.environ["EVENT_OBSERVER_FAULT_PASSWORD"])
    check_startup_failure(
        RabbitMQAdapter(RabbitMQConfig(invalid)), source, mode, TransportAuthenticationError
    )


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_startup_rejects_missing_permissions(mode, rabbit_control, rabbit_source):
    url, request = rabbit_control
    _, source = rabbit_source
    username, password = f"fault-{uuid4().hex}", os.environ["EVENT_OBSERVER_FAULT_PASSWORD"]
    path = f"/api/users/{username}"
    request(path, method="PUT", data={"password": password, "tags": ""})
    try:
        # Queue declaration and passive exchange inspection are allowed. Bind must fail.
        request(
            f"/api/permissions/%2F/{username}",
            method="PUT",
            data={"configure": r"^amq\.gen-", "write": r"(?!)", "read": f"^{re.escape(source.name)}$"},
        )
        restricted = RabbitMQAdapter(RabbitMQConfig(url_with_credentials(url, username, password)))
        check_startup_failure(restricted, source, mode, TransportAuthorizationError)

        def gone(timeout):
            connections = request("/api/connections", timeout=timeout)
            return None if any(connection["user"] == username for connection in connections) else True

        wait_until_ready("RabbitMQ denied reader cleanup", gone, timeout=IO_TIMEOUT)
    finally:
        request(path, method="DELETE")


def _close_subscription_connection(request, queue_name: str, consumer_tag: str) -> None:
    """Close exactly the owner verified by both queue and consumer management metadata."""
    assert queue_name.startswith("amq.gen-") and consumer_tag.startswith("observer-")
    queue_path = "/api/queues/%2F/" + quote(queue_name, safe="")

    def probe(timeout):
        queue = request(queue_path, timeout=timeout)
        assert queue["name"] == queue_name and queue["exclusive"] is True and queue["vhost"] == "/"
        owned = (queue.get("owner_pid_details") or {}).get("name")
        for consumer in queue.get("consumer_details", []):
            if consumer["consumer_tag"] == consumer_tag and owned:
                named = (consumer.get("channel_details") or {}).get("connection_name")
                if named:
                    assert named == owned
                    return str(owned)
        return None

    name = wait_until_ready("RabbitMQ management ownership", probe, timeout=15)
    request("/api/connections/" + quote(name, safe=""), method="DELETE")


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_disconnect_when_handoff_is_full(mode, rabbit_control, rabbit_source):
    _, request = rabbit_control
    harness, source = rabbit_source

    async def run_async():
        async with async_session(harness.adapter, source) as session:
            assert (await harness.publish(source, b"held-without-settlement")).accepted
            async with aclosing(session.receive()) as records:
                raw = await asyncio.wait_for(anext(records), IO_TIMEOUT)
                assert raw.payload == b"held-without-settlement" and raw.delivery_id in session._pending
                assert (
                    len(session._pending) == session.readiness.source_metadata["rabbitmq.prefetch_count"] == 1
                )
                assert (await harness.publish(source, b"queued-at-broker")).accepted
                await asyncio.to_thread(
                    _close_subscription_connection, request, session._queue.name, session._consumer_tag
                )
                error = await asyncio.wait_for(session.wait_failure(), IO_TIMEOUT)
                assert isinstance(error, TransportInterruptedError)
                with pytest.raises(TransportInterruptedError) as caught:
                    await anext(records)
                assert caught.value is error

    if mode == "async":
        asyncio.run(run_async())
    else:
        with sync_session(harness.adapter, source) as session:
            assert asyncio.run(harness.publish(source, b"held-without-settlement")).accepted
            raw = poll_until_record(session)
            assert raw.payload == b"held-without-settlement" and raw.delivery_id in session._pending
            assert len(session._pending) == session.readiness.source_metadata["rabbitmq.prefetch_count"] == 1
            assert asyncio.run(harness.publish(source, b"queued-at-broker")).accepted
            _close_subscription_connection(request, session._queue, session._consumer_tag)
            error = drive_blocking_failure(session, TransportConnectionError)
            assert error.cause.reply_code == 320


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_blackhole_after_ready(mode, rabbit_source):
    harness, source = rabbit_source

    async def run_async():
        async with EventObserver(transport=harness.adapter).watch(source=source) as watcher:
            with paused_owned_broker("rabbitmq"):
                start = time.monotonic()
                with pytest.raises(EventTimeoutError):
                    await watcher.wait_for(lambda _: True, timeout=0.25)
                assert 0.20 <= time.monotonic() - start < 2.0
            assert (await harness.publish(source, b"after-pause")).accepted
            assert await watcher.wait_for(lambda _: True, timeout=IO_TIMEOUT) == b"after-pause"

    if mode == "async":
        asyncio.run(run_async())
    else:
        with SyncEventObserver(transport=harness.adapter).watch(source=source) as watcher:
            with paused_owned_broker("rabbitmq"):
                start = time.monotonic()
                with pytest.raises(EventTimeoutError):
                    watcher.wait_for(lambda _: True, timeout=0.25)
                assert 0.20 <= time.monotonic() - start < 2.0
            assert asyncio.run(harness.publish(source, b"after-pause")).accepted
            assert watcher.wait_for(lambda _: True, timeout=IO_TIMEOUT) == b"after-pause"


@contextmanager
def _recover_rabbit(url, request):
    """Wait for the private RabbitMQ node to serve native and cleanup requests again."""
    import pika

    try:
        yield
    finally:

        def probe(timeout):
            try:
                request("/api/overview", timeout=timeout / 2)
                parameters = pika.URLParameters(url)
                parameters.connection_attempts = 1
                parameters.socket_timeout = timeout / 2
                parameters.stack_timeout = timeout / 2
                with pika.BlockingConnection(parameters):
                    return True
            except (pika.exceptions.ProbableAuthenticationError, pika.exceptions.ProbableAccessDeniedError):
                raise
            except (TimeoutError, URLError, OSError, pika.exceptions.AMQPConnectionError):
                return None

        wait_until_ready("RabbitMQ management after restart", probe, timeout=45)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_restart_after_ready(mode, rabbit_control, rabbit_source):
    url, request = rabbit_control
    harness, source = rabbit_source

    async def run_async():
        async with async_session(harness.adapter, source) as session:
            await asyncio.to_thread(restart_owned_broker, "rabbitmq")
            error = await asyncio.wait_for(session.wait_failure(), IO_TIMEOUT)
            assert isinstance(error, TransportInterruptedError)
            async with aclosing(session.receive()) as records:
                with pytest.raises(TransportInterruptedError) as caught:
                    await anext(records)
                assert caught.value is error

    with _recover_rabbit(url, request):
        if mode == "async":
            asyncio.run(run_async())
        else:
            with sync_session(harness.adapter, source) as session:
                restart_owned_broker("rabbitmq")
                drive_blocking_failure(session, (TransportConnectionError, TransportInterruptedError))
    replacement = asyncio.run(harness.create_source(case_id="after-rabbitmq-restart"))
    with sync_session(harness.adapter, replacement) as session:
        assert asyncio.run(harness.publish(replacement, b"fresh-subscription")).accepted
        assert poll_until_record(session).payload == b"fresh-subscription"


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_stalled_close_after_ready(mode, rabbit_source):
    harness, source = rabbit_source

    async def run_async():
        from event_observer.config import TransportBufferLimits
        from event_observer.models import OpenContext, StartPosition

        session = await harness.adapter.open(
            source,
            StartPosition.latest(),
            context=OpenContext("stalled", deadline(), TransportBufferLimits()),
        )
        try:
            assert session.readiness.boundary.source == source.name
            with paused_owned_broker("rabbitmq"), pytest.raises(ObserverCleanupTimeoutError):
                await session.close(deadline=deadline(0.3))
            assert not session._closed
        finally:
            if not session._connection.is_closed:
                await asyncio.wait_for(session._connection.close(), IO_TIMEOUT)
        assert session._connection.is_closed

    if mode == "async":
        asyncio.run(run_async())
    else:
        with sync_session(harness.adapter, source) as session:
            with paused_owned_broker("rabbitmq"), pytest.raises(ObserverCleanupTimeoutError):
                session.close(deadline=deadline(0.3))
            assert session._connection.is_closed
