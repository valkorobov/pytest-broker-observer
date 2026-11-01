"""Real Redis Streams and AllureReporter: run with REDIS_URL and --alluredir."""

import asyncio
from collections.abc import Iterator
import json
import os
from uuid import uuid4

import pytest

from event_observer import ScopeStart, SyncEventObserver, TransportSource
from event_observer.diagnostics import DiagnosticPolicy, DisclosurePolicy, ReportingMode
from event_observer.errors import EventTimeoutError
from event_observer.integrations.allure import AllureReporter
from event_observer.matchers import header_equals
from event_observer.models import RawEvent
from event_observer.testing import RedisStreamsAdapterHarness

allure = pytest.importorskip("allure")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("REDIS_URL"),
        reason="Set REDIS_URL to run the Redis Streams + Allure examples",
    ),
]


class JsonDecoder:
    def decode(self, event: RawEvent) -> dict:
        if event.payload is None:
            raise ValueError("This example expects a JSON object")
        return json.loads(event.payload)


def redact_operation(payload: dict) -> dict:
    # Only this independent report copy is changed, preserving the test's message.
    payload.pop("access_token", None)
    return payload


@pytest.fixture
def redis_stream_allure_source(
    request: pytest.FixtureRequest,
) -> Iterator[tuple[SyncEventObserver, RedisStreamsAdapterHarness, TransportSource]]:
    harness = RedisStreamsAdapterHarness(os.environ["REDIS_URL"])
    try:
        with allure.step("Create a unique typed Redis stream for the test"):
            source = asyncio.run(harness.create_source(case_id=request.node.name))
        events = SyncEventObserver(
            transport=harness.adapter,
            reporters=[AllureReporter()],
            diagnostics=DiagnosticPolicy(
                mode=ReportingMode.SUMMARY,
                disclosure=DisclosurePolicy.REDACTED_PAYLOAD,
                redactor=redact_operation,
            ),
        )
        yield events, harness, source
    finally:
        with allure.step("Delete only this test's Redis stream"):
            asyncio.run(harness.cleanup())


@allure.feature("pytest-broker-observer")
@allure.story("Real Redis Streams through Docker Compose")
class TestRedisStreamsAllureReporter:
    @allure.title("Redis Streams: an event after READY is found by operation-id")
    def test_redis_stream_correlated_success(
        self,
        redis_stream_allure_source: tuple[SyncEventObserver, RedisStreamsAdapterHarness, TransportSource],
    ):
        events, harness, source = redis_stream_allure_source
        operation_id = uuid4().hex.encode()
        published = {
            "operation_id": operation_id.decode(),
            "status": "completed",
            "access_token": "demo-private-token",
        }

        with events.watch(source=source, decoder=JsonDecoder()) as watcher:
            operation = watcher.scope(header_equals("operation-id", operation_id))
            mark = operation.mark()
            with allure.step("Publish completed through an independent XADD producer after READY"):
                receipt = asyncio.run(
                    harness.publish(
                        source,
                        json.dumps(published).encode(),
                        headers=(("operation-id", operation_id),),
                    )
                )
                assert receipt.accepted

            completed = operation.wait_for_event(
                lambda payload: payload["status"] == "completed", timeout=10, since=mark
            )
            with allure.step("Check the observed body and server-generated stream ID"):
                assert completed.payload == published
                assert "access_token" in completed.payload
                assert completed.raw.source == source.name
                assert completed.raw.key is None
                assert completed.raw.metadata["redis.stream_id"] == receipt.metadata["redis.stream_id"]

    @allure.title("Redis Streams: started arrives; completed times out with mismatch diagnostics")
    def test_redis_stream_expected_timeout(
        self,
        redis_stream_allure_source: tuple[SyncEventObserver, RedisStreamsAdapterHarness, TransportSource],
    ):
        events, harness, source = redis_stream_allure_source
        operation_id = uuid4().hex.encode()
        published = {
            "operation_id": operation_id.decode(),
            "status": "started",
            "access_token": "demo-private-token",
        }

        with events.watch(source=source, decoder=JsonDecoder()) as watcher:
            operation = watcher.scope(
                header_equals("operation-id", operation_id), start=ScopeStart.from_now()
            )
            with allure.step("Publish started and retain the Redis stream receipt"):
                receipt = asyncio.run(
                    harness.publish(
                        source,
                        json.dumps(published).encode(),
                        headers=(("operation-id", operation_id),),
                    )
                )
                assert receipt.accepted

            # Confirm a real entry arrived before demonstrating the expected timeout.
            started = operation.wait_for(lambda payload: payload["status"] == "started", timeout=10)
            assert started == published

            # The second expectation scans the retained Journal with its own cursor.
            with pytest.raises(EventTimeoutError):
                operation.wait_for(lambda payload: payload["status"] == "completed", timeout=0.1)

            # Matched and mismatched report copies are redacted; the test value stays intact.
            assert started == published
            assert "access_token" in started
