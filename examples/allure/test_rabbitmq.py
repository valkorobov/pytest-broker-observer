"""Real RabbitMQ and AllureReporter: run with RABBITMQ_URL and --alluredir."""

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
from event_observer.testing import RabbitMQAdapterHarness

allure = pytest.importorskip("allure")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("RABBITMQ_URL"),
        reason="Set RABBITMQ_URL to run the RabbitMQ + Allure examples",
    ),
]


class JsonDecoder:
    def decode(self, event: RawEvent) -> dict:
        if event.payload is None:
            raise ValueError("This example expects a JSON object")
        return json.loads(event.payload)


def redact_operation(payload: dict) -> dict:
    # Diagnostics passes an independent copy, preserving the test's message.
    payload.pop("access_token", None)
    return payload


@pytest.fixture
def rabbitmq_allure_source(
    request: pytest.FixtureRequest,
) -> Iterator[tuple[SyncEventObserver, RabbitMQAdapterHarness, TransportSource]]:
    harness = RabbitMQAdapterHarness(os.environ["RABBITMQ_URL"])
    try:
        with allure.step("Create a unique RabbitMQ exchange for the test"):
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
        with allure.step("Delete only this test's RabbitMQ exchange"):
            asyncio.run(harness.cleanup())


@allure.feature("pytest-broker-observer")
@allure.story("Real RabbitMQ through Docker Compose")
class TestRabbitMQAllureReporter:
    @allure.title("RabbitMQ: an event after READY is found by operation-id")
    def test_rabbitmq_correlated_success(
        self,
        rabbitmq_allure_source: tuple[SyncEventObserver, RabbitMQAdapterHarness, TransportSource],
    ):
        events, harness, source = rabbitmq_allure_source
        operation_id = uuid4().hex.encode()
        published = {
            "operation_id": operation_id.decode(),
            "status": "completed",
            "access_token": "demo-private-token",
        }

        with events.watch(source=source, decoder=JsonDecoder()) as watcher:
            operation = watcher.scope(header_equals("operation-id", operation_id))
            mark = operation.mark()
            with allure.step("Publish completed to the exchange after the observer queue is READY"):
                # Replace the harness publication with the action of your application.
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
            with allure.step("Check the event received through the dedicated observer queue"):
                assert completed.payload == published
                assert "access_token" in completed.payload
                assert completed.raw.source == source.name
                assert completed.raw.key is None
                assert isinstance(completed.raw.metadata["rabbitmq.routing_key"], str)

    @allure.title("RabbitMQ: started arrives; waiting for completed times out and retains mismatches")
    def test_rabbitmq_expected_timeout(
        self,
        rabbitmq_allure_source: tuple[SyncEventObserver, RabbitMQAdapterHarness, TransportSource],
    ):
        events, harness, source = rabbitmq_allure_source
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
            with allure.step("Publish started and wait for broker confirmation"):
                receipt = asyncio.run(
                    harness.publish(
                        source,
                        json.dumps(published).encode(),
                        headers=(("operation-id", operation_id),),
                    )
                )
                assert receipt.accepted

            # Establish that the bound observer queue delivered a real message.
            started = operation.wait_for(lambda payload: payload["status"] == "started", timeout=10)
            assert started == published

            # This scans the retained Journal again, without asking RabbitMQ for replay.
            with pytest.raises(EventTimeoutError):
                operation.wait_for(lambda payload: payload["status"] == "completed", timeout=0.1)

            # Redaction of matched and mismatched report copies leaves the test value intact.
            assert started == published
            assert "access_token" in started
