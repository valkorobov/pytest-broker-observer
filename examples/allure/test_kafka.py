"""Real Kafka and AllureReporter: run with KAFKA_BOOTSTRAP_SERVERS and --alluredir."""

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
from event_observer.testing import KafkaAdapterHarness

allure = pytest.importorskip("allure")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        reason="Set KAFKA_BOOTSTRAP_SERVERS to run the Kafka + Allure examples",
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
def kafka_allure_source(
    request: pytest.FixtureRequest,
) -> Iterator[tuple[SyncEventObserver, KafkaAdapterHarness, TransportSource]]:
    harness = KafkaAdapterHarness(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    try:
        with allure.step("Create a unique Kafka topic for the test"):
            source = asyncio.run(harness.create_source(case_id=request.node.name))
        events = SyncEventObserver(
            transport=harness.adapter,
            reporters=[AllureReporter()],
            # Explicit disclosure shows matched and mismatched bodies after redaction.
            diagnostics=DiagnosticPolicy(
                mode=ReportingMode.SUMMARY,
                disclosure=DisclosurePolicy.REDACTED_PAYLOAD,
                redactor=redact_operation,
            ),
        )
        yield events, harness, source
    finally:
        with allure.step("Delete only this test's Kafka topic"):
            asyncio.run(harness.cleanup())


@allure.feature("pytest-broker-observer")
@allure.story("Real Kafka through Docker Compose")
class TestKafkaAllureReporter:
    @allure.title("Kafka: an event after READY is found by operation-id")
    def test_kafka_correlated_success(
        self,
        kafka_allure_source: tuple[SyncEventObserver, KafkaAdapterHarness, TransportSource],
    ):
        events, harness, source = kafka_allure_source
        operation_id = uuid4().hex.encode()
        published = {
            "operation_id": operation_id.decode(),
            "status": "completed",
            "access_token": "demo-private-token",
        }

        with events.watch(source=source, decoder=JsonDecoder()) as watcher:
            operation = watcher.scope(
                header_equals("operation-id", operation_id), start=ScopeStart.from_now()
            )
            with allure.step("Publish completed through an independent producer after READY"):
                # In an application test, replace this call with the action of the service under test.
                receipt = asyncio.run(
                    harness.publish(
                        source,
                        json.dumps(published).encode(),
                        key=operation_id,
                        headers=(("operation-id", operation_id),),
                    )
                )
                assert receipt.accepted

            completed = operation.wait_for_event(lambda payload: payload["status"] == "completed", timeout=10)
            with allure.step("Check the event and the broker-confirmed offset"):
                # Only the report's copy is redacted; returned test data keeps the token.
                assert completed.payload == published
                assert "access_token" in completed.payload
                assert completed.raw.source == source.name
                assert completed.raw.key == operation_id
                assert completed.raw.metadata["kafka.partition"] == receipt.metadata["kafka.partition"]
                assert completed.raw.metadata["kafka.offset"] == receipt.metadata["kafka.offset"]

    @allure.title("Kafka: started is received; waiting for completed times out and retains mismatches")
    def test_kafka_expected_timeout(
        self,
        kafka_allure_source: tuple[SyncEventObserver, KafkaAdapterHarness, TransportSource],
    ):
        events, harness, source = kafka_allure_source
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
            with allure.step("Publish started and wait for broker acknowledgement"):
                receipt = asyncio.run(
                    harness.publish(
                        source,
                        json.dumps(published).encode(),
                        headers=(("operation-id", operation_id),),
                    )
                )
                assert receipt.accepted

            # First establish that a real record was received, ruling out an empty-stream timeout.
            started = operation.wait_for(lambda payload: payload["status"] == "started", timeout=10)
            assert started == published

            # An independent expectation sees started again as a mismatch; the test remains PASSED.
            with pytest.raises(EventTimeoutError):
                operation.wait_for(lambda payload: payload["status"] == "completed", timeout=0.1)

            # Both report copies omit the token, while the received payload remains intact.
            assert started == published
            assert "access_token" in started
