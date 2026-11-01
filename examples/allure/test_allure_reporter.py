"""Runnable Allure examples without Kafka; use --alluredir to create a report."""

import asyncio
from importlib.util import find_spec
import json

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.diagnostics import DiagnosticPolicy, DisclosurePolicy, ReportingMode
from event_observer.errors import EventTimeoutError
from event_observer.integrations.allure import AllureReporter
from event_observer.models import RawEvent
from event_observer.testing import FakeTransport

allure = pytest.importorskip("allure")
async_example = (
    pytest.mark.asyncio
    if find_spec("pytest_asyncio") is not None
    else pytest.mark.skip(reason="Install the pytest-asyncio extra to run the asynchronous example")
)


class JsonDecoder:
    def decode(self, event: RawEvent) -> dict:
        if event.payload is None:
            raise ValueError("This example expects a JSON object")
        return json.loads(event.payload)


def redact_operation(payload: dict) -> dict:
    # The redactor receives a separate copy; the Journal contents are preserved.
    payload.pop("access_token", None)
    return payload


@allure.feature("pytest-broker-observer")
class TestAllureReporterExamples:
    @allure.story("Synchronous test")
    @allure.title("Successful expectation: metadata-only summary")
    def test_sync_success(self, sync_events: SyncEventObserver, transport: FakeTransport):
        with sync_events.watch(source="operations") as watcher:
            with allure.step("Simulate operation completion after READY"):
                transport.publish(b"completed", source="operations")

            completed = watcher.wait_for_event(lambda payload: payload == b"completed", timeout=1)

            assert completed.payload == b"completed"
            assert completed.raw.source == "operations"

    @async_example
    @allure.story("Asynchronous test")
    @allure.title("Two parallel expectations receive the same event")
    async def test_async_parallel_waiters(self, async_events: EventObserver, transport: FakeTransport):
        async with async_events.watch(source="operations") as watcher:
            first = asyncio.create_task(watcher.wait_for(lambda payload: payload == b"completed", timeout=1))
            second = asyncio.create_task(
                watcher.wait_for_event(lambda payload: payload == b"completed", timeout=1)
            )

            # The Allure step ends before await, so it cannot overlap with other steps.
            with allure.step("Publish an event for two waiters"):
                transport.publish(b"completed", source="operations")

            payload, observed = await asyncio.gather(first, second)
            assert payload == observed.payload == b"completed"

    @allure.story("Expectation diagnostics")
    @allure.title("Expected timeout: the test passes and diagnostic attachments are retained")
    def test_expected_timeout(self, sync_events: SyncEventObserver, transport: FakeTransport):
        with sync_events.watch(source="operations") as watcher:
            transport.publish(b"started", source="operations")
            assert watcher.wait_for(lambda payload: payload == b"started", timeout=1) == b"started"

            # pytest.raises keeps this scenario passing.
            with pytest.raises(EventTimeoutError):
                watcher.wait_for(lambda payload: payload == b"completed", timeout=0.05)

    @allure.story("Content disclosure through an explicit policy")
    @allure.title("A mismatched JSON payload appears in the report after redaction")
    def test_redacted_payload(self, transport: FakeTransport):
        events = SyncEventObserver(
            transport=transport,
            reporters=[AllureReporter()],
            diagnostics=DiagnosticPolicy(
                mode=ReportingMode.SUMMARY,
                disclosure=DisclosurePolicy.REDACTED_PAYLOAD,
                redactor=redact_operation,
            ),
        )
        with events.watch(source="operations", decoder=JsonDecoder()) as watcher:
            transport.publish(
                json.dumps(
                    {
                        "operation_id": "operation-42",
                        "status": "pending",
                        "access_token": "demo-private-token",
                    }
                ).encode(),
                source="operations",
            )
            original = watcher.wait_for(lambda payload: payload["status"] == "pending", timeout=1)

            with pytest.raises(EventTimeoutError):
                watcher.wait_for(lambda payload: payload["status"] == "completed", timeout=0.05)

            # The token is absent from message-content.json, while the test data remains unchanged.
            assert original["access_token"] == "demo-private-token"

    @allure.story("Intentional demonstration failure")
    @allure.title("FAILED: completed did not arrive; enabled by a separate flag")
    def test_intentional_timeout_failure(
        self,
        demo_failure_enabled: None,
        sync_events: SyncEventObserver,
        transport: FakeTransport,
    ):
        # This example intentionally exits pytest with code 1 when --allure-demo-failure is set.
        with sync_events.watch(source="operations") as watcher:
            transport.publish(b"rejected", source="operations")
            assert watcher.wait_for(lambda payload: payload == b"rejected", timeout=1) == b"rejected"
            watcher.wait_for(lambda payload: payload == b"completed", timeout=0.05)
