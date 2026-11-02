"""Published blocking contracts against a genuinely synchronous fake harness."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from event_observer.errors import TransportError
from event_observer.models import StartPosition, TransportSource
from event_observer.testing import FakeTransport, PublishReceipt
from event_observer.testing.contracts.sync import (
    SyncIsolationContract,
    SyncLifecycleContract,
    SyncReadinessContract,
    _context,
    _sessions,
)


class FakeSyncAdapterHarness:
    def __init__(self):
        self.transport = FakeTransport()
        self.adapter = self.transport.sync_adapter()

    def create_source(self, *, case_id):
        return TransportSource(case_id)

    def publish(self, source, payload, *, key=None, headers=()):
        self.transport.publish(payload, source=source.name, key=key, headers=headers)
        return PublishReceipt()

    def cleanup(self):
        import time

        for session in self.transport.sessions:
            session.close(deadline=time.monotonic() + 1)


@pytest.fixture
def sync_adapter_harness():
    harness = FakeSyncAdapterHarness()
    try:
        yield harness
    finally:
        harness.cleanup()


class TestFakeBlockingAdapterContracts(SyncLifecycleContract, SyncReadinessContract, SyncIsolationContract):
    pass


def test_failure_is_visible_without_publication_or_polling(sync_adapter_harness):
    source = sync_adapter_harness.create_source(case_id="idle-failure")
    with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
        error = TransportError("Controlled idle transport failure")
        sync_adapter_harness.transport.fail(error)
        assert sessions[0].failure() is error
        with pytest.raises(TransportError) as caught:
            sessions[0].poll(0)
        assert caught.value is error


@pytest.mark.parametrize(
    ("method", "capability", "value"),
    [
        ("test_readiness_earliest_replays_available_history", "start_modes", frozenset({"latest"})),
        ("test_readiness_null_payload_is_distinct_from_empty_bytes", "supports_null_payload", False),
        ("test_readiness_preserves_duplicate_and_null_headers", "supports_duplicate_headers", False),
        ("test_readiness_preserves_message_key", "supports_key", False),
    ],
)
def test_undeclared_optional_cases_are_explicit_not_applicable_before_source_creation(
    method, capability, value
):
    capabilities = replace(FakeTransport.capabilities, **{capability: value})
    harness = SimpleNamespace(adapter=SimpleNamespace(capabilities=capabilities))
    properties = []
    with pytest.raises(pytest.skip.Exception, match="NOT_APPLICABLE"):
        getattr(SyncReadinessContract(), method)(harness, lambda key, value: properties.append((key, value)))
    assert properties[-1] == ("contract_applicability", "NOT_APPLICABLE")
    assert any(name == "contract_precondition" for name, _ in properties)


def test_blocking_contract_execution_does_not_create_or_run_asyncio(sync_adapter_harness, monkeypatch):
    import asyncio

    def forbidden(*args, **kwargs):
        raise AssertionError("A blocking contract attempted to use an event loop")

    monkeypatch.setattr(asyncio, "run", forbidden)
    monkeypatch.setattr(asyncio, "new_event_loop", forbidden)
    monkeypatch.setattr(asyncio, "get_running_loop", forbidden)
    SyncReadinessContract().test_readiness_immediate_publish_is_observed(sync_adapter_harness)
    session = sync_adapter_harness.adapter.open(
        TransportSource("no-loop"), StartPosition.latest(), context=_context()
    )
    assert session.poll(0) is None
