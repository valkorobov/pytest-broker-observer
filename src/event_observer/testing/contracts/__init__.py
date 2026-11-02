"""Reusable adapter contracts; provide a function-scoped ``adapter_harness``.

Example::

    class TestMyAdapter(LifecycleContract, ReadinessContract, IsolationContract):
        pass

Each method is a separate pytest node. Optional capability checks record an
explicit NOT_APPLICABLE result only when the adapter does not advertise that
capability. Core readiness, independent observation and cleanup always run.
The harness fixture owns source/infrastructure cleanup, including failed tests.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import time
from typing import AsyncGenerator, Callable
from uuid import uuid4

import pytest

from ...config import TransportBufferLimits
from ...errors import ConfigurationError, TransportSubscriptionError
from ...models import OpenContext, StartPosition, TransportSource
from ...transports.base import TransportSession
from ..harness import AdapterTestHarness

_STARTUP_SECONDS = 10.0
_RECEIVE_SECONDS = 5.0
_CLOSE_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class ContractPrecondition:
    """Describe one declared capability needed by an optional contract case.

    :ivar capability: TransportCapabilities attribute inspected by the check.
    :ivar value: Required boolean value or member of a capability set.
    """

    capability: str
    value: str | bool


def _require(
    harness: AdapterTestHarness, precondition: ContractPrecondition, record_property: Callable
) -> None:
    """Record optional contract applicability and skip only undeclared semantics.

    :param harness: Adapter harness whose declared capabilities are inspected.
    :param precondition: Capability requirement for the current contract case.
    :param record_property: Pytest recorder used for contract_precondition and contract_applicability
        properties.
    :raises pytest.skip.Exception: The adapter does not advertise the requested optional capability.
    """
    declared = getattr(harness.adapter.capabilities, precondition.capability)
    applicable = (
        precondition.value in declared
        if isinstance(declared, (set, frozenset))
        else declared == precondition.value
    )
    record_property("contract_precondition", f"{precondition.capability}={precondition.value}")
    record_property("contract_applicability", "APPLICABLE" if applicable else "NOT_APPLICABLE")
    if not applicable:
        pytest.skip(
            f"NOT_APPLICABLE: adapter does not declare {precondition.capability}={precondition.value}"
        )


async def _source(harness: AdapterTestHarness) -> TransportSource:
    """Create a uniquely named per-case resource through the harness ownership boundary.

    :param harness: Harness responsible for source creation and later fixture cleanup.
    :returns: The newly prepared TransportSource.
    """
    return await harness.create_source(case_id=f"observer-contract-{uuid4().hex}")


def _context() -> OpenContext:
    """Build a fresh contract opening context with a ten-second startup deadline.

    :returns: An OpenContext with unique observer identity and default transport buffer limits.
    """
    return OpenContext(
        observer_id=uuid4().hex,
        startup_deadline=time.monotonic() + _STARTUP_SECONDS,
        buffers=TransportBufferLimits(),
    )


@asynccontextmanager
async def _sessions(
    harness: AdapterTestHarness,
    source: TransportSource,
    *starts: StartPosition,
) -> AsyncGenerator[list[TransportSession], None]:
    """Open requested independent sessions and close every opened handle in reverse order.

    :param harness: Adapter factory under contract test.
    :param source: Existing isolated source created by the harness.
    :param starts: One start policy per requested independent session.
    :yields: Successfully opened session handles while the contract body executes.
    :raises TimeoutError: Opening or closing a session exceeds the contract budget.
    """
    opened: list[TransportSession] = []
    try:
        for start in starts:
            session = await asyncio.wait_for(
                harness.adapter.open(source, start, context=_context()),
                timeout=_STARTUP_SECONDS,
            )
            opened.append(session)
        yield opened
    finally:
        for session in reversed(opened):
            await asyncio.wait_for(
                session.close(deadline=time.monotonic() + _CLOSE_SECONDS),
                timeout=_CLOSE_SECONDS,
            )


async def _receive(session: TransportSession):
    """Obtain one delivery under a finite deadline and close the temporary iterator.

    :param session: Session whose single receive iterator has not yet been consumed.
    :returns: The first received RawEvent.
    :raises TimeoutError: No event arrives before the five-second receive budget.
    """
    iterator = session.receive()
    try:
        return await asyncio.wait_for(anext(iterator), timeout=_RECEIVE_SECONDS)
    finally:
        close = getattr(iterator, "aclose", None)
        if close is not None:
            await close()


@pytest.mark.contract
class LifecycleContract:
    """Verify mandatory preparation, single-reader ownership, monitor independence, and cleanup."""

    @pytest.mark.asyncio
    async def test_lifecycle_open_has_resolved_readiness(self, adapter_harness: AdapterTestHarness):
        """Verify that latest open resolves the source, mode, clock, and an independent isolation strategy.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            readiness = sessions[0].readiness
            assert readiness.boundary.source == source.name
            assert readiness.boundary.start_mode == "latest"
            assert readiness.ready_monotonic <= time.monotonic()
            assert readiness.resolved_capabilities.isolation_modes.intersection(
                {"independent_reader", "dedicated_subscription"}
            )

    @pytest.mark.asyncio
    async def test_lifecycle_second_receive_iterator_is_rejected(self, adapter_harness: AdapterTestHarness):
        """Verify that requesting a second receive iterator fails instead of creating another reader.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            first = sessions[0].receive()
            second = None
            try:
                with pytest.raises((TransportSubscriptionError, ConfigurationError)):
                    second = sessions[0].receive()
                    await asyncio.wait_for(anext(second), timeout=_RECEIVE_SECONDS)
            finally:
                for stream in (first, second):
                    close = getattr(stream, "aclose", None)
                    if close is not None:
                        await close()

    @pytest.mark.asyncio
    async def test_lifecycle_close_is_idempotent(self, adapter_harness: AdapterTestHarness):
        """Verify that repeatedly closing the same session completes safely.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            for _ in range(2):
                await asyncio.wait_for(
                    sessions[0].close(deadline=time.monotonic() + _CLOSE_SECONDS), timeout=_CLOSE_SECONDS
                )

    @pytest.mark.asyncio
    async def test_lifecycle_close_releases_failure_monitor(self, adapter_harness: AdapterTestHarness):
        """Verify that clean close resolves an independently waiting failure monitor with None.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            monitor = asyncio.create_task(sessions[0].wait_failure())
            try:
                await sessions[0].close(deadline=time.monotonic() + _CLOSE_SECONDS)
                assert await asyncio.wait_for(monitor, timeout=_CLOSE_SECONDS) is None
            finally:
                monitor.cancel()
                await asyncio.gather(monitor, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_lifecycle_cancel_monitor_does_not_damage_session(
        self, adapter_harness: AdapterTestHarness
    ):
        """Verify that cancelling one failure waiter preserves delivery and later clean monitor completion.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            monitor = asyncio.create_task(sessions[0].wait_failure())
            # Let the coroutine register its independent wait before cancellation.
            await asyncio.sleep(0)
            monitor.cancel()
            with pytest.raises(asyncio.CancelledError):
                await monitor
            receipt = await adapter_harness.publish(source, b"after-cancel")
            assert receipt.accepted
            assert (await _receive(sessions[0])).payload == b"after-cancel"
            await sessions[0].close(deadline=time.monotonic() + _CLOSE_SECONDS)
            assert await asyncio.wait_for(sessions[0].wait_failure(), timeout=_CLOSE_SECONDS) is None


@pytest.mark.contract
class ReadinessContract:
    """Verify empty-source readiness, startup boundaries, and declared raw-envelope capabilities."""

    @pytest.mark.asyncio
    async def test_readiness_empty_source_needs_no_probe_message(self, adapter_harness: AdapterTestHarness):
        """Verify that an empty source reaches readiness without a synthetic probe publication.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            assert sessions[0].readiness.boundary.source == source.name

    @pytest.mark.asyncio
    async def test_readiness_immediate_publish_is_observed(self, adapter_harness: AdapterTestHarness):
        """Verify that a broker-accepted publication immediately after readiness is observed.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            receipt = await adapter_harness.publish(source, b"immediately-after-ready")
            assert receipt.accepted
            event = await _receive(sessions[0])
            assert event.payload == b"immediately-after-ready"
            assert event.source == source.name

    @pytest.mark.asyncio
    async def test_readiness_latest_excludes_published_history(self, adapter_harness: AdapterTestHarness):
        """Verify that latest excludes pre-open history while retaining a post-readiness publication.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        assert (await adapter_harness.publish(source, b"before-boundary")).accepted
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            assert (await adapter_harness.publish(source, b"after-boundary")).accepted
            assert (await _receive(sessions[0])).payload == b"after-boundary"

    @pytest.mark.asyncio
    async def test_readiness_earliest_replays_available_history(
        self, adapter_harness: AdapterTestHarness, record_property
    ):
        """Verify declared earliest replay against a publication accepted before opening.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        :param record_property: Pytest property recorder receiving explicit capability applicability
            results.
        """
        _require(adapter_harness, ContractPrecondition("start_modes", "earliest"), record_property)
        source = await _source(adapter_harness)
        assert (await adapter_harness.publish(source, b"available-history")).accepted
        async with _sessions(adapter_harness, source, StartPosition.earliest()) as sessions:
            assert (await _receive(sessions[0])).payload == b"available-history"

    @pytest.mark.asyncio
    async def test_readiness_null_payload_is_distinct_from_empty_bytes(
        self, adapter_harness: AdapterTestHarness, record_property
    ):
        """Verify declared null-payload support without collapsing it into empty bytes.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        :param record_property: Pytest property recorder receiving explicit capability applicability
            results.
        """
        _require(adapter_harness, ContractPrecondition("supports_null_payload", True), record_property)
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            stream = sessions[0].receive()
            try:
                assert (await adapter_harness.publish(source, None)).accepted
                assert (await asyncio.wait_for(anext(stream), timeout=_RECEIVE_SECONDS)).payload is None
                assert (await adapter_harness.publish(source, b"")).accepted
                assert (await asyncio.wait_for(anext(stream), timeout=_RECEIVE_SECONDS)).payload == b""
            finally:
                close = getattr(stream, "aclose", None)
                if close is not None:
                    await close()

    @pytest.mark.asyncio
    async def test_readiness_preserves_duplicate_and_null_headers(
        self, adapter_harness: AdapterTestHarness, record_property
    ):
        """Verify declared header preservation for duplicates, null values, and empty values.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        :param record_property: Pytest property recorder receiving explicit capability applicability
            results.
        """
        _require(adapter_harness, ContractPrecondition("supports_headers", True), record_property)
        _require(adapter_harness, ContractPrecondition("supports_duplicate_headers", True), record_property)
        _require(adapter_harness, ContractPrecondition("supports_null_header_values", True), record_property)
        source = await _source(adapter_harness)
        headers = (("duplicate", b"first"), ("duplicate", None), ("empty", b""))
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            assert (await adapter_harness.publish(source, b"headers", headers=headers)).accepted
            assert (await _receive(sessions[0])).headers == headers

    @pytest.mark.asyncio
    async def test_readiness_preserves_message_key(
        self, adapter_harness: AdapterTestHarness, record_property
    ):
        """Verify declared native-key support using a nonempty byte key.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        :param record_property: Pytest property recorder receiving explicit capability applicability
            results.
        """
        _require(adapter_harness, ContractPrecondition("supports_key", True), record_property)
        source = await _source(adapter_harness)
        async with _sessions(adapter_harness, source, StartPosition.latest()) as sessions:
            assert (await adapter_harness.publish(source, b"keyed", key=b"observation-key")).accepted
            assert (await _receive(sessions[0])).key == b"observation-key"


@pytest.mark.contract
class IsolationContract:
    """Verify that separate sessions and sources never consume or close one another."""

    @pytest.mark.asyncio
    async def test_isolation_sessions_both_observe_one_publication(self, adapter_harness: AdapterTestHarness):
        """Verify that two independent sessions both observe the same accepted publication.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(
            adapter_harness, source, StartPosition.latest(), StartPosition.latest()
        ) as sessions:
            assert sessions[0] is not sessions[1]
            assert (await adapter_harness.publish(source, b"independent-readers")).accepted
            events = await asyncio.gather(*(_receive(session) for session in sessions))
            assert [event.payload for event in events] == [b"independent-readers"] * 2

    @pytest.mark.asyncio
    async def test_isolation_closing_one_session_preserves_another(self, adapter_harness: AdapterTestHarness):
        """Verify that closing one session leaves another reader operational.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        source = await _source(adapter_harness)
        async with _sessions(
            adapter_harness, source, StartPosition.latest(), StartPosition.latest()
        ) as sessions:
            await sessions[0].close(deadline=time.monotonic() + _CLOSE_SECONDS)
            assert (await adapter_harness.publish(source, b"surviving-reader")).accepted
            assert (await _receive(sessions[1])).payload == b"surviving-reader"

    @pytest.mark.asyncio
    async def test_isolation_sources_do_not_cross_deliver(self, adapter_harness: AdapterTestHarness):
        """Verify that independent source names route only their corresponding publications.

        :param adapter_harness: Function-scoped fixture owning isolated source creation, confirmed
            publication, and cleanup.
        """
        first, second = await _source(adapter_harness), await _source(adapter_harness)
        async with _sessions(adapter_harness, first, StartPosition.latest()) as first_sessions:
            async with _sessions(adapter_harness, second, StartPosition.latest()) as second_sessions:
                assert (await adapter_harness.publish(first, b"source-one")).accepted
                assert (await adapter_harness.publish(second, b"source-two")).accepted
                assert (await _receive(first_sessions[0])).payload == b"source-one"
                assert (await _receive(second_sessions[0])).payload == b"source-two"


__all__ = ["LifecycleContract", "ReadinessContract", "IsolationContract", "ContractPrecondition"]
