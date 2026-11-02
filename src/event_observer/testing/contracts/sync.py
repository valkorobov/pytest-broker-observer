"""Reusable blocking adapter contracts with a ``sync_adapter_harness`` fixture.

Inherit SyncLifecycleContract, SyncReadinessContract, and SyncIsolationContract.
Every case is an ordinary test function using only blocking transport methods.
Source creation and publication belong to an independent test-side harness;
its implementation is outside the observer runtime. Optional cases use the
same explicit capability applicability rules as the asynchronous contracts.

:ivar _STARTUP_SECONDS: Absolute native preparation budget supplied to each session.
:ivar _RECEIVE_SECONDS: Maximum relative wait for a confirmed independent publication.
:ivar _CLOSE_SECONDS: Native cleanup budget supplied to every opened session.
:ivar __all__: Blocking harness protocol and the three reusable contract classes.
"""

from __future__ import annotations

from contextlib import contextmanager
import time
from typing import Callable, Iterator, Protocol
from uuid import uuid4

import pytest

from ...config import TransportBufferLimits
from ...errors import ConfigurationError, TransportSubscriptionError
from ...models import OpenContext, RawEvent, StartPosition, TransportSource
from ...transports.base import DeliveryOutcome
from ...transports.sync_base import SyncTransportAdapter, SyncTransportSession
from ..harness import PublishReceipt
from ._capabilities import ContractPrecondition, require_capability

_STARTUP_SECONDS = 10.0
_RECEIVE_SECONDS = 5.0
_CLOSE_SECONDS = 5.0


class SyncAdapterTestHarness(Protocol):
    """Own independent test resources and publication for blocking contract cases.

    :ivar adapter: Blocking adapter whose declared capabilities determine optional applicability.
    """

    @property
    def adapter(self) -> SyncTransportAdapter:
        """Expose the lazy blocking adapter without opening a session.

        :returns: A structurally compatible blocking adapter under contract test.
        """
        ...

    def create_source(self, *, case_id: str) -> TransportSource:
        """Prepare a unique fixture-owned source without using the observation reader.

        :param case_id: Unique case identifier used to name and track the source.
        :returns: An existing source ready for independent observation.
        """
        ...

    def publish(
        self,
        source: TransportSource,
        payload: bytes | None,
        *,
        key: bytes | None = None,
        headers: tuple[tuple[str, bytes | None], ...] = (),
    ) -> PublishReceipt:
        """Publish through an independent test-side path and confirm broker acceptance.

        :param source: Fixture-owned source receiving the publication.
        :param payload: Raw bytes or declared transport-supported null body.
        :param key: Optional native message key, subject to declared capabilities.
        :param headers: Ordered normalized header pairs supported by this transport.
        :returns: A detached receipt confirming native publication acceptance.
        """
        ...

    def cleanup(self) -> None:
        """Release only fixture-owned sources, including after a failed contract case.

        :returns: None after all fixture-owned resources have been released.
        """
        ...


def _require(
    harness: SyncAdapterTestHarness, precondition: ContractPrecondition, record_property: Callable
) -> None:
    """Apply the shared capability rule to a blocking test harness.

    :param harness: Blocking harness exposing static adapter capabilities.
    :param precondition: Optional semantics required by the current contract node.
    :param record_property: Pytest recorder receiving identical applicability metadata.
    :returns: None when the optional capability is declared.
    :raises pytest.skip.Exception: If the capability is explicitly NOT_APPLICABLE.
    """
    require_capability(harness.adapter.capabilities, precondition, record_property)


def _source(harness: SyncAdapterTestHarness) -> TransportSource:
    """Prepare one uniquely named source through the independent fixture boundary.

    :param harness: Harness responsible for source creation and later fixture cleanup.
    :returns: An existing isolated source for this contract case.
    """
    return harness.create_source(case_id=f"blocking-contract-{uuid4().hex}")


def _context(*, max_events: int = 256) -> OpenContext:
    """Build a finite opening context in the same system-clock domain as the contract.

    :param max_events: Event handoff capacity; a single-slot case exercises settlement flow control.
    :returns: Fresh observer identity, absolute deadline, and bounded transfer settings.
    """
    return OpenContext(
        uuid4().hex, time.monotonic() + _STARTUP_SECONDS, TransportBufferLimits(max_events=max_events)
    )


@contextmanager
def _sessions(
    harness: SyncAdapterTestHarness,
    source: TransportSource,
    *starts: StartPosition,
    max_events: int = 256,
) -> Iterator[list[SyncTransportSession]]:
    """Open independent blocking sessions and close all allocated handles in reverse order.

    :param harness: Independent blocking adapter factory under test.
    :param source: Fixture-owned existing source selected for observation.
    :param starts: One native starting policy per requested independent reader.
    :param max_events: Transfer capacity supplied to each session.
    :yields: Successfully opened READY sessions while the contract body executes.
    :raises BaseException: If opening, the contract body, or cleanup fails; body errors stay primary.
    """
    opened = []
    primary = None
    try:
        for start in starts:
            opened.append(harness.adapter.open(source, start, context=_context(max_events=max_events)))
        yield opened
    except BaseException as error:
        primary = error
        raise
    finally:
        cleanup = None
        for session in reversed(opened):
            try:
                session.close(deadline=time.monotonic() + _CLOSE_SECONDS)
            except BaseException as error:
                if cleanup is None:
                    cleanup = error
        if cleanup is not None:
            if primary is not None:
                primary.add_note("A blocking contract session also failed to close")
            else:
                raise cleanup


def _settle(session: SyncTransportSession, event: RawEvent) -> None:
    """Commit acceptance of a verified contract envelope through declared native settlement.

    :param session: Reader advertising its resolved settlement capability.
    :param event: Normalized event accepted by this contract's simulated Core boundary.
    :returns: None after explicit acknowledgement, or immediately when settlement is unsupported.
    """
    if session.readiness.resolved_capabilities.supports_settlement:
        assert isinstance(event.delivery_id, str) and event.delivery_id
        settle = getattr(session, "settle", None)
        assert callable(settle), "Declared settlement requires a blocking settle method"
        settle(event.delivery_id, DeliveryOutcome.ADMITTED)


def _receive(session: SyncTransportSession, *, settle: bool = True) -> RawEvent:
    """Poll until one normalized envelope arrives under a single finite deadline.

    :param session: Independent blocking reader whose failures take precedence over queued data.
    :param settle: Whether to declare safe acceptance of the normalized contract envelope immediately.
    :returns: One RawEvent detached by the transport; acceptance uses only the declared settlement API.
    :raises TimeoutError: If no event arrives within the receive budget.
    :raises Exception: If the session reports a terminal transport failure.
    """
    deadline = time.monotonic() + _RECEIVE_SECONDS
    while (remaining := deadline - time.monotonic()) > 0:
        error = session.failure()
        if error is not None:
            raise error
        event = session.poll(min(0.1, remaining))
        if event is not None:
            assert isinstance(event, RawEvent)
            if settle:
                _settle(session, event)
            return event
    raise TimeoutError("A confirmed contract publication was not observed before its receive deadline")


@pytest.mark.contract
class SyncLifecycleContract:
    """Verify READY, bounded polling, explicit settlement, failure probes, and idempotent close."""

    def test_lifecycle_open_has_resolved_readiness(self, sync_adapter_harness: SyncAdapterTestHarness):
        """Require an existing source and declared independent isolation before returning READY.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            ready = sessions[0].readiness
            assert ready.boundary.source == source.name
            assert ready.boundary.start_mode == "latest"
            assert ready.ready_monotonic <= time.monotonic()
            assert ready.resolved_capabilities.isolation_modes.intersection(
                {"independent_reader", "dedicated_subscription"}
            )

    def test_lifecycle_poll_is_bounded_and_declared_settlement_is_safe(
        self, sync_adapter_harness: SyncAdapterTestHarness
    ):
        """Reject invalid poll budgets and exercise safe acknowledgement with one transfer slot.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest(), max_events=1) as sessions:
            session = sessions[0]
            began = time.monotonic()
            assert session.poll(0) is None
            assert time.monotonic() - began < _RECEIVE_SECONDS
            for invalid in (-1, True, float("inf"), float("nan")):
                with pytest.raises(ConfigurationError):
                    session.poll(invalid)
            assert sync_adapter_harness.publish(source, b"first").accepted
            first = _receive(session, settle=False)
            assert first.payload == b"first"
            _settle(session, first)
            if session.readiness.resolved_capabilities.supports_settlement:
                with pytest.raises((TransportSubscriptionError, ConfigurationError)):
                    getattr(session, "settle")(first.delivery_id, DeliveryOutcome.ADMITTED)
            assert sync_adapter_harness.publish(source, b"after-acceptance").accepted
            assert _receive(session).payload == b"after-acceptance"

    def test_lifecycle_close_is_idempotent(self, sync_adapter_harness: SyncAdapterTestHarness):
        """Close one owned reader repeatedly without orphaned or shared resource operations.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            for _ in range(2):
                sessions[0].close(deadline=time.monotonic() + _CLOSE_SECONDS)

    def test_lifecycle_close_releases_failure_signal(self, sync_adapter_harness: SyncAdapterTestHarness):
        """Confirm that healthy intentional close resolves idle failure inspection immediately.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            session = sessions[0]
            assert session.failure() is None
            session.close(deadline=time.monotonic() + _CLOSE_SECONDS)
            assert session.failure() is None
            assert session.poll(0) is None

    def test_lifecycle_failure_probes_do_not_consume_or_damage_deliveries(
        self, sync_adapter_harness: SyncAdapterTestHarness
    ):
        """Inspect failure repeatedly without blocking or removing the reader's accepted publication.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            session = sessions[0]
            assert sync_adapter_harness.publish(source, b"after-probes").accepted
            began = time.monotonic()
            for _ in range(3):
                assert session.failure() is None
            assert time.monotonic() - began < _RECEIVE_SECONDS
            assert _receive(session).payload == b"after-probes"


@pytest.mark.contract
class SyncReadinessContract:
    """Verify empty-source preparation, native starts, and declared envelope preservation."""

    def test_readiness_empty_source_needs_no_probe_message(
        self, sync_adapter_harness: SyncAdapterTestHarness
    ):
        """Reach READY on an empty source without a synthetic business publication.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            assert sessions[0].readiness.boundary.source == source.name
            assert sessions[0].poll(0) is None

    def test_readiness_immediate_publish_is_observed(self, sync_adapter_harness: SyncAdapterTestHarness):
        """Observe a confirmed independent publication immediately after preparation returns.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            assert sync_adapter_harness.publish(source, b"immediately-after-ready").accepted
            event = _receive(sessions[0])
            assert event.payload == b"immediately-after-ready"
            assert event.source == source.name

    def test_readiness_latest_excludes_published_history(self, sync_adapter_harness: SyncAdapterTestHarness):
        """Exclude pre-open history while preserving a publication after the latest boundary.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        assert sync_adapter_harness.publish(source, b"before-boundary").accepted
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            assert sync_adapter_harness.publish(source, b"after-boundary").accepted
            assert _receive(sessions[0]).payload == b"after-boundary"

    def test_readiness_earliest_replays_available_history(
        self, sync_adapter_harness: SyncAdapterTestHarness, record_property
    ):
        """Replay accepted history only when the adapter declares earliest support.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        :param record_property: Pytest recorder receiving explicit capability applicability metadata.
        """
        _require(sync_adapter_harness, ContractPrecondition("start_modes", "earliest"), record_property)
        source = _source(sync_adapter_harness)
        assert sync_adapter_harness.publish(source, b"available-history").accepted
        with _sessions(sync_adapter_harness, source, StartPosition.earliest()) as sessions:
            assert _receive(sessions[0]).payload == b"available-history"

    def test_readiness_null_payload_is_distinct_from_empty_bytes(
        self, sync_adapter_harness: SyncAdapterTestHarness, record_property
    ):
        """Preserve the difference between declared null bodies and empty byte bodies.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        :param record_property: Pytest recorder receiving explicit capability applicability metadata.
        """
        _require(sync_adapter_harness, ContractPrecondition("supports_null_payload", True), record_property)
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            assert sync_adapter_harness.publish(source, None).accepted
            assert _receive(sessions[0]).payload is None
            assert sync_adapter_harness.publish(source, b"").accepted
            assert _receive(sessions[0]).payload == b""

    def test_readiness_preserves_duplicate_and_null_headers(
        self, sync_adapter_harness: SyncAdapterTestHarness, record_property
    ):
        """Preserve declared duplicate, null-valued, and empty native header pairs.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        :param record_property: Pytest recorder receiving explicit capability applicability metadata.
        """
        for capability in ("supports_headers", "supports_duplicate_headers", "supports_null_header_values"):
            _require(sync_adapter_harness, ContractPrecondition(capability, True), record_property)
        source = _source(sync_adapter_harness)
        headers = (("duplicate", b"first"), ("duplicate", None), ("empty", b""))
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            assert sync_adapter_harness.publish(source, b"headers", headers=headers).accepted
            assert _receive(sessions[0]).headers == headers

    def test_readiness_preserves_message_key(
        self, sync_adapter_harness: SyncAdapterTestHarness, record_property
    ):
        """Preserve native message keys only when the transport advertises that domain.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        :param record_property: Pytest recorder receiving explicit capability applicability metadata.
        """
        _require(sync_adapter_harness, ContractPrecondition("supports_key", True), record_property)
        source = _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, source, StartPosition.latest()) as sessions:
            assert sync_adapter_harness.publish(source, b"keyed", key=b"observation-key").accepted
            assert _receive(sessions[0]).key == b"observation-key"


@pytest.mark.contract
class SyncIsolationContract:
    """Verify independent readers, independent closure, and source routing separation."""

    def test_isolation_sessions_both_observe_one_publication(
        self, sync_adapter_harness: SyncAdapterTestHarness
    ):
        """Deliver one publication independently to two sessions over the same source.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(
            sync_adapter_harness, source, StartPosition.latest(), StartPosition.latest()
        ) as sessions:
            assert sessions[0] is not sessions[1]
            assert sync_adapter_harness.publish(source, b"independent-readers").accepted
            assert [_receive(session).payload for session in sessions] == [b"independent-readers"] * 2

    def test_isolation_closing_one_session_preserves_another(
        self, sync_adapter_harness: SyncAdapterTestHarness
    ):
        """Close one observer without damaging another subscription or native cursor.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        source = _source(sync_adapter_harness)
        with _sessions(
            sync_adapter_harness, source, StartPosition.latest(), StartPosition.latest()
        ) as sessions:
            sessions[0].close(deadline=time.monotonic() + _CLOSE_SECONDS)
            assert sync_adapter_harness.publish(source, b"surviving-reader").accepted
            assert _receive(sessions[1]).payload == b"surviving-reader"

    def test_isolation_sources_do_not_cross_deliver(self, sync_adapter_harness: SyncAdapterTestHarness):
        """Route each source's publication exclusively to its independent source reader.

        :param sync_adapter_harness: Function-scoped harness owning source creation, publication, and cleanup.
        """
        first, second = _source(sync_adapter_harness), _source(sync_adapter_harness)
        with _sessions(sync_adapter_harness, first, StartPosition.latest()) as first_sessions:
            with _sessions(sync_adapter_harness, second, StartPosition.latest()) as second_sessions:
                assert sync_adapter_harness.publish(first, b"first-source").accepted
                assert sync_adapter_harness.publish(second, b"second-source").accepted
                assert _receive(first_sessions[0]).payload == b"first-source"
                assert _receive(second_sessions[0]).payload == b"second-source"


__all__ = [
    "SyncAdapterTestHarness",
    "SyncLifecycleContract",
    "SyncReadinessContract",
    "SyncIsolationContract",
]
