"""Markers select an admission interval without pausing or consuming observation."""

import asyncio
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import threading
import time

import pytest

from event_observer import (
    EventObserver,
    JournalLimits,
    JournalPolicy,
    ObservationMark,
    OverflowPolicy,
    RuntimeLimits,
    ScopeStart,
    SyncEventObserver,
)
from event_observer.errors import (
    ConfigurationError,
    EventTimeoutError,
    IncompleteObservationError,
    ObserverClosedError,
    ObserverFailedError,
    ReentrantSyncCallError,
    ScopeResourceLimitError,
    ThreadAffinityError,
    TransportError,
)
from event_observer.testing import FakeTransport


class FrozenClock:
    def monotonic(self):
        return 100.0

    def utcnow(self):
        return datetime(2026, 11, 2, tzinfo=timezone.utc)


async def admitted(watcher, count):
    async with asyncio.timeout(1):
        while watcher.snapshot().next_sequence < count:
            await asyncio.sleep(0)


def sync_admitted(watcher, count):
    deadline = time.monotonic() + 1
    while watcher.snapshot().next_sequence < count:
        if time.monotonic() >= deadline:
            pytest.fail("The continuously running reader did not admit the test action's events")


def retention(policy):
    return JournalPolicy(JournalLimits(max_events=2, max_bytes=4096, max_event_bytes=1024), policy)


@pytest.mark.parametrize("sync", [False, True])
def test_reusable_mark_excludes_history_but_finds_events_admitted_during_the_action(sync):
    transport = FakeTransport()
    configuration = dict(transport=transport, clock=FrozenClock())

    async def async_case():
        async with EventObserver(**configuration).watch(source="events") as watcher:
            transport.publish(b"old")
            await admitted(watcher, 1)
            mark = watcher.mark()
            assert isinstance(mark, ObservationMark)
            assert mark.sequence == 1
            transport.publish(b"new-one")
            transport.publish(b"new-two")
            await admitted(watcher, 3)  # The action finishes before any marked wait begins.
            first = await watcher.wait_for(lambda _: True, since=mark, timeout=1)
            event = await watcher.wait_for_event(lambda _: True, since=mark, timeout=1)
            values = await watcher.wait_for_count(lambda _: True, 2, since=mark, timeout=1)
            default = await watcher.wait_for(lambda _: True, timeout=1)
            return mark, event, first, values, default

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            transport.publish(b"old")
            sync_admitted(watcher, 1)
            mark = watcher.mark()
            assert isinstance(mark, ObservationMark)
            assert mark.sequence == 1
            transport.publish(b"new-one")
            transport.publish(b"new-two")
            sync_admitted(watcher, 3)
            first = watcher.wait_for(lambda _: True, since=mark, timeout=1)
            event = watcher.wait_for_event(lambda _: True, since=mark, timeout=1)
            values = watcher.wait_for_count(lambda _: True, 2, since=mark, timeout=1)
            default = watcher.wait_for(lambda _: True, timeout=1)
            return mark, event, first, values, default

    mark, event, first, values, default = sync_case() if sync else asyncio.run(async_case())
    assert first == event.payload == b"new-one"
    assert values == [b"new-one", b"new-two"]
    assert default == b"old"
    assert event.sequence == mark.sequence
    # Identical timestamps must not make a pre-mark record eligible or exclude a post-mark record.
    assert event.recorded_monotonic == mark.recorded_monotonic == 100.0
    assert mark.recorded_at == FrozenClock().utcnow()
    with pytest.raises(FrozenInstanceError):
        mark.sequence = 0
    assert transport.open_count == transport.close_count == 1


@pytest.mark.parametrize("sync", [False, True])
def test_scope_marks_preserve_nested_filters_identity_and_scope_quota(sync):
    transport = FakeTransport()
    parent_calls, child_calls = [], []
    configuration = dict(transport=transport, runtime_limits=RuntimeLimits(max_scopes=2))

    def make_scopes(watcher):
        parent = watcher.scope(lambda payload: parent_calls.append(payload) or payload.startswith(b"order:"))
        child = parent.scope(lambda payload: child_calls.append(payload) or payload.endswith(b":completed"))
        mark = child.mark()
        assert not parent_calls and not child_calls
        return child, mark, child.snapshot()

    def publish():
        for payload in (b"other:completed", b"order:started", b"order:completed"):
            transport.publish(payload)

    async def async_case():
        async with EventObserver(**configuration).watch(source="events") as watcher:
            transport.publish(b"order:completed")
            await admitted(watcher, 1)
            child, mark, original = make_scopes(watcher)
            publish()
            await admitted(watcher, 4)
            assert await child.wait_for(lambda _: True, since=mark, timeout=1) == b"order:completed"
            snapshot = child.snapshot()
            # A scope-created mark selects a watcher interval, without implicitly carrying its filters.
            assert await watcher.wait_for(lambda _: True, since=mark, timeout=1) == b"other:completed"
            with pytest.raises(ScopeResourceLimitError):
                watcher.scope(lambda _: True)
            return mark, original, snapshot, child.snapshot().lower_bound

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            transport.publish(b"order:completed")
            sync_admitted(watcher, 1)
            child, mark, original = make_scopes(watcher)
            publish()
            sync_admitted(watcher, 4)
            assert child.wait_for(lambda _: True, since=mark, timeout=1) == b"order:completed"
            snapshot = child.snapshot()
            assert watcher.wait_for(lambda _: True, since=mark, timeout=1) == b"other:completed"
            with pytest.raises(ScopeResourceLimitError):
                watcher.scope(lambda _: True)
            return mark, original, snapshot, child.snapshot().lower_bound

    mark, original, snapshot, lower = sync_case() if sync else asyncio.run(async_case())
    assert mark.sequence == 1
    assert parent_calls == [b"other:completed", b"order:started", b"order:completed"]
    assert child_calls == [b"order:started", b"order:completed"]
    assert snapshot.scope_id == original.scope_id
    assert lower == snapshot.lower_bound == original.lower_bound == 0
    assert snapshot.filter_count == 2
    [expectation] = snapshot.expectations
    assert expectation.scope_id == original.scope_id
    assert expectation.lower_bound == mark.sequence
    assert transport.open_count == transport.close_count == 1


@pytest.mark.parametrize("sync", [False, True])
def test_an_earlier_marker_cannot_expand_a_narrowed_parent_scope(sync):
    transport = FakeTransport()

    async def async_case():
        async with EventObserver(transport=transport).watch(source="events") as watcher:
            earlier = watcher.mark()
            transport.publish(b"old")
            await admitted(watcher, 1)
            parent = watcher.scope(lambda _: True, start=ScopeStart.from_now())
            child = parent.scope(lambda _: True, start=ScopeStart.from_sequence(0))
            transport.publish(b"new")
            await admitted(watcher, 2)
            assert await child.wait_for(lambda _: True, since=earlier, timeout=1) == b"new"
            assert (
                child.diagnostics.snapshot().expectations[-1].lower_bound == child.snapshot().lower_bound == 1
            )

    def sync_case():
        with SyncEventObserver(transport=transport).watch(source="events") as watcher:
            earlier = watcher.mark()
            transport.publish(b"old")
            sync_admitted(watcher, 1)
            parent = watcher.scope(lambda _: True, start=ScopeStart.from_now())
            child = parent.scope(lambda _: True, start=ScopeStart.from_sequence(0))
            transport.publish(b"new")
            sync_admitted(watcher, 2)
            assert child.wait_for(lambda _: True, since=earlier, timeout=1) == b"new"
            assert (
                child.diagnostics.snapshot().expectations[-1].lower_bound == child.snapshot().lower_bound == 1
            )

    sync_case() if sync else asyncio.run(async_case())


@pytest.mark.parametrize("sync", [False, True])
def test_wrong_type_foreign_and_future_markers_are_configuration_errors(sync):
    transport = FakeTransport()
    factory = SyncEventObserver(transport=transport) if sync else EventObserver(transport=transport)

    async def async_case():
        async with factory.watch(source="events") as first:
            foreign = first.mark()
        async with factory.watch(source="events") as watcher:
            own = watcher.mark()
            invalid = (0, 1.0, True, datetime.now(timezone.utc), object(), foreign, replace(own, sequence=1))
            for mark in invalid:
                with pytest.raises(ConfigurationError):
                    await watcher.wait_for(lambda _: True, since=mark, timeout=0.01)
            transport.publish(b"usable")
            assert await watcher.wait_for(lambda _: True, since=own, timeout=1) == b"usable"

    def sync_case():
        with factory.watch(source="events") as first:
            foreign = first.mark()
        with factory.watch(source="events") as watcher:
            own = watcher.mark()
            invalid = (0, 1.0, True, datetime.now(timezone.utc), object(), foreign, replace(own, sequence=1))
            for mark in invalid:
                with pytest.raises(ConfigurationError):
                    watcher.wait_for(lambda _: True, since=mark, timeout=0.01)
            transport.publish(b"usable")
            assert watcher.wait_for(lambda _: True, since=own, timeout=1) == b"usable"

    sync_case() if sync else asyncio.run(async_case())


@pytest.mark.parametrize("sync", [False, True])
def test_mark_requires_an_active_watcher_and_remains_readable_after_close(sync):
    transport = FakeTransport()

    async def async_case():
        watcher = EventObserver(transport=transport).watch(source="events")
        with pytest.raises(ObserverClosedError):
            watcher.mark()
        async with watcher:
            mark = watcher.mark()
        with pytest.raises(ObserverClosedError):
            watcher.mark()
        return mark

    def sync_case():
        watcher = SyncEventObserver(transport=transport).watch(source="events")
        with pytest.raises(ObserverClosedError):
            watcher.mark()
        with watcher:
            mark = watcher.mark()
        with pytest.raises(ObserverClosedError):
            watcher.mark()
        return mark

    mark = sync_case() if sync else asyncio.run(async_case())
    assert mark.sequence == 0
    assert mark.observer_id
    assert mark.recorded_at.utcoffset().total_seconds() == 0
    assert transport.open_count == transport.close_count == 1


@pytest.mark.parametrize("sync", [False, True])
def test_losses_before_the_mark_do_not_poison_the_new_complete_interval(sync):
    transport = FakeTransport()
    configuration = dict(transport=transport, journal=retention(OverflowPolicy.DROP_OLDEST))

    def publish_old():
        for payload in (b"old-zero", b"old-one", b"old-two"):
            transport.publish(payload)

    async def async_case():
        async with EventObserver(**configuration).watch(source="events") as watcher:
            publish_old()
            await admitted(watcher, 3)
            assert not watcher.snapshot().complete
            mark = watcher.mark()
            transport.publish(b"new")
            await admitted(watcher, 4)
            assert await watcher.wait_for(lambda _: True, since=mark, timeout=1) == b"new"
            assert watcher.diagnostics.snapshot().expectations[-1].complete
            with pytest.raises(IncompleteObservationError):
                await watcher.wait_for(lambda _: True, timeout=1)

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            publish_old()
            sync_admitted(watcher, 3)
            assert not watcher.snapshot().complete
            mark = watcher.mark()
            transport.publish(b"new")
            sync_admitted(watcher, 4)
            assert watcher.wait_for(lambda _: True, since=mark, timeout=1) == b"new"
            assert watcher.diagnostics.snapshot().expectations[-1].complete
            with pytest.raises(IncompleteObservationError):
                watcher.wait_for(lambda _: True, timeout=1)

    sync_case() if sync else asyncio.run(async_case())


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("overflow", [OverflowPolicy.DROP_OLDEST, OverflowPolicy.DROP_NEWEST])
def test_loss_after_a_mark_blocks_strict_success_but_allows_explicit_incomplete_search(sync, overflow):
    transport = FakeTransport()
    configuration = dict(transport=transport, journal=retention(overflow))
    retained = b"new-two" if overflow is OverflowPolicy.DROP_OLDEST else b"new-one"

    def action():
        for payload in (b"new-one", b"new-two", b"new-three"):
            transport.publish(payload)

    async def async_case():
        async with EventObserver(**configuration).watch(source="events") as watcher:
            transport.publish(b"old")
            await admitted(watcher, 1)
            mark = watcher.mark()
            action()
            await admitted(watcher, 4)
            with pytest.raises(IncompleteObservationError):
                await watcher.wait_for(lambda _: True, since=mark, timeout=1)
            observed = await watcher.wait_for_event(
                lambda _: True, since=mark, timeout=1, require_complete=False
            )
            assert observed.payload == retained
            assert observed.sequence >= mark.sequence
            assert watcher.diagnostics.snapshot().expectations[-1].lower_bound == mark.sequence

    def sync_case():
        with SyncEventObserver(**configuration).watch(source="events") as watcher:
            transport.publish(b"old")
            sync_admitted(watcher, 1)
            mark = watcher.mark()
            action()
            sync_admitted(watcher, 4)
            with pytest.raises(IncompleteObservationError):
                watcher.wait_for(lambda _: True, since=mark, timeout=1)
            observed = watcher.wait_for_event(lambda _: True, since=mark, timeout=1, require_complete=False)
            assert observed.payload == retained
            assert observed.sequence >= mark.sequence
            assert watcher.diagnostics.snapshot().expectations[-1].lower_bound == mark.sequence

    sync_case() if sync else asyncio.run(async_case())


@pytest.mark.asyncio
async def test_a_mark_after_drop_newest_can_select_an_empty_complete_range():
    transport = FakeTransport()
    async with EventObserver(transport=transport, journal=retention(OverflowPolicy.DROP_NEWEST)).watch(
        source="events"
    ) as watcher:
        for payload in (b"one", b"two", b"dropped"):
            transport.publish(payload)
        await admitted(watcher, 3)
        mark = watcher.mark()
        with pytest.raises(EventTimeoutError) as caught:
            await watcher.wait_for(lambda _: True, since=mark, timeout=0.01)
        assert caught.value.diagnostics.expectations[-1].complete
        assert caught.value.diagnostics.expectations[-1].lower_bound == mark.sequence


def test_sync_mark_preserves_thread_affinity_and_reentrant_call_guards():
    transport = FakeTransport()
    with SyncEventObserver(transport=transport).watch(source="events") as watcher:
        mark = watcher.mark()
        errors = []

        def foreign_thread():
            try:
                watcher.mark()
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=foreign_thread)
        thread.start()
        thread.join(1)
        assert not thread.is_alive()
        assert len(errors) == 1 and isinstance(errors[0], ThreadAffinityError)

        def callback(payload):
            with pytest.raises(ReentrantSyncCallError):
                watcher.mark()
            return True

        transport.publish(b"new")
        assert watcher.wait_for(callback, since=mark, timeout=1) == b"new"


@pytest.mark.parametrize("sync", [False, True])
def test_mark_after_failed_close_preserves_the_terminal_failure_for_watcher_and_scope(sync):
    transport = FakeTransport()
    failure = TransportError("Injected terminal failure")

    def assert_failed_mark(view, observer_id):
        with pytest.raises(ObserverFailedError) as caught:
            view.mark()
        assert caught.value.cause is caught.value.__cause__ is failure
        assert caught.value.observer_id == observer_id
        assert caught.value.diagnostics is not None

    async def async_case():
        with pytest.raises(ObserverFailedError):
            async with EventObserver(transport=transport).watch(source="events") as watcher:
                scope = watcher.scope(lambda _: True)
                transport.fail(failure)
                with pytest.raises(ObserverFailedError):
                    await watcher.wait_for(lambda _: True, timeout=1)
        assert_failed_mark(watcher, watcher.observer_id)
        assert_failed_mark(scope, watcher.observer_id)

    def sync_case():
        with pytest.raises(ObserverFailedError):
            with SyncEventObserver(transport=transport).watch(source="events") as watcher:
                scope = watcher.scope(lambda _: True)
                transport.fail(failure)
                with pytest.raises(ObserverFailedError):
                    watcher.wait_for(lambda _: True, timeout=1)
        assert_failed_mark(watcher, watcher.observer_id)
        assert_failed_mark(scope, watcher.observer_id)

    sync_case() if sync else asyncio.run(async_case())
    assert transport.close_count == 1


@pytest.mark.parametrize(
    ("recorded_at", "recorded_monotonic"),
    [
        pytest.param(datetime(2026, 11, 2, tzinfo=timezone.utc), 10**1000, id="monotonic-overflow"),
        pytest.param(datetime.min.replace(tzinfo=timezone(timedelta(hours=1))), 0.0, id="utc-underflow"),
        pytest.param(datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))), 0.0, id="utc-overflow"),
    ],
)
def test_unrepresentable_mark_times_raise_configuration_error(recorded_at, recorded_monotonic):
    with pytest.raises(ConfigurationError):
        ObservationMark("watcher", 0, recorded_at, recorded_monotonic)
