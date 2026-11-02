"""Deterministic regression checks for runtime boundaries and resource cleanup."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
import threading
import time
from types import SimpleNamespace

import pytest

from event_observer import (
    EventObserver,
    JournalLimits,
    JournalPolicy,
    LifecyclePolicy,
    OverflowPolicy,
    RawEvent,
    RuntimeLimits,
    ScopeStart,
    StartPosition,
    SyncEventObserver,
)
from event_observer.errors import (
    EventTimeoutError,
    IncompleteObservationError,
    ObserverFailedError,
    ObserverStartupTimeoutError,
    ProcessAffinityError,
    TransportError,
)
from event_observer.models import ObservedEvent
from event_observer.testing import FakeTransport


async def admitted(watcher, count):
    async with asyncio.timeout(1.0):
        while watcher.snapshot().next_sequence < count:
            await asyncio.sleep(0)


class ManualClock:
    def __init__(self, value=100.0):
        self.value = value

    def monotonic(self):
        return self.value

    def utcnow(self):
        return datetime(2026, 11, 1, tzinfo=timezone.utc)


class VariableSizeAccess:
    def clone_for_user(self, payload):
        return payload  # bytes are immutable

    def estimate_bytes(self, payload):
        return 8192 if payload == b"drop-decoded" else len(payload or b"")


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize("stage", ["open", "initial_receive"])
def test_core_rejects_startup_that_exceeds_its_shared_clock_deadline(sync, stage):
    clock = ManualClock()
    lifecycle = LifecyclePolicy(startup_timeout=0.5)
    entered = []
    records = []

    class LateTransport(FakeTransport):
        async def open(self, source, start, *, context):
            session = await super().open(source, start, context=context)
            self.startup_deadline = context.startup_deadline
            if stage == "open":
                clock.value = context.startup_deadline + 1
            return session

        def sync_adapter(self):
            adapter = super().sync_adapter()
            original_open = adapter.open

            def open(source, start, *, context):
                session = original_open(source, start, context=context)
                self.startup_deadline = context.startup_deadline
                if stage == "open":
                    clock.value = context.startup_deadline + 1
                return session

            adapter.open = open
            return adapter

    class Decoder:
        def decode(self, raw):
            if stage == "initial_receive":
                clock.value = adapter.startup_deadline + 1
            return raw.payload

    adapter = LateTransport((RawEvent(b"startup event", "events"),))
    factory = SyncEventObserver if sync else EventObserver
    watcher = factory(transport=adapter, clock=clock, lifecycle=lifecycle).watch(
        source="events", start=StartPosition.earliest(), decoder=Decoder()
    )

    def sync_case():
        try:
            with watcher:
                entered.append(True)
        finally:
            records.extend(watcher.diagnostics.snapshot().records)

    async def async_case():
        try:
            async with watcher:
                entered.append(True)
        finally:
            records.extend(watcher.diagnostics.snapshot().records)

    with pytest.raises(ObserverStartupTimeoutError) as caught:
        sync_case() if sync else asyncio.run(async_case())
    assert entered == []
    assert isinstance(caught.value.__cause__, TimeoutError)
    assert caught.value.cause is caught.value.__cause__
    assert adapter.open_count == adapter.close_count == 1
    assert adapter.sessions[0].closed
    assert not any(record.kind == "ready" for record in records)
    if sync:
        assert not watcher._runtime.thread.is_alive()
    else:
        assert all(task.done() for task in watcher._tasks)


@pytest.mark.asyncio
async def test_caller_cancellation_keeps_priority_when_async_open_also_expires():
    clock = ManualClock()

    class CancelledLateTransport(FakeTransport):
        async def open(self, source, start, *, context):
            session = await super().open(source, start, context=context)
            clock.value = context.startup_deadline + 1
            asyncio.current_task().cancel("Caller interrupted startup")
            return session

    adapter = CancelledLateTransport()
    watcher = EventObserver(transport=adapter, clock=clock).watch(source="events")
    with pytest.raises(asyncio.CancelledError, match="Caller interrupted startup"):
        await asyncio.create_task(watcher.__aenter__())
    assert adapter.open_count == adapter.close_count == 1
    assert adapter.sessions[0].closed
    assert not any(record.kind == "ready" for record in watcher.diagnostics.snapshot().records)


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_loss_ledger_compaction_never_restores_old_completeness():
    adapter = FakeTransport()
    policy = JournalPolicy(
        JournalLimits(max_events=16, max_bytes=4096, max_event_bytes=1024, max_loss_ranges=2),
        OverflowPolicy.DROP_NEWEST,
    )
    async with EventObserver(transport=adapter, journal=policy).watch(
        source="events", payload_access=VariableSizeAccess()
    ) as watcher:
        for payload in (
            b"keep-0",
            b"drop-decoded",
            b"keep-2",
            b"drop-decoded",
            b"keep-4",
            b"drop-decoded",
            b"keep-6",
        ):
            adapter.publish(payload)
        await admitted(watcher, 7)
        snapshot = watcher.snapshot()
        assert len(snapshot.losses) == 2
        assert snapshot.losses[0].reason == "conservative"
        assert snapshot.losses[0].first_sequence == 1
        assert snapshot.losses[0].last_sequence == 3
        assert not snapshot.complete
        with pytest.raises(IncompleteObservationError):
            await watcher.wait_for(lambda payload: payload == b"keep-2", timeout=0.1)
        narrowed = watcher.scope(lambda payload: True, start=ScopeStart.from_sequence(2))
        with pytest.raises(IncompleteObservationError):
            await narrowed.wait_for(lambda payload: payload == b"keep-2", timeout=0.1)
        fresh = watcher.scope(lambda payload: True, start=ScopeStart.from_now())
        assert fresh.lower_bound == 7
        assert fresh.snapshot().complete
        adapter.publish(b"fresh")
        assert await fresh.wait_for(lambda payload: payload == b"fresh", timeout=0.1) == b"fresh"
        assert not watcher.snapshot().complete


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_append_between_empty_scan_and_sleep_cannot_lose_wakeup(monkeypatch):
    adapter = FakeTransport()
    async with EventObserver(transport=adapter).watch(source="events") as watcher:
        original_batch = watcher._journal.batch
        injected = False

        def scan_then_append(lower, limit, upper=None):
            nonlocal injected
            batch = original_batch(lower, limit, upper)
            if not injected and not batch:
                injected = True
                # This hook places admission exactly after the empty scan and
                # before the engine's version/notification check, without sleeps.
                event = ObservedEvent(
                    watcher.observer_id,
                    watcher._journal.next_sequence,
                    b"between-scan-and-sleep",
                    RawEvent(b"between-scan-and-sleep", "events"),
                    datetime.now(timezone.utc),
                    time.monotonic(),
                )
                watcher._journal.append(event, 512)
                watcher._notify()
            return batch

        monkeypatch.setattr(watcher._journal, "batch", scan_then_append)
        assert await watcher.wait_for(lambda payload: True, timeout=0.05) == b"between-scan-and-sleep"
        assert injected
        assert watcher.snapshot().next_sequence == 1


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_known_failure_in_matcher_prevents_result_commit():
    adapter = FakeTransport()
    error = TransportError("injected known failure")
    with pytest.raises(ObserverFailedError) as exit_error:
        async with EventObserver(transport=adapter).watch(source="events") as watcher:
            adapter.publish(b"candidate")
            await admitted(watcher, 1)

            def matcher(payload):
                watcher._fail(error)
                return True

            with pytest.raises(ObserverFailedError) as expectation_error:
                await watcher.wait_for(matcher, timeout=0.1)
            assert expectation_error.value.cause is error
            assert watcher._result_bytes == 0
            assert not watcher._waiter_tasks
    assert exit_error.value.cause is error
    assert adapter.close_count == 1


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_known_failure_has_priority_over_matcher_error():
    adapter = FakeTransport()
    error = TransportError("known before matcher returns")
    with pytest.raises(ObserverFailedError):
        async with EventObserver(transport=adapter).watch(source="events") as watcher:
            adapter.publish(b"candidate")
            await admitted(watcher, 1)

            def matcher(payload):
                watcher._fail(error)
                raise ValueError("callback failed after observation failed")

            with pytest.raises(ObserverFailedError) as caught:
                await watcher.wait_for(matcher, timeout=0.1)
            assert caught.value.cause is error


@pytest.mark.asyncio
@pytest.mark.reliability
@pytest.mark.parametrize("wanted", [b"before", b"on", b"after"])
async def test_deadline_accepts_pre_and_exact_boundary_but_rejects_late_admission(wanted):
    clock = ManualClock()
    adapter = FakeTransport()
    limits = RuntimeLimits(scan_batch_events=1)
    async with EventObserver(transport=adapter, clock=clock, runtime_limits=limits).watch(
        source="events"
    ) as watcher:
        deadline = 101.0
        for index, (when, payload) in enumerate(
            ((100.999, b"before"), (101.0, b"on"), (101.001, b"after")), start=1
        ):
            clock.value = when
            adapter.publish(payload)
            await admitted(watcher, index)
        if wanted == b"after":
            with pytest.raises(EventTimeoutError) as caught:
                await watcher._expect(
                    watcher, lambda payload: payload == wanted, 1, 0.1, None, True, absolute_deadline=deadline
                )
            assert caught.value.diagnostics.expectations[-1].deadline_overrun > 0
        else:
            result = await watcher._expect(
                watcher, lambda payload: payload == wanted, 1, 0.1, None, True, absolute_deadline=deadline
            )
            assert result[0].payload == wanted
            assert result[0].recorded_monotonic <= deadline
            assert result[0].recorded_at == clock.utcnow()


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_late_loss_does_not_invalidate_eligible_earlier_result():
    clock = ManualClock()
    adapter = FakeTransport()
    policy = JournalPolicy(
        JournalLimits(max_events=1, max_bytes=4096, max_event_bytes=1024), OverflowPolicy.DROP_NEWEST
    )
    async with EventObserver(transport=adapter, clock=clock, journal=policy).watch(
        source="events"
    ) as watcher:
        clock.value = 100.5
        adapter.publish(b"eligible")
        await admitted(watcher, 1)
        clock.value = 101.5
        adapter.publish(b"late-and-dropped")
        await admitted(watcher, 2)
        assert not watcher.snapshot().complete
        result = await watcher._expect(
            watcher, lambda payload: True, 1, 0.1, None, True, absolute_deadline=101.0
        )
        assert result[0].payload == b"eligible"


@pytest.mark.reliability
def test_one_hundred_sync_lifecycles_leave_no_runtime_threads():
    before = {thread.ident for thread in threading.enumerate() if thread.name == "event-observer-sync"}
    adapter = FakeTransport()
    observer = SyncEventObserver(transport=adapter)
    for index in range(100):
        with observer.watch(source="events") as watcher:
            expected = f"event-{index}".encode()
            adapter.publish(expected)
            assert watcher.wait_for(lambda payload: payload == expected, timeout=0.2) == expected
        assert watcher._runtime.thread is not None
        assert not watcher._runtime.thread.is_alive()
        assert watcher.snapshot().next_sequence == 1
    assert adapter.open_count == adapter.close_count == 100
    after = {thread.ident for thread in threading.enumerate() if thread.name == "event-observer-sync"}
    assert after == before


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_active_async_watcher_rejects_foreign_process(monkeypatch):
    import event_observer.observer as observer_module

    adapter = FakeTransport()
    async with EventObserver(transport=adapter).watch(source="events") as watcher:
        with monkeypatch.context() as patch:
            patch.setattr(observer_module, "os", SimpleNamespace(getpid=lambda: os.getpid() + 1))
            with pytest.raises(ProcessAffinityError):
                watcher.snapshot()
            with pytest.raises(ProcessAffinityError):
                await watcher.wait_for(lambda payload: True, timeout=0.1)
        adapter.publish(b"still-usable")
        assert await watcher.wait_for(lambda payload: True, timeout=0.1) == b"still-usable"


@pytest.mark.reliability
def test_active_sync_handle_rejects_foreign_process(monkeypatch):
    import event_observer._runtime.threaded as sync_module

    adapter = FakeTransport()
    with SyncEventObserver(transport=adapter).watch(source="events") as watcher:
        with monkeypatch.context() as patch:
            patch.setattr(sync_module, "os", SimpleNamespace(getpid=lambda: os.getpid() + 1))
            with pytest.raises(ProcessAffinityError):
                watcher.snapshot()
        adapter.publish(b"still-usable")
        assert watcher.wait_for(lambda payload: True, timeout=0.1) == b"still-usable"


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_cancellation_while_transport_open_is_pending_rolls_back():
    class PendingOpen(FakeTransport):
        def __init__(self):
            super().__init__(ready=threading.Event())
            self.started = asyncio.Event()

        async def open(self, source, start, *, context):
            self.started.set()
            return await super().open(source, start, context=context)

    adapter = PendingOpen()
    watcher = EventObserver(transport=adapter).watch(source="events")
    entering = asyncio.create_task(watcher.__aenter__())
    await asyncio.wait_for(adapter.started.wait(), timeout=1)
    entering.cancel()
    with pytest.raises(asyncio.CancelledError):
        await entering
    assert watcher.cleanup_state == "complete"
    assert not watcher._tasks
    assert adapter.open_count == adapter.close_count == 0
    assert not adapter.sessions


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_cancellation_after_session_open_closes_allocated_session():
    class CancelAfterOpen(FakeTransport):
        async def open(self, source, start, *, context):
            session = await super().open(source, start, context=context)
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            return session

    adapter = CancelAfterOpen()
    watcher = EventObserver(transport=adapter).watch(source="events")
    with pytest.raises(asyncio.CancelledError):
        await asyncio.create_task(watcher.__aenter__())
    assert adapter.open_count == adapter.close_count == 1
    assert adapter.sessions[0].closed
    assert watcher.cleanup_state == "complete"
    assert all(task.done() for task in watcher._tasks)


@pytest.mark.asyncio
@pytest.mark.reliability
async def test_cancelling_context_exit_waits_for_tracked_cleanup(monkeypatch):
    close_gate = threading.Event()
    adapter = FakeTransport(close_gate=close_gate)
    watcher = EventObserver(transport=adapter).watch(source="events")
    await watcher.__aenter__()
    session = adapter.sessions[0]
    original_close = session.close
    close_started = asyncio.Event()

    async def tracked_close(*, deadline):
        close_started.set()
        await original_close(deadline=deadline)

    monkeypatch.setattr(session, "close", tracked_close)
    exiting = asyncio.create_task(watcher.__aexit__(None, None, None))
    try:
        await asyncio.wait_for(close_started.wait(), timeout=1)
        exiting.cancel()
        await asyncio.sleep(0)
        assert not exiting.done(), "Cancellation must await the tracked cleanup task"
    finally:
        close_gate.set()
        outcomes = await asyncio.gather(exiting, return_exceptions=True)
    assert isinstance(outcomes[0], asyncio.CancelledError)
    assert adapter.close_count == 1
    assert session.closed
    assert watcher.cleanup_state == "complete"
    assert watcher._close_task is not None and watcher._close_task.done()
    assert all(task.done() for task in watcher._tasks)
