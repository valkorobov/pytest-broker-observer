import asyncio
from dataclasses import dataclass
import threading
import time

import pytest

from event_observer import (
    EventObserver,
    SyncEventObserver,
    JournalLimits,
    JournalPolicy,
    OverflowPolicy,
    RuntimeLimits,
    ScopeStart,
    LifecyclePolicy,
)
from event_observer.errors import (
    ConfigurationError,
    EventTimeoutError,
    ExpectationResourceLimitError,
    IncompleteObservationError,
    JournalOverflowError,
    ObserverFailedError,
    ObserverClosedError,
    ObserverCleanupTimeoutError,
    ObserverStartupTimeoutError,
    InvalidMatcherResultError,
    MatcherEvaluationError,
    ThreadAffinityError,
    ReentrantSyncCallError,
    SyncAPIInAsyncContextError,
    EventLoopMismatchError,
)
from event_observer.testing import FakeTransport


async def admitted(watcher, count):
    async with asyncio.timeout(1):
        while watcher.snapshot().next_sequence < count:
            await asyncio.sleep(0)


def retention(policy, *, max_events=2, max_loss_ranges=1024):
    return JournalPolicy(
        JournalLimits(
            max_events=max_events, max_bytes=4096, max_event_bytes=1024, max_loss_ranges=max_loss_ranges
        ),
        policy,
    )


@pytest.mark.asyncio
async def test_lazy_ready_history_and_non_consuming_count():
    adapter = FakeTransport()
    observer = EventObserver(transport=adapter)
    context = observer.watch(source="events")
    assert adapter.open_count == 0
    async with context as watcher:
        assert adapter.open_count == 1
        for value in (b"created", b"completed", b"completed"):
            adapter.publish(value)
        await admitted(watcher, 3)
        assert await watcher.wait_for(lambda p: p == b"completed", timeout=0.1) == b"completed"
        assert await watcher.wait_for_count(lambda p: p == b"completed", 2, timeout=0.1) == [b"completed"] * 2
        event = await watcher.wait_for_event(lambda p: p == b"created", timeout=0.1)
        assert event.sequence == 0
        assert event.recorded_at.utcoffset().total_seconds() == 0
    assert adapter.close_count == 1
    await context.close()
    assert adapter.close_count == 1
    with pytest.raises(ConfigurationError):
        await context.__aenter__()


@pytest.mark.asyncio
async def test_scopes_from_now_and_nested_short_circuit():
    adapter = FakeTransport()
    async with EventObserver(transport=adapter).watch(source="events") as w:
        adapter.publish(b"old")
        await admitted(w, 1)
        parent = w.scope(lambda p: p.startswith(b"new"), start=ScopeStart.from_now())
        calls = []
        child = parent.scope(lambda p: calls.append(p) or True, start=ScopeStart.from_sequence(0))
        assert child.lower_bound == 1
        with pytest.raises(AttributeError):
            child.lower_bound = 0
        adapter.publish(b"irrelevant")
        adapter.publish(b"new-event")
        result = await child.wait_for(lambda p: True, timeout=0.1)
        assert result == b"new-event"
        assert calls == [b"new-event"]
        child.snapshot()
        assert calls == [b"new-event"]


@pytest.mark.asyncio
async def test_hundred_waiters_with_half_cancelled():
    adapter = FakeTransport()
    async with EventObserver(transport=adapter).watch(source="events") as w:
        tasks = [asyncio.create_task(w.wait_for(lambda p: p == b"ok", timeout=1)) for _ in range(100)]
        await asyncio.sleep(0)
        for task in tasks[:50]:
            task.cancel()
        adapter.publish(b"ok")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert all(isinstance(result, asyncio.CancelledError) for result in results[:50])
        assert results[50:] == [b"ok"] * 50
        assert not w._waiter_tasks
        assert w._result_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [OverflowPolicy.DROP_OLDEST, OverflowPolicy.DROP_NEWEST])
async def test_retention_loss_persists_for_new_waiters_but_explicit_scope_recovers(policy):
    adapter = FakeTransport()
    async with EventObserver(transport=adapter, journal=retention(policy)).watch(source="events") as w:
        for i in range(3):
            adapter.publish(str(i).encode())
        await admitted(w, 3)
        with pytest.raises(IncompleteObservationError):
            await w.wait_for(lambda p: True, timeout=0.1)
        assert await w.wait_for(lambda p: True, timeout=0.1, require_complete=False) in (b"0", b"1")
        scope = w.scope(lambda p: True, start=ScopeStart.from_now())
        assert scope.snapshot().complete
        # With DROP_NEWEST make room by intentionally leaving a short clean range
        # with no new event; completeness remains an explicit interval property.
        with pytest.raises(EventTimeoutError):
            await scope.wait_for(lambda p: True, timeout=0.001)
        assert not w.snapshot().complete


@pytest.mark.asyncio
async def test_journal_failure_propagates_waiter_and_normal_exit():
    adapter = FakeTransport()
    with pytest.raises(ObserverFailedError) as caught:
        async with EventObserver(transport=adapter, journal=retention(OverflowPolicy.FAIL)).watch(
            source="events"
        ) as w:
            for _ in range(3):
                adapter.publish(b"record")
            with pytest.raises(ObserverFailedError) as waiter_error:
                await w.wait_for(lambda p: False, timeout=1)
            assert isinstance(waiter_error.value.__cause__, JournalOverflowError)
    assert isinstance(caught.value.cause, JournalOverflowError)
    assert adapter.close_count == 1


@pytest.mark.asyncio
async def test_failure_without_waiter_visible_at_exit():
    adapter = FakeTransport()
    with pytest.raises(ObserverFailedError):
        async with EventObserver(transport=adapter).watch(source="events"):
            adapter.fail()
            await asyncio.sleep(0.003)
    assert adapter.close_count == 1


@pytest.mark.asyncio
async def test_waiter_errors_do_not_kill_reader_and_have_current_diagnostics():
    adapter = FakeTransport()
    async with EventObserver(transport=adapter).watch(source="events") as w:
        adapter.publish(b"event")
        with pytest.raises(InvalidMatcherResultError):
            await w.wait_for(lambda p: 1, timeout=0.1)

        def fails(value):
            raise ValueError("secret-cause")

        with pytest.raises(MatcherEvaluationError) as caught:
            await w.wait_for(fails, timeout=0.1)
        assert caught.value.observer_id == w.observer_id
        assert caught.value.expectation_id
        assert caught.value.sequence == 0
        assert await w.wait_for(lambda p: True, timeout=0.1) == b"event"
        with pytest.raises(EventTimeoutError) as caught:
            await w.wait_for(lambda p: False, timeout=0.001)
        assert caught.value.diagnostics.expectations[-1].outcome == "timeout"


@dataclass
class Mutable:
    values: list


class MutableDecoder:
    def decode(self, raw):
        return Mutable([raw.payload])


@pytest.mark.asyncio
async def test_mutation_isolation_predicate_result_and_snapshot():
    adapter = FakeTransport()
    async with EventObserver(transport=adapter).watch(source="events", decoder=MutableDecoder()) as w:
        adapter.publish(b"original")

        def changes(payload):
            payload.values.append(b"mutated")
            return True

        result = await w.wait_for(changes, timeout=0.1)
        assert result.values == [b"original"]
        result.values.clear()
        snapshot = w.snapshot()
        snapshot.events[0].payload.values.clear()
        assert (await w.wait_for(lambda p: True, timeout=0.1)).values == [b"original"]


@pytest.mark.asyncio
async def test_count_does_not_reuse_single_record():
    adapter = FakeTransport()
    async with EventObserver(transport=adapter).watch(source="events") as w:
        adapter.publish(b"one")
        with pytest.raises(EventTimeoutError) as caught:
            await w.wait_for_count(lambda p: True, 2, timeout=0.003)
        assert caught.value.diagnostics.expectations[-1].matched == 1
        assert w._result_bytes == 0


@pytest.mark.asyncio
async def test_waiter_and_result_resource_limits():
    adapter = FakeTransport()
    limits = RuntimeLimits(max_active_waiters=1, max_result_events=1)
    async with EventObserver(transport=adapter, runtime_limits=limits).watch(source="events") as w:
        task = asyncio.create_task(w.wait_for(lambda p: True, timeout=1))
        await asyncio.sleep(0)
        with pytest.raises(ExpectationResourceLimitError):
            await w.wait_for(lambda p: True, timeout=1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        with pytest.raises(ExpectationResourceLimitError):
            await w.wait_for_count(lambda p: True, 2, timeout=0.1)


@pytest.mark.asyncio
async def test_startup_timeout_has_no_reader_tasks():
    adapter = FakeTransport(ready=threading.Event())
    policy = LifecyclePolicy(startup_timeout=0.003)
    watcher = EventObserver(transport=adapter, lifecycle=policy).watch(source="events")
    with pytest.raises(ObserverStartupTimeoutError):
        await watcher.__aenter__()
    assert watcher.cleanup_state == "complete"
    assert not watcher._tasks


@pytest.mark.asyncio
async def test_cleanup_timeout_and_original_exception_priority():
    adapter = FakeTransport(close_gate=threading.Event())
    policy = LifecyclePolicy(shutdown_timeout=0.003)
    watcher = EventObserver(transport=adapter, lifecycle=policy).watch(source="events")
    with pytest.raises(ObserverCleanupTimeoutError):
        async with watcher:
            pass
    assert watcher.cleanup_state == "abandoned"
    adapter.close_gate.set()
    adapter2 = FakeTransport(close_gate=threading.Event())
    with pytest.raises(AssertionError, match="original"):
        async with EventObserver(transport=adapter2, lifecycle=policy).watch(source="events"):
            raise AssertionError("original")
    adapter2.close_gate.set()


@pytest.mark.asyncio
async def test_close_wakes_and_finishes_waiter():
    adapter = FakeTransport()
    async with EventObserver(transport=adapter).watch(source="events") as w:
        task = asyncio.create_task(w.wait_for(lambda p: True, timeout=5))
        await asyncio.sleep(0)
        await asyncio.gather(w.close(), w.close())
        with pytest.raises(ObserverClosedError):
            await task
    assert adapter.close_count == 1


def test_sync_reader_continues_while_service_blocks_and_no_threads_accumulate():
    before = {t.ident for t in threading.enumerate() if t.name == "event-observer-runtime"}
    for _ in range(10):
        adapter = FakeTransport()
        with SyncEventObserver(transport=adapter).watch(source="events") as w:
            scope = w.scope(lambda p: True, start=ScopeStart.from_now())
            adapter.publish(b"received-during-action")
            time.sleep(0.003)
            assert w.snapshot().next_sequence == 1
            assert scope.wait_for(lambda p: True, timeout=0.1) == b"received-during-action"
        assert w.diagnostics.snapshot() is not None
        assert adapter.close_count == 1
        w.close()
    assert {t.ident for t in threading.enumerate() if t.name == "event-observer-runtime"} == before


def test_sync_foreign_thread_and_reentrant_calls_fail_immediately():
    adapter = FakeTransport()
    with SyncEventObserver(transport=adapter).watch(source="events") as w:
        errors = []

        def foreign():
            try:
                w.snapshot()
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=foreign)
        thread.start()
        thread.join(1)
        assert isinstance(errors[0], ThreadAffinityError)
        adapter.publish(b"record")

        def reentrant(payload):
            with pytest.raises(ReentrantSyncCallError):
                w.snapshot()
            return True

        assert w.wait_for(reentrant, timeout=0.1) == b"record"


@pytest.mark.asyncio
async def test_sync_inside_async_is_refused():
    with pytest.raises(SyncAPIInAsyncContextError):
        with SyncEventObserver(transport=FakeTransport()).watch(source="events"):
            pass


def test_foreign_async_loop_refused():
    adapter = FakeTransport()
    loop = asyncio.new_event_loop()
    watcher = EventObserver(transport=adapter).watch(source="events")
    loop.run_until_complete(watcher.__aenter__())

    async def foreign():
        with pytest.raises(EventLoopMismatchError):
            watcher.snapshot()

    asyncio.run(foreign())
    loop.run_until_complete(watcher.close())
    loop.close()


@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf")])
def test_invalid_budgets_rejected(value):
    with pytest.raises(ConfigurationError):
        LifecyclePolicy(startup_timeout=value)


@pytest.mark.parametrize("start", [lambda: ScopeStart("invalid"), lambda: ScopeStart("from_sequence", True)])
def test_invalid_scope_start_rejected(start):
    with pytest.raises(ConfigurationError):
        start()
