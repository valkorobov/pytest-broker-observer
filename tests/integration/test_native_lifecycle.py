"""Repeated owned native sessions release observation tasks, threads, and sockets."""

import asyncio
import os
import threading
from time import monotonic, sleep

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.config import LifecyclePolicy
from event_observer.errors import ObserverStartupTimeoutError, TransportConnectionError

from tests.integration.native_lifecycle import make_harness, process_handles, unavailable_adapter


pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        not os.environ.get("EVENT_OBSERVER_FAULTS"),
        reason="Run the isolated broker fault suite for owned native lifecycle checks",
    ),
]


_CYCLES = 6
_FAILED_STARTS = 4
_STARTUP = LifecyclePolicy(startup_timeout=1)
_UNAVAILABLE = (TransportConnectionError, ObserverStartupTimeoutError)


def resources():
    """Snapshot live Python threads and the process's native handles."""
    result = {"python_threads": frozenset(threading.enumerate())}
    handles = process_handles()
    if handles is not None:
        native_threads, sockets = handles
        result.update(native_threads=native_threads, sockets=sockets)
    return result


def remaining_resources(baseline):
    """Keep only resources started after the baseline capture."""
    return {name: len(handles - baseline[name]) for name, handles in resources().items()}


def record_counts(record_property, baseline, remaining):
    for name, handles in baseline.items():
        record_property(f"baseline_{name}", len(handles))
        record_property(f"remaining_{name}", remaining[name])
    record_property("lifecycles", _CYCLES)
    record_property("failed_startups", _FAILED_STARTS)


@pytest.mark.asyncio
async def test_async_reader_releases_native_resources(record_property):
    broker = os.environ["EVENT_OBSERVER_FAULTS"]
    harness = make_harness(broker, os.environ)
    try:
        source = await harness.create_source(case_id="async-lifecycles")
        observer = EventObserver(transport=harness.adapter)

        async def cycle(index):
            payload = str(index).encode()
            async with observer.watch(source=source) as watcher:
                assert (await harness.publish(source, payload)).accepted
                assert await watcher.wait_for(lambda value: value == payload, timeout=8) == payload

        # Warm the independent publishing client and the runtime's execution pool.
        await cycle(-1)
        baseline = resources()
        tasks = asyncio.all_tasks()
        for index in range(_CYCLES):
            await cycle(index)
        with unavailable_adapter(broker, os.environ) as adapter:
            unavailable = EventObserver(transport=adapter, lifecycle=_STARTUP)
            for _ in range(_FAILED_STARTS):
                with pytest.raises(_UNAVAILABLE):
                    async with unavailable.watch(source=source):
                        pytest.fail("An unreachable broker reached READY")
        deadline = monotonic() + 4
        while any((remaining := remaining_resources(baseline)).values()) and monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert not any(remaining.values()), remaining
        remaining_tasks = asyncio.all_tasks() - tasks
        assert not remaining_tasks
        record_property("baseline_asyncio_tasks", len(tasks))
        record_property("remaining_asyncio_tasks", len(remaining_tasks))
        record_counts(record_property, baseline, remaining)
    finally:
        await harness.cleanup()


def test_sync_reader_releases_native_resources(record_property):
    broker = os.environ["EVENT_OBSERVER_FAULTS"]
    harness = make_harness(broker, os.environ)
    # The publisher uses a persistent test-side loop. Observer I/O stays on
    # the blocking actor and the broker adapter's native worker.
    with asyncio.Runner() as publisher:
        try:
            source = publisher.run(harness.create_source(case_id="sync-lifecycles"))
            observer = SyncEventObserver(transport=harness.adapter)

            def cycle(index):
                payload = str(index).encode()
                with observer.watch(source=source) as watcher:
                    assert publisher.run(harness.publish(source, payload)).accepted
                    assert watcher.wait_for(lambda value: value == payload, timeout=8) == payload

            cycle(-1)
            baseline = resources()
            for index in range(_CYCLES):
                cycle(index)
            with unavailable_adapter(broker, os.environ) as adapter:
                unavailable = SyncEventObserver(transport=adapter, lifecycle=_STARTUP)
                for _ in range(_FAILED_STARTS):
                    with pytest.raises(_UNAVAILABLE):
                        with unavailable.watch(source=source):
                            pytest.fail("An unreachable broker reached READY")
            deadline = monotonic() + 4
            while any((remaining := remaining_resources(baseline)).values()) and monotonic() < deadline:
                sleep(0.02)
            assert not any(remaining.values()), remaining
            record_counts(record_property, baseline, remaining)
        finally:
            publisher.run(harness.cleanup())
