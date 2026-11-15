"""Simultaneous independent native readers in separate pytest-xdist workers."""

import os
from pathlib import Path

import pytest

from event_observer import EventObserver, SyncEventObserver, TransportSource

from tests.integration.native_lifecycle import make_harness, worker_ready


pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        not os.environ.get("EVENT_OBSERVER_XDIST_COORDINATION"),
        reason="Run the isolated broker fault suite for an owned shared-source test",
    ),
]


def ownership():
    """Read the owned source and shared coordination directory from the runner."""
    directory = Path(os.environ["EVENT_OBSERVER_XDIST_COORDINATION"])
    source = TransportSource(os.environ["EVENT_OBSERVER_XDIST_SOURCE"])
    harness = make_harness(os.environ["EVENT_OBSERVER_FAULTS"], os.environ)
    return directory, source, harness.adapter


@pytest.mark.asyncio
async def test_async_worker_observes_the_shared_publication():
    directory, source, transport = ownership()
    async with EventObserver(transport=transport).watch(source=source) as watcher:
        worker_ready(directory, "async", watcher.observer_id, os.environ["PYTEST_XDIST_WORKER"])
        assert await watcher.wait_for(lambda value: value == b"shared-source-publication", timeout=40)


def test_sync_worker_observes_the_shared_publication():
    directory, source, transport = ownership()
    with SyncEventObserver(transport=transport).watch(source=source) as watcher:
        worker_ready(directory, "sync", watcher.observer_id, os.environ["PYTEST_XDIST_WORKER"])
        assert watcher.wait_for(lambda value: value == b"shared-source-publication", timeout=40)
