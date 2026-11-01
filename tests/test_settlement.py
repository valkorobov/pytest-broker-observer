"""Settlement is explicit and follows durable in-memory outcome accounting."""

from dataclasses import replace

import pytest

from event_observer import (
    DecodeErrorPolicy,
    DecodingPolicy,
    EventObserver,
    JournalLimits,
    JournalPolicy,
    OverflowPolicy,
    RawEvent,
)
from event_observer.errors import EventDecodingError, ObserverFailedError, UnknownMessageTypeError
from event_observer.testing import FakeTransport
from event_observer.transports.base import DeliveryOutcome


class SettlingTransport(FakeTransport):
    capabilities = replace(FakeTransport.capabilities, supports_settlement=True)

    def __init__(self):
        super().__init__()
        self.settled = []

    async def open(self, *args, **kwargs):
        session = await super().open(*args, **kwargs)

        async def settle(delivery_id, outcome):
            self.settled.append((delivery_id, outcome))

        session.settle = settle
        return session


@pytest.mark.asyncio
async def test_settlement_admitted_and_explicit_discard():
    adapter = SettlingTransport()
    journal = JournalPolicy(
        JournalLimits(max_events=1, max_bytes=4096, max_event_bytes=1024), OverflowPolicy.DROP_NEWEST
    )
    async with EventObserver(transport=adapter, journal=journal).watch(source="events") as watcher:
        adapter.publish(RawEvent(b"one", "events", delivery_id="one"))
        adapter.publish(RawEvent(b"two", "events", delivery_id="two"))
        assert await watcher.wait_for(lambda p: True, timeout=1, require_complete=False) == b"one"
        assert adapter.settled == [
            ("one", DeliveryOutcome.ADMITTED),
            ("two", DeliveryOutcome.DISCARDED_BY_EXPLICIT_JOURNAL_POLICY),
        ]
        assert not watcher.snapshot().complete


@pytest.mark.asyncio
async def test_filtered_settles_but_decode_failure_does_not():
    adapter = SettlingTransport()

    class Decoder:
        def decode(self, raw):
            if raw.payload == b"unknown":
                raise UnknownMessageTypeError("unknown")
            raise EventDecodingError("invalid")

    with pytest.raises(ObserverFailedError):
        async with EventObserver(transport=adapter).watch(
            source="events",
            decoder=Decoder(),
            decoding=DecodingPolicy(DecodeErrorPolicy.SKIP_UNKNOWN_TYPE),
        ) as watcher:
            adapter.publish(RawEvent(b"unknown", "events", delivery_id="filtered"))
            adapter.publish(RawEvent(b"bad", "events", delivery_id="failed"))
            await watcher.wait_for(lambda p: True, timeout=1)
    assert adapter.settled == [("filtered", DeliveryOutcome.FILTERED)]
