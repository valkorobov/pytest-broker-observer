"""Owned Kafka startup signals remain observed when readiness races a timeout."""

import asyncio
import gc

import pytest

from event_observer.errors import TransportReadinessError
from event_observer.models import StartPosition, TransportSource
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig, KafkaSession

from tests.test_kafka import Native, context, setup_native


@pytest.mark.asyncio
async def test_startup_timeout_cancels_single_owner_ready_signal_before_cleanup(monkeypatch):
    native = Native()
    native.suppress_fetch = True
    setup_native(monkeypatch, native)
    loop = asyncio.get_running_loop()
    unhandled = []
    previous_handler = loop.get_exception_handler()
    original_close = KafkaSession.close
    original_remaining = KafkaSession._remaining
    late_error = TransportReadinessError("Native readiness failed at the timeout boundary")
    signals = []

    def remaining(self):
        # Keep native probing in flight until the caller stops waiting. The
        # controlled close below delivers the competing error if still possible.
        return original_remaining(self) if self._stop.is_set() else 0.01

    async def close(self, *, deadline):
        signals.append(self._ready)
        if not self._ready.done():
            self._ready.set_exception(late_error)
        await original_close(self, deadline=deadline)

    monkeypatch.setattr(KafkaSession, "_remaining", remaining)
    monkeypatch.setattr(KafkaSession, "close", close)
    loop.set_exception_handler(lambda owner, details: unhandled.append(details))
    try:
        with pytest.raises(TimeoutError):
            await KafkaAdapter(KafkaConfig("broker")).open(
                TransportSource("events"), StartPosition.latest(), context=context(timeout=0.01)
            )
        await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
        assert signals[0].cancelled()
        assert not any(details.get("exception") is late_error for details in unhandled)
        assert native.clients[0].closed
    finally:
        loop.set_exception_handler(previous_handler)
