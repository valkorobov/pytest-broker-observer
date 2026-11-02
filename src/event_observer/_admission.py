"""Synchronous admission policy shared by asynchronous and blocking runtimes."""

from __future__ import annotations

import inspect
from typing import TypeVar

from ._runtime.clock import Clock
from .config import DecodeErrorPolicy, DecodingPolicy
from .decoders import Decoder, PayloadAccess
from .diagnostics import DiagnosticCollector
from .errors import EventDecodingError, PayloadIsolationError, UnknownMessageTypeError
from .journal import EventJournal
from .models import ObservedEvent, RawEvent
from .trace import extract_trace_context
from .transports.base import DeliveryOutcome

T = TypeVar("T")


def admit_raw(
    raw: RawEvent,
    *,
    observer_id: str,
    journal: EventJournal,
    decoder: Decoder[T],
    access: PayloadAccess[T],
    clock: Clock,
    decoding: DecodingPolicy,
    diagnostics: DiagnosticCollector,
) -> DeliveryOutcome:
    """Validate, decode, account, and commit one delivery without scheduling or settlement.

    The owning runtime may notify waiters and settle the delivery only after this
    function returns a committed outcome. Exceptions leave settlement to transport
    cleanup. Explicit filtering advances the Journal without recording retention loss.

    :param raw: Detached transport envelope awaiting a Core admission decision.
    :param observer_id: Identity attached to the admitted canonical event.
    :param journal: Single-owner bounded history receiving the committed outcome.
    :param decoder: Synchronous network-free payload decoder.
    :param access: Payload accounting and isolation strategy selected by the watcher.
    :param clock: Shared clock supplying admission UTC and monotonic timestamps.
    :param decoding: Policy controlling explicitly unknown message types.
    :param diagnostics: Bounded collector receiving admission or filtering metadata.
    :returns: Admitted, explicitly filtered, or explicitly discarded delivery outcome.
    :raises EventDecodingError: The decoder rejects a delivery or returns an awaitable.
    :raises PayloadIsolationError: The payload accounting result is not a nonnegative integer.
    :raises EventTooLargeError: Raw accounting exceeds the single-event hard limit.
    :raises JournalOverflowError: Retention exceeds the Journal's configured FAIL policy.
    """
    size = journal.check_raw(raw)
    try:
        payload = decoder.decode(raw)
        if inspect.isawaitable(payload):
            if inspect.iscoroutine(payload):
                payload.close()
            raise EventDecodingError("Decoder must return a value synchronously")
    except UnknownMessageTypeError:
        if decoding.error_policy == DecodeErrorPolicy.SKIP_UNKNOWN_TYPE:
            journal.skip_filtered()
            diagnostics.record("filtered", reason_code="unknown_type")
            return DeliveryOutcome.FILTERED
        raise
    except EventDecodingError:
        raise
    except Exception as exc:
        raise EventDecodingError("Custom decoder failed") from exc
    decoded_size = access.estimate_bytes(payload)
    if isinstance(decoded_size, bool) or not isinstance(decoded_size, int) or decoded_size < 0:
        raise PayloadIsolationError("Payload size estimate must be a nonnegative integer")
    event = ObservedEvent(
        observer_id,
        journal.next_sequence,
        payload,
        raw,
        clock.utcnow(),
        clock.monotonic(),
        getattr(getattr(payload, "DESCRIPTOR", None), "full_name", None),
        extract_trace_context(raw.headers),
    )
    admitted = journal.append(event, size + decoded_size + 128)
    diagnostics.record("admission", event_sequence=event.sequence, retained=admitted)
    return DeliveryOutcome.ADMITTED if admitted else DeliveryOutcome.DISCARDED_BY_EXPLICIT_JOURNAL_POLICY
