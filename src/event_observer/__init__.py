"""Bounded broker event observation. Core imports require only Python's stdlib."""

from .config import (
    DecodeErrorPolicy,
    DecodingPolicy,
    JournalLimits,
    JournalPolicy,
    LifecyclePolicy,
    ObservationIdentity,
    OverflowPolicy,
    RuntimeLimits,
    TransportBufferLimits,
)
from .diagnostics import DiagnosticLimits, DiagnosticPolicy, DisclosurePolicy, ReportingMode
from .models import (
    EpochMilliseconds,
    ObservedEvent,
    ObservationMark,
    RawEvent,
    ScopeStart,
    StartPosition,
    TransportSource,
)
from .observer import EventObserver, EventScope, EventWatcher
from .sync import SyncEventObserver, SyncEventScope, SyncEventWatcher

__version__ = "0.1.0"
__all__ = [
    "EventObserver",
    "SyncEventObserver",
    "EventWatcher",
    "SyncEventWatcher",
    "EventScope",
    "SyncEventScope",
    "RawEvent",
    "ObservedEvent",
    "ObservationMark",
    "StartPosition",
    "EpochMilliseconds",
    "ScopeStart",
    "TransportSource",
    "JournalLimits",
    "JournalPolicy",
    "OverflowPolicy",
    "RuntimeLimits",
    "TransportBufferLimits",
    "LifecyclePolicy",
    "DecodingPolicy",
    "DecodeErrorPolicy",
    "ObservationIdentity",
    "DiagnosticPolicy",
    "DiagnosticLimits",
    "DisclosurePolicy",
    "ReportingMode",
]
