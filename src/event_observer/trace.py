"""Passive W3C trace-context extraction, independent of OpenTelemetry."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class TraceContextStatus(str, Enum):
    MISSING = "MISSING"
    VALID = "VALID"
    INVALID = "INVALID"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True, slots=True)
class TraceContextInfo:
    status: TraceContextStatus
    trace_id: str | None = None
    span_id: str | None = None
    flags: str | None = None
    tracestate: str | None = None
    tracestate_valid: bool = True


_PARENT = re.compile(r"([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})")
_SIMPLE_KEY = re.compile(r"[a-z][a-z0-9_*/-]{0,255}\Z")
_MULTI_KEY = re.compile(r"[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13}\Z")


def _valid_tracestate(value: str) -> bool:
    if not value or len(value) > 512:
        return False
    members = value.split(",")
    if len(members) > 32:
        return False
    seen: set[str] = set()
    for member in members:
        member = member.strip(" \t")
        if "=" not in member:
            return False
        key, val = member.split("=", 1)
        if not (_SIMPLE_KEY.fullmatch(key) or _MULTI_KEY.fullmatch(key)) or key in seen:
            return False
        if not val or len(val) > 256 or val[-1] == " ":
            return False
        if any(ord(char) < 0x20 or ord(char) > 0x7E or char in ",=" for char in val):
            return False
        seen.add(key)
    return True


def extract_trace_context(
    headers: Iterable[tuple[str, bytes | None]], *, include_tracestate: bool = False
) -> TraceContextInfo:
    parents: list[bytes | None] = []
    states: list[bytes | None] = []
    for name, value in headers:
        if name.lower() == "traceparent":
            parents.append(value)
        elif name.lower() == "tracestate":
            states.append(value)
    if not parents:
        return TraceContextInfo(TraceContextStatus.MISSING)
    if len(parents) > 1:
        return TraceContextInfo(TraceContextStatus.AMBIGUOUS)
    try:
        value = parents[0].decode("ascii") if parents[0] is not None else ""
    except UnicodeError:
        return TraceContextInfo(TraceContextStatus.INVALID)
    match = _PARENT.match(value)
    if match is None:
        return TraceContextInfo(TraceContextStatus.INVALID)
    version, trace_id, span_id, flags = match.groups()
    if version == "ff" or int(trace_id, 16) == 0 or int(span_id, 16) == 0:
        return TraceContextInfo(TraceContextStatus.INVALID)
    if version == "00" and len(value) != 55:
        return TraceContextInfo(TraceContextStatus.INVALID)
    # For later versions the known prefix remains interpretable. Unknown fields
    # must be separated by a dash; their format belongs to the future version.
    if version != "00" and len(value) > 55 and value[55] != "-":
        return TraceContextInfo(TraceContextStatus.INVALID)
    state = None
    state_valid = True
    if states:
        try:
            state = ",".join(part.decode("ascii") for part in states if part is not None)
            state_valid = all(part is not None for part in states) and _valid_tracestate(state)
        except UnicodeError:
            state_valid = False
        if not state_valid or not include_tracestate:
            state = None
    return TraceContextInfo(TraceContextStatus.VALID, trace_id, span_id, flags, state, state_valid)


__all__ = ["TraceContextInfo", "TraceContextStatus", "extract_trace_context"]
