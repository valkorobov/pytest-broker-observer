"""Scheduler-independent lifecycle and detached scope diagnostic models."""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class LifecycleState(str, Enum):
    """Lifecycle states for a single-use observation context.

    :ivar CREATED: Constructed without transport resources.
    :ivar STARTING: Opening the transport and confirming readiness.
    :ivar READY: Collecting events and accepting expectations.
    :ivar FAILED: A terminal operation or cleanup failure is known.
    :ivar STOPPING: Releasing tasks and transport resources.
    :ivar CLOSED: Transport and runtime cleanup completed.
    """

    CREATED = "created"
    STARTING = "starting"
    READY = "ready"
    FAILED = "failed"
    STOPPING = "stopping"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class ScopeSnapshot:
    """Detached diagnostics for an immutable scope definition.

    :ivar observer_id: Identity of the watcher owning this scope.
    :ivar scope_id: Stable identity used to associate expectations with the scope.
    :ivar lower_bound: Journal sequence at which the scope definition starts.
    :ivar complete: Whether the scope definition contains no known retention loss.
    :ivar expectations: Retained expectation summaries belonging to the scope.
    :ivar name: Optional caller-supplied scope label.
    :ivar filter_count: Number of filters inherited and added by the scope.
    """

    observer_id: str
    scope_id: str
    lower_bound: int
    complete: bool
    expectations: tuple[Any, ...]
    name: str | None = None
    filter_count: int = 0
