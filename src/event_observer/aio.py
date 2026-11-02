"""Explicit asynchronous API backed by the caller's asyncio event loop.

The original top-level EventObserver/EventWatcher/EventScope imports remain
available. The blocking implementation lives independently in ``sync``.
"""

from .observer import EventObserver, EventScope, EventWatcher

__all__ = ["EventObserver", "EventScope", "EventWatcher"]
