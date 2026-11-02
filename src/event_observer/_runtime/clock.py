"""One monotonic domain per observer; asyncio receives relative durations only."""

from datetime import datetime, timezone
import time
from typing import Protocol


class Clock(Protocol):
    """Define the monotonic deadline domain and reporting wall clock shared by one watcher."""

    def monotonic(self) -> float:
        """Read the monotonic clock used for admission and absolute internal deadlines.

        :returns: Seconds in a stable clock domain shared by all runtime operations.
        """
        ...

    def utcnow(self) -> datetime:
        """Read the wall clock for diagnostic timestamps without defining event eligibility.

        :returns: A timezone-aware datetime representing current UTC.
        """
        ...


class SystemClock:
    """Use the standard-library monotonic timer and timezone-aware UTC wall clock."""

    def monotonic(self) -> float:
        """Read time.monotonic for the production runtime deadline domain.

        :returns: Monotonic seconds.
        """
        return time.monotonic()

    def utcnow(self) -> datetime:
        """Read the current timezone-aware UTC wall clock.

        :returns: The current datetime with UTC timezone information.
        """
        return datetime.now(timezone.utc)
