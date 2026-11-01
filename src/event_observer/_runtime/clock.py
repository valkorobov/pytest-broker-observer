"""One monotonic domain per observer; asyncio receives relative durations only."""

from datetime import datetime, timezone
import time
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def utcnow(self) -> datetime: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def utcnow(self) -> datetime:
        return datetime.now(timezone.utc)
