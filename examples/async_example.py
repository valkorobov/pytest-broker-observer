"""Run with `uv run python examples/async_example.py`; no broker required."""

import asyncio

from event_observer import EventObserver, ScopeStart
from event_observer.testing import FakeTransport


async def main() -> None:
    transport = FakeTransport()
    async with EventObserver(transport=transport).watch(source="example.events") as events:
        operation = events.scope(
            lambda value: value.startswith(b"operation-42:"), start=ScopeStart.from_now()
        )
        transport.publish(b"operation-42:created", source="example.events")
        transport.publish(b"operation-42:completed", source="example.events")
        result = await operation.wait_for_count(lambda value: True, 2, timeout=1)
        print([value.decode() for value in result])


if __name__ == "__main__":
    asyncio.run(main())
