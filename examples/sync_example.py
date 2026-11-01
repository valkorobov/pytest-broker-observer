"""Run with `uv run python examples/sync_example.py`; no broker required."""

from event_observer import ScopeStart, SyncEventObserver
from event_observer.testing import FakeTransport


def main() -> None:
    transport = FakeTransport()
    with SyncEventObserver(transport=transport).watch(source="example.events") as events:
        operation = events.scope(
            lambda value: value.startswith(b"operation-42:"), start=ScopeStart.from_now()
        )
        transport.publish(b"operation-42:created", source="example.events")
        transport.publish(b"operation-42:completed", source="example.events")
        result = operation.wait_for(lambda value: value.endswith(b":completed"), timeout=1)
        print(result.decode())


if __name__ == "__main__":
    main()
