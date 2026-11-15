"""Shared broker and coordination helpers for the Stage 4 tests."""

import asyncio
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import socket
from time import monotonic
from urllib.parse import urlsplit, urlunsplit

from event_observer.testing.kafka import KafkaAdapterHarness
from event_observer.testing.rabbitmq import RabbitMQAdapterHarness
from event_observer.testing.redis_streams import RedisStreamsAdapterHarness


_BROKERS = {
    "kafka": ("KAFKA_BOOTSTRAP_SERVERS", KafkaAdapterHarness),
    "rabbitmq": ("RABBITMQ_URL", RabbitMQAdapterHarness),
    "redis-streams": ("REDIS_URL", RedisStreamsAdapterHarness),
}


def make_harness(broker, environment):
    """Return the independent harness for the selected broker."""
    variable, constructor = _BROKERS[broker]
    return constructor(environment[variable])


@contextmanager
def unavailable_adapter(broker, environment):
    """Reserve an unlistening loopback port for reproducible startup rollback."""
    variable, constructor = _BROKERS[broker]
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        host = f"127.0.0.1:{reserved.getsockname()[1]}"
        if broker != "kafka":
            url = urlsplit(environment[variable])
            credentials = url.netloc.rpartition("@")[0]
            host = urlunsplit(url._replace(netloc=f"{credentials}@{host}"))
        yield constructor(host).adapter


def worker_ready(directory, mode, observer_id, worker):
    """Atomically expose a READY worker with its observer and process identities."""
    assert re.fullmatch(r"gw[0-9]+", worker)
    state = {"worker": worker, "pid": os.getpid(), "observer_id": observer_id}
    temporary = directory / f"{mode}.tmp"
    temporary.write_text(json.dumps(state), encoding="utf-8")
    temporary.replace(directory / f"{mode}.json")


async def wait_for_workers(directory, workers):
    """Wait for distinct processes to confirm READY before the producer acts."""
    deadline = monotonic() + 30
    paths = [directory / f"{mode}.json" for mode in ("async", "sync")]
    while monotonic() < deadline and not workers.done():
        if all(path.is_file() for path in paths):
            states = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
            for field in ("worker", "pid", "observer_id"):
                if len({state[field] for state in states}) != 2:
                    raise AssertionError(f"Observer workers must have distinct {field} values")
            return dict(zip(("async", "sync"), states, strict=True))
        await asyncio.sleep(0.025)
    # Pytest records the original result, including an observation timeout if
    # one of the workers never signalled READY.
    return {}


def process_handles():
    """Snapshot Linux native threads and open socket identities."""
    proc = Path("/proc/self")
    if not proc.is_dir():
        return None
    sockets = set()
    for entry in (proc / "fd").iterdir():
        try:
            target = os.readlink(entry)
        except FileNotFoundError:
            # The directory enumeration can observe its own now-closed FD.
            continue
        if target.startswith("socket:"):
            sockets.add(target)
    threads = {entry.name for entry in (proc / "task").iterdir()}
    return frozenset(threads), frozenset(sockets)
