"""Drive both Kafka transport protocols with the same bounded fault assertions."""

import asyncio
from contextlib import ExitStack, contextmanager
import json
import re
import time
from uuid import uuid4

import pytest

from event_observer.config import TransportBufferLimits
from event_observer.models import OpenContext

from tests.integration.containers.common import _owned_broker


def delete_kafka_records(topic, partition, offset):
    """Trim one owned partition and return the broker-confirmed low watermark."""
    if not re.fullmatch(r"observer-contract-[A-Za-z0-9._-]+-[a-f0-9]{32}", topic):
        raise ValueError("Kafka fault controls require a uniquely named test topic")
    # Python 3.14 clients at the supported floor can segfault after delete_records().
    # Use the broker's own bounded admin CLI to perform the same DeleteRecords RPC.
    # https://github.com/confluentinc/confluent-kafka-python/issues/2275
    payload = json.dumps(
        {"version": 1, "partitions": [{"topic": topic, "partition": partition, "offset": offset}]}
    )
    script = """
        work=$(mktemp -d)
        trap 'rm -rf "$work"' EXIT
        printf '%s\\n' "$1" > "$work/offsets.json"
        printf 'request.timeout.ms=8000\\ndefault.api.timeout.ms=12000\\n' > "$work/admin.properties"
        /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server kafka:29092 \\
            --command-config "$work/admin.properties" --offset-json-file "$work/offsets.json"
    """
    with _owned_broker("kafka") as wrapped:
        result = wrapped.exec_run(["sh", "-ec", script, "delete-kafka-records", payload])
    output = result.output.decode("utf-8", errors="replace")
    watermark = re.search(
        rf"^partition: {re.escape(topic)}-{partition}\s+low_watermark:\s+([0-9]+)$", output, re.M
    )
    assert result.exit_code == 0 and watermark is not None, output[-2048:]
    return int(watermark[1])


class KafkaFaultReader:
    def __init__(self, session, run):
        self.session = session
        self._run = run
        self._stream = session.receive() if run is not None else None

    def next(self, timeout=12):
        if self._run is not None:
            return self._run(asyncio.wait_for(anext(self._stream), timeout))
        deadline = time.monotonic() + timeout
        while (remaining := deadline - time.monotonic()) > 0:
            event = self.session.poll(min(0.1, remaining))
            if event is not None:
                return event
        pytest.fail("Kafka did not deliver or report a terminal failure within the poll budget")

    def assert_failed(self, error_type):
        if self._run is not None:
            expected = self.failure()
            assert isinstance(expected, error_type)
        with pytest.raises(error_type) as raised:
            self.next()
        if self._run is None:
            expected = self.failure()
        assert raised.value is expected
        return expected

    def failure(self):
        if self._run is not None:
            return self._run(asyncio.wait_for(self.session.wait_failure(), 12))
        return self.session.failure()


@contextmanager
def kafka_fault_reader(mode, adapter, source, start, *, startup_timeout=15):
    """Open one reader and always close its exact native session under a finite deadline."""
    context = OpenContext(
        f"kafka-fault-{uuid4().hex}", time.monotonic() + startup_timeout, TransportBufferLimits()
    )
    with ExitStack() as stack:
        if mode == "async":
            run = stack.enter_context(asyncio.Runner()).run
            session = run(adapter.open(source, start, context=context))
            stack.callback(lambda: run(session.close(deadline=time.monotonic() + 8)))
        else:
            run = None
            session = adapter.sync_adapter().open(source, start, context=context)
            stack.callback(lambda: session.close(deadline=time.monotonic() + 8))
        reader = KafkaFaultReader(session, run)
        if reader._stream is not None:
            stack.callback(lambda: run(reader._stream.aclose()))
        yield reader
