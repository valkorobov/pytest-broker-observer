"""Public configuration and stdlib-only import guarantees."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from importlib.resources import files
from pathlib import Path
import subprocess
import sys
from typing import get_overloads

import pytest

from event_observer import (
    DecodingPolicy,
    EpochMilliseconds,
    EventObserver,
    JournalLimits,
    JournalPolicy,
    LifecyclePolicy,
    RawEvent,
    RuntimeLimits,
    ScopeStart,
    StartPosition,
    SyncEventObserver,
    TransportBufferLimits,
    TransportSource,
)
from event_observer.errors import ConfigurationError
from event_observer.models import ObservationBoundary, TransportCapabilities, TransportReadiness


@pytest.mark.parametrize("stream_id", ["0-0", "1-2", "18446744073709551615-18446744073709551615"])
def test_native_stream_positions_preserve_the_full_id_and_remain_exclusive(stream_id):
    start = StartPosition.after_stream_id(stream_id)
    assert start.mode.value == "stream_id"
    assert start.stream_id == stream_id
    assert start.offsets == {}
    assert start.timestamp_ms is None


@pytest.mark.parametrize(
    "stream_id",
    ["", "1", "$", "+", "1-2-3", "-1-0", "1--1", "01-0", "1-00", "1-2\n", "18446744073709551616-0", None, 1],
)
def test_native_stream_positions_reject_invalid_or_ambiguous_ids(stream_id):
    with pytest.raises(ConfigurationError):
        StartPosition.after_stream_id(stream_id)


def test_native_stream_positions_cannot_be_mixed_with_kafka_offsets_or_timestamps():
    with pytest.raises(ConfigurationError):
        StartPosition(stream_id="1-0")
    with pytest.raises(ConfigurationError):
        StartPosition(mode="stream_id", stream_id="1-0", offsets={0: 1})
    with pytest.raises(ConfigurationError):
        StartPosition(mode="stream_id", stream_id="1-0", timestamp_ms=1)


@pytest.mark.parametrize(
    "factory",
    [
        lambda value: JournalLimits(max_events=value),
        lambda value: JournalLimits(max_bytes=value),
        lambda value: RuntimeLimits(max_active_waiters=value),
        lambda value: RuntimeLimits(max_scopes=value),
        lambda value: RuntimeLimits(max_result_events=value),
        lambda value: TransportBufferLimits(max_events=value),
    ],
)
@pytest.mark.parametrize("value", [True, "10", 1.5])
def test_integer_resource_limits_reject_coercion(factory, value):
    with pytest.raises(ConfigurationError):
        factory(value)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: JournalLimits(max_bytes=100, max_event_bytes=101),
        lambda: JournalPolicy(overflow="unknown"),
        lambda: DecodingPolicy(error_policy="unknown"),
        lambda: TransportBufferLimits(max_bytes=0),
        lambda: RuntimeLimits(scan_batch_events=-1),
    ],
)
def test_conflicting_and_unknown_policies_fail_before_io(factory):
    with pytest.raises(ConfigurationError):
        factory()


@pytest.mark.parametrize(
    "policy,attribute,value",
    [
        (JournalLimits(), "max_events", 1),
        (JournalPolicy(), "overflow", "drop_oldest"),
        (RuntimeLimits(), "max_scopes", 1),
        (TransportBufferLimits(), "max_bytes", 1),
        (LifecyclePolicy(), "startup_timeout", 1),
        (DecodingPolicy(), "error_policy", "skip_unknown_type"),
    ],
)
def test_resource_policies_are_frozen(policy, attribute, value):
    with pytest.raises(FrozenInstanceError):
        setattr(policy, attribute, value)


def test_default_policy_instances_do_not_share_mutable_state():
    first, second = JournalPolicy(), JournalPolicy()
    assert first.limits is not second.limits
    assert first.limits == second.limits
    assert first.overflow.value == "fail"
    assert first.limits.max_events == 10_000
    assert first.limits.max_event_bytes <= first.limits.max_bytes


def test_raw_envelope_detaches_and_freezes_headers_and_metadata():
    headers = [["same", b"first"], ["same", None], ["empty", b""]]
    metadata = {"kafka.partition": 0, "kafka.offset": 42}
    raw = RawEvent(b"", "events", headers=headers, metadata=metadata)
    headers[0][1] = b"changed"
    headers.append(["extra", b"changed"])
    metadata["kafka.offset"] = -1
    assert raw.headers == (("same", b"first"), ("same", None), ("empty", b""))
    assert raw.metadata["kafka.offset"] == 42
    with pytest.raises(TypeError):
        raw.metadata["kafka.offset"] = 0
    with pytest.raises(FrozenInstanceError):
        raw.payload = b"changed"


def test_null_payload_and_empty_bytes_have_distinct_values():
    null = RawEvent(None, "events", key=None, headers=(("value", None),))
    empty = RawEvent(b"", "events", key=b"", headers=(("value", b""),))
    assert null != empty
    assert null.payload is None
    assert empty.payload == b""
    assert null.key is None
    assert empty.key == b""
    assert null.headers != empty.headers


@pytest.mark.parametrize(
    "kwargs",
    [
        {"payload": "text", "source": "events"},
        {"payload": b"", "source": ""},
        {"payload": b"", "source": "events", "key": "text"},
        {"payload": b"", "source": "events", "headers": (("name", "text"),)},
        {"payload": b"", "source": "events", "headers": ((1, b"value"),)},
        {"payload": b"", "source": "events", "metadata": {"nested": {"secret": "value"}}},
    ],
)
def test_raw_envelope_rejects_non_normalized_data(kwargs):
    with pytest.raises(ConfigurationError):
        RawEvent(**kwargs)


@pytest.mark.parametrize("partitions", [(), (0, 0), (-1,), (True,), (1.5,), ("0",)])
def test_source_partition_selection_requires_unique_nonnegative_integers(partitions):
    with pytest.raises(ConfigurationError):
        TransportSource("events", partitions=partitions)


def test_source_partition_selection_is_detached():
    selected = [0, 2]
    source = TransportSource("events", partitions=selected)
    selected.append(4)
    assert source.partitions == (0, 2)


def test_explicit_offset_mapping_is_detached_and_frozen():
    offsets = {0: 125, 1: 242}
    start = StartPosition.at_offset(offsets)
    offsets[0] = -1
    assert start.offsets == {0: 125, 1: 242}
    with pytest.raises(TypeError):
        start.offsets[0] = 0


@pytest.mark.parametrize("offsets", [{}, {True: 0}, {0: True}, {-1: 0}, {0: -1}, {"0": 0}, {0: 1.5}])
def test_explicit_offsets_reject_invalid_positions(offsets):
    with pytest.raises(ConfigurationError):
        StartPosition.at_offset(offsets)


def test_timestamp_start_requires_explicit_clock_units_and_timezone():
    instant = datetime(2020, 1, 1, tzinfo=timezone.utc)
    aware = StartPosition.at_timestamp(instant)
    epoch = StartPosition.at_timestamp(EpochMilliseconds(1_577_836_800_000), require_existing=True)
    assert aware.timestamp_ms == epoch.timestamp_ms == 1_577_836_800_000
    assert not aware.require_existing
    assert epoch.require_existing


@pytest.mark.parametrize("timestamp", [datetime(2020, 1, 1), 1577836800.0, 1577836800000, True])
def test_ambiguous_timestamp_values_are_configuration_errors(timestamp):
    with pytest.raises(ConfigurationError):
        StartPosition.at_timestamp(timestamp)


@pytest.mark.parametrize("value", [-1, True, 1.5, "1"])
def test_epoch_milliseconds_are_nonnegative_integer_values(value):
    with pytest.raises(ConfigurationError):
        EpochMilliseconds(value)


@pytest.mark.parametrize("value", [-1, True, 1.5, None, "1"])
def test_scope_sequences_are_nonnegative_integer_values(value):
    with pytest.raises(ConfigurationError):
        ScopeStart.from_sequence(value)


def test_resolved_transport_metadata_and_boundary_are_detached():
    positions = {0: 17}
    metadata = {"kafka.partition_count": 1}
    declared_starts = {"latest"}
    capabilities = TransportCapabilities(start_modes=declared_starts)
    boundary = ObservationBoundary("events", "latest", positions)
    readiness = TransportReadiness(boundary, capabilities, metadata)
    positions[0] = -1
    metadata["kafka.partition_count"] = 0
    declared_starts.add("earliest")
    assert readiness.boundary.positions == {0: 17}
    assert readiness.source_metadata == {"kafka.partition_count": 1}
    assert readiness.resolved_capabilities.start_modes == frozenset({"latest"})
    with pytest.raises(TypeError):
        readiness.source_metadata["changed"] = True


def test_both_public_watch_factories_have_raw_decoder_and_message_overloads():
    assert len(get_overloads(EventObserver.watch)) == 3
    assert len(get_overloads(SyncEventObserver.watch)) == 3
    assert files("event_observer").joinpath("py.typed").is_file()


def test_clean_core_and_passive_plugin_import_need_only_the_standard_library():
    source = str(Path(__file__).resolve().parents[1] / "src")
    script = r"""
import importlib.abc
import socket
import sys
import threading
from types import SimpleNamespace

sys.path.insert(0, sys.argv[1])
blocked = ("pytest", "pytest_asyncio", "confluent_kafka", "aio_pika", "aiormq", "pika", "redis", "google.protobuf", "allure", "opentelemetry")
class NoOptionalDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise ImportError("optional dependency must remain lazy: " + fullname)
sys.meta_path.insert(0, NoOptionalDependencies())
def forbidden(*args, **kwargs):
    raise AssertionError("import/factory unexpectedly started background work")
threading.Thread.start = forbidden
socket.socket.connect = forbidden
socket.create_connection = forbidden
import event_observer
import event_observer.integrations.pytest_plugin as plugin
from event_observer.models import TransportCapabilities
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig
from event_observer.testing import RabbitMQAdapterHarness
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig
from event_observer.testing import RedisStreamsAdapterHarness
RabbitMQAdapter(RabbitMQConfig(url="amqp://observer:observer@127.0.0.1:5672/"))
RabbitMQAdapterHarness("amqp://observer:observer@127.0.0.1:5672/")
RedisStreamsAdapter(RedisStreamsConfig(url="redis://:observer@127.0.0.1:6379/0"))
RedisStreamsAdapterHarness("redis://:observer@127.0.0.1:6379/0")
async def not_opened(*args, **kwargs):
    raise AssertionError("factory unexpectedly opened a transport")
adapter = SimpleNamespace(open=not_opened, capabilities=TransportCapabilities())
event_observer.EventObserver(transport=adapter).watch(source="events")
sync_adapter = SimpleNamespace(open=forbidden, capabilities=TransportCapabilities())
event_observer.SyncEventObserver(transport=sync_adapter).watch(source="events")
assert not any(name in sys.modules for name in blocked)
assert not any(name.startswith("pytest_") for name in vars(plugin))
print("stdlib core and passive plugin verified")
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-S", "-c", script, source],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "stdlib core and passive plugin verified"
