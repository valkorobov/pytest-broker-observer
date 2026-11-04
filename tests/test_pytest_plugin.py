"""Factory fixture contracts, isolated plugin loading, and project composition."""

import asyncio
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from event_observer import EventObserver, JournalPolicy, LifecyclePolicy, SyncEventObserver
from event_observer.diagnostics import DiagnosticPolicy
from event_observer.errors import ConfigurationError
from event_observer.testing import FakeTransport
from event_observer.transports.kafka import KafkaAdapter, KafkaConfig
from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig
from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig

pytest_plugins = ["pytester"]

TRANSPORTS = [
    ("kafka_transport_factory", KafkaAdapter, KafkaConfig(bootstrap_servers="localhost:9092")),
    ("rabbitmq_transport_factory", RabbitMQAdapter, RabbitMQConfig(url="amqp://localhost/")),
    ("redis_streams_transport_factory", RedisStreamsAdapter, RedisStreamsConfig(url="redis://localhost/0")),
]


@pytest.mark.parametrize("fixture_name,adapter_type,config", TRANSPORTS)
def test_transport_factories_preserve_constructors_and_create_distinct_adapters(
    request, fixture_name, adapter_type, config, sync_observer_factory, async_observer_factory
):
    factory = request.getfixturevalue(fixture_name)
    assert factory is adapter_type
    first, second = factory(config), factory(config=config)
    assert first is not second
    assert first.config is config
    assert second.config is config
    assert isinstance(sync_observer_factory(transport=first), SyncEventObserver)
    assert isinstance(async_observer_factory(transport=first), EventObserver)


@pytest.mark.parametrize(
    "fixture_name,observer_type",
    [("sync_observer_factory", SyncEventObserver), ("async_observer_factory", EventObserver)],
)
def test_observer_factories_preserve_constructors_and_policies(request, fixture_name, observer_type):
    factory = request.getfixturevalue(fixture_name)
    transport = FakeTransport()
    journal, lifecycle, diagnostics = JournalPolicy(), LifecyclePolicy(), DiagnosticPolicy()
    reporter = SimpleNamespace(emit=lambda report: None)
    reporters = [reporter]
    observer = factory(
        transport=transport,
        journal=journal,
        lifecycle=lifecycle,
        diagnostics=diagnostics,
        reporters=reporters,
    )
    assert factory is observer_type
    assert observer.journal is journal
    assert observer.lifecycle is lifecycle
    assert observer.diagnostic_policy is diagnostics
    assert observer.reporters == (reporter,)
    reporters.clear()
    assert observer.reporters == (reporter,)
    other = factory(transport=transport)
    assert other is not observer
    assert other.journal is not observer.journal
    assert transport.open_count == transport.close_count == 0
    with pytest.raises(ConfigurationError):
        factory(transport=object())


def test_sync_factory_keeps_watch_lifetimes_explicit(sync_observer_factory):
    transport = FakeTransport()
    observer = sync_observer_factory(transport=transport)
    with observer.watch(source="events") as watcher:
        transport.publish(b"completed")
        assert watcher.wait_for(lambda payload: payload == b"completed", timeout=1) == b"completed"
    assert transport.open_count == transport.close_count == 1
    with pytest.raises(AssertionError, match="test failure"):
        with observer.watch(source="events"):
            raise AssertionError("test failure")
    assert transport.open_count == transport.close_count == 2


def test_async_factory_needs_no_async_pytest_fixture(async_observer_factory):
    transport = FakeTransport()
    observer = async_observer_factory(transport=transport)

    async def observe():
        async with observer.watch(source="events") as watcher:
            transport.publish(b"completed")
            assert await watcher.wait_for(lambda payload: payload == b"completed", timeout=1) == b"completed"

    asyncio.run(observe())
    assert transport.open_count == transport.close_count == 1


PROJECT_FIXTURES = """
import pytest
from event_observer.testing import FakeTransport

@pytest.fixture(scope="session")
def transport(sync_observer_factory, async_observer_factory):
    return FakeTransport()

@pytest.fixture(scope="session")
def session_events(sync_observer_factory, transport):
    return sync_observer_factory(transport=transport)

@pytest.fixture
def events(sync_observer_factory, transport):
    return sync_observer_factory(transport=transport)
"""

PROJECT_TEST = """
def test_observe(events, session_events, transport):
    assert events is not session_events
    with events.watch(source="events") as watcher:
        transport.publish(b"completed")
        assert watcher.wait_for(lambda payload: payload == b"completed", timeout=1) == b"completed"
    assert transport.open_count == transport.close_count == 1
"""


def test_entry_point_autoloads_factories_for_project_fixtures(pytester, monkeypatch):
    monkeypatch.delenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", raising=False)
    pytester.makeini("[pytest]")
    pytester.makeconftest(PROJECT_FIXTURES)
    pytester.makepyfile(PROJECT_TEST)
    result = pytester.runpytest_subprocess("-q")
    result.assert_outcomes(passed=1)


def test_entry_point_can_be_loaded_explicitly_without_autoload(pytester, monkeypatch):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    pytester.makeini("[pytest]")
    pytester.makeconftest(PROJECT_FIXTURES)
    pytester.makepyfile(PROJECT_TEST)
    result = pytester.runpytest_subprocess("-p", "event_observer", "-q")
    result.assert_outcomes(passed=1)


def test_plugin_can_be_disabled(pytester, monkeypatch):
    monkeypatch.delenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", raising=False)
    pytester.makeini("[pytest]")
    pytester.makepyfile("def test_disabled(sync_observer_factory): pass")
    result = pytester.runpytest_subprocess("-p", "no:event_observer", "-q")
    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*fixture 'sync_observer_factory' not found*"])


def test_project_can_override_factory_without_affecting_other_factories(pytester):
    pytester.makeini("[pytest]")
    pytester.makeconftest("""
        from functools import partial
        import pytest
        from event_observer import LifecyclePolicy

        @pytest.fixture(scope="session")
        def sync_observer_factory(sync_observer_factory):
            return partial(sync_observer_factory, lifecycle=LifecyclePolicy(startup_timeout=3))
    """)
    pytester.makepyfile("""
        from event_observer.testing import FakeTransport

        def test_override(sync_observer_factory, async_observer_factory):
            transport = FakeTransport()
            assert sync_observer_factory(transport=transport).lifecycle.startup_timeout == 3
            assert async_observer_factory(transport=transport).lifecycle.startup_timeout == 15
            assert transport.open_count == 0
    """)
    result = pytester.runpytest_subprocess("-p", "event_observer", "-q")
    result.assert_outcomes(passed=1)


def test_plugin_factories_remain_lazy_without_optional_dependencies(tmp_path):
    test_file = tmp_path / "test_minimal.py"
    test_file.write_text(
        """
from event_observer.transports.kafka import KafkaConfig
from event_observer.transports.rabbitmq import RabbitMQConfig
from event_observer.transports.redis_streams import RedisStreamsConfig

def test_factories(kafka_transport_factory, rabbitmq_transport_factory,
                   redis_streams_transport_factory, sync_observer_factory, async_observer_factory):
    for factory, config in (
        (kafka_transport_factory, KafkaConfig(bootstrap_servers="localhost:9092")),
        (rabbitmq_transport_factory, RabbitMQConfig(url="amqp://localhost/")),
        (redis_streams_transport_factory, RedisStreamsConfig(url="redis://localhost/0")),
    ):
        transport = factory(config)
        for create in (sync_observer_factory, async_observer_factory):
            observer = create(transport=transport)
            observer.watch(source="events")
""",
        encoding="utf-8",
    )
    script = """
import asyncio
import importlib.abc
import socket
import sys
import threading
import pytest

blocked = ("pytest_asyncio", "confluent_kafka", "aio_pika", "aiormq", "pamqp", "pika", "redis",
           "google.protobuf", "allure", "opentelemetry")
class NoOptionalDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise ImportError("optional dependency must remain lazy: " + fullname)
sys.meta_path.insert(0, NoOptionalDependencies())
def forbidden(*args, **kwargs):
    raise AssertionError("plugin/factory unexpectedly opened resources")
threading.Thread.start = forbidden
socket.socket.connect = forbidden
socket.create_connection = forbidden
asyncio.new_event_loop = forbidden
asyncio.run = forbidden
asyncio.Runner = forbidden
class RegistrationCheck:
    def pytest_configure(self):
        assert "event_observer.integrations._pytest_factories" in sys.modules
        assert "event_observer.aio" not in sys.modules
        for name in ("kafka", "rabbitmq", "redis_streams", "sync_kafka", "sync_rabbitmq", "sync_redis_streams"):
            assert "event_observer.transports." + name not in sys.modules
exit_code = pytest.main(["-p", "event_observer", "-c", "/dev/null", sys.argv[1], "-q"],
                        plugins=[RegistrationCheck()])
assert not any(name == prefix or name.startswith(prefix + ".")
               for prefix in blocked for name in sys.modules)
raise SystemExit(exit_code)
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script, str(test_file)],
        cwd=tmp_path,
        env={**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
