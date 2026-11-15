"""Exercise a clean installed candidate using its own Python interpreter.

Run with the target interpreter and ``-I``. The stdlib and pytest-only phases
require their explicitly minimal environments. Native smoke checks use actual
SDK classes and policies without opening broker connections; real transport
contracts remain separate integration evidence.
"""

import argparse
import asyncio
import importlib
import importlib.metadata
import importlib.resources
import importlib.util
import inspect
import json
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET


OPTIONAL_MODULES = (
    "pytest",
    "pytest_asyncio",
    "confluent_kafka",
    "aio_pika",
    "aiormq",
    "pika",
    "redis",
    "google",
    "allure",
    "opentelemetry",
    "testcontainers",
)
EXTRA_MODULES = {
    "kafka": {"confluent_kafka"},
    "rabbitmq": {"aio_pika", "aiormq", "pika"},
    "redis": {"redis"},
    "protobuf": {"google"},
    "pytest-asyncio": {"pytest", "pytest_asyncio"},
    "allure": {"pytest", "allure"},
    # Kafka testcontainers versions may bring a related native Kafka SDK.
    "contract-tests": {"pytest", "pytest_asyncio", "confluent_kafka", "testcontainers"},
}


def assert_installed():
    """Reject base interpreters and editable/source-checkout imports."""
    import event_observer

    assert sys.prefix != sys.base_prefix, "an isolated candidate environment is required"
    assert Path(event_observer.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()), (
        "candidate package must be installed inside the isolated environment"
    )
    assert importlib.resources.files(event_observer).joinpath("py.typed").is_file()


def forbidden(*args, **kwargs):
    """Fail if a constructor or disconnected SDK smoke attempts network I/O."""
    raise AssertionError("this installed smoke must not acquire network resources")


class StdlibInstallTests(unittest.TestCase):
    """Import, laziness and both ordinary stdlib observation backends."""

    def test_imports_and_constructors_need_only_stdlib(self):
        for name in OPTIONAL_MODULES:
            self.assertNotIn(name, sys.modules)
            self.assertIsNone(importlib.util.find_spec(name))
        with patch("threading.Thread.start", forbidden), patch.object(socket.socket, "connect", forbidden):
            assert_installed()
            from event_observer import EventObserver, SyncEventObserver
            from event_observer.integrations import pytest_plugin
            from event_observer.transports.kafka import KafkaAdapter, KafkaConfig
            from event_observer.transports.rabbitmq import RabbitMQAdapter, RabbitMQConfig
            from event_observer.transports.redis_streams import RedisStreamsAdapter, RedisStreamsConfig

            for adapter in (
                KafkaAdapter(KafkaConfig("localhost:9092")),
                RabbitMQAdapter(RabbitMQConfig("amqp://localhost/")),
                RedisStreamsAdapter(RedisStreamsConfig("redis://localhost/0")),
            ):
                EventObserver(transport=adapter).watch(source="events")
                SyncEventObserver(transport=adapter).watch(source="events")
            self.assertEqual(pytest_plugin.pytest_plugins, ["event_observer.integrations._pytest_factories"])
        for name in OPTIONAL_MODULES:
            self.assertNotIn(name, sys.modules)

    def test_blocking_observation_never_starts_asyncio(self):
        assert_installed()
        from event_observer import SyncEventObserver
        from event_observer.testing import FakeTransport

        transport = FakeTransport()
        with (
            patch.object(asyncio, "new_event_loop", forbidden),
            patch.object(asyncio, "run", forbidden),
            patch.object(asyncio, "Runner", forbidden),
        ):
            with SyncEventObserver(transport=transport).watch(source="events") as watcher:
                mark = watcher.mark()
                transport.publish(b"completed")
                self.assertEqual(
                    watcher.wait_for(lambda body: body == b"completed", since=mark, timeout=1), b"completed"
                )
        self.assertEqual(transport.open_count, transport.close_count)

    def test_async_observation_uses_the_callers_loop(self):
        assert_installed()
        from event_observer import EventObserver
        from event_observer.testing import FakeTransport

        transport = FakeTransport()

        async def scenario():
            async with EventObserver(transport=transport).watch(source="events") as watcher:
                mark = watcher.mark()
                transport.publish(b"completed")
                self.assertEqual(
                    await watcher.wait_for(lambda body: body == b"completed", since=mark, timeout=1),
                    b"completed",
                )

        asyncio.run(scenario())
        self.assertEqual(transport.open_count, transport.close_count)


class NativeInstallTests(unittest.TestCase):
    """Disconnected native SDK API checks, distinct from broker contracts."""

    def setUp(self):
        assert_installed()

    @patch.object(socket.socket, "connect", forbidden)
    def test_kafka_native_types_and_both_adapter_paths(self):
        from event_observer.transports import kafka, sync_kafka

        native = kafka._load_native()
        self.assertIs(sync_kafka._load_native(), native)
        partition = native.TopicPartition("events", 0, 7)
        self.assertEqual((partition.topic, partition.partition, partition.offset), ("events", 0, 7))
        for method in (
            "assign",
            "poll",
            "seek",
            "position",
            "get_watermark_offsets",
            "offsets_for_times",
            "list_topics",
            "close",
        ):
            self.assertTrue(callable(getattr(native.Consumer, method)))
        config = kafka.KafkaConfig("localhost:9092")
        self.assertEqual(
            kafka.KafkaAdapter(config).capabilities, sync_kafka.SyncKafkaAdapter(config).capabilities
        )

    @patch.object(socket.socket, "connect", forbidden)
    def test_rabbitmq_async_native_message_and_channel_api(self):
        from event_observer.transports import rabbitmq

        native = rabbitmq._load_native()
        message = native.Message(body=b"completed", headers={"marker": b"42"})
        self.assertEqual(message.body, b"completed")
        self.assertEqual(rabbitmq._normalize_headers(message.headers), (("marker", b"42"),))
        self.assertTrue(inspect.iscoroutinefunction(native.connect))
        for method in ("declare_exchange", "declare_queue", "set_qos", "close"):
            self.assertTrue(callable(getattr(native.Channel, method)))

    @patch.object(socket.socket, "connect", forbidden)
    def test_rabbitmq_blocking_native_parameters_and_channel_api(self):
        from event_observer.transports import sync_rabbitmq

        native = sync_rabbitmq._load_native()
        parameters = native.URLParameters("amqp://localhost/")
        self.assertEqual(parameters.host, "localhost")
        self.assertTrue(callable(native.BlockingConnection))
        channel = importlib.import_module("pika.adapters.blocking_connection").BlockingChannel
        for method in (
            "exchange_declare",
            "basic_qos",
            "queue_declare",
            "queue_bind",
            "basic_consume",
            "basic_ack",
            "basic_nack",
            "add_on_cancel_callback",
        ):
            self.assertTrue(callable(getattr(channel, method)))

    def test_redis_async_real_pool_retry_and_atomic_pipeline_api(self):
        from event_observer import TransportBufferLimits
        from event_observer.models import OpenContext
        from event_observer.transports import redis_streams

        # Windows may open a loopback socket when initializing asyncio's
        # self-pipe. Block client connections after that setup has finished.
        @patch.object(socket.socket, "connect", forbidden)
        async def scenario():
            native = redis_streams._load_native()
            retry = redis_streams._no_retry()
            config = redis_streams.RedisStreamsConfig("redis://localhost/0")
            context = OpenContext("compatibility", time.monotonic() + 5, TransportBufferLimits())
            options = redis_streams._client_options(config, context, retry, single_connection_client=True)
            client = native.Redis.from_url(config.url, **options)
            self.assertEqual(client.connection_pool.max_connections, 1)
            client.set_response_callback("XREAD", redis_streams._raw_xread)
            async with client.pipeline(transaction=True) as pipeline:
                pipeline.client_id()
                pipeline.info("server")
                pipeline.xinfo_stream("events")
                pipeline.xread({"events": "0-0"}, count=1)
                self.assertEqual(len(pipeline.command_stack), 4)
            await client.aclose()

        asyncio.run(scenario())

    @patch.object(socket.socket, "connect", forbidden)
    def test_redis_blocking_real_pool_retry_and_atomic_pipeline_api(self):
        from event_observer import TransportBufferLimits
        from event_observer.models import OpenContext
        from event_observer.transports import redis_streams, sync_redis_streams

        native = sync_redis_streams._load_native()
        retry = sync_redis_streams._no_retry()
        config = redis_streams.RedisStreamsConfig("redis://localhost/0")
        context = OpenContext("compatibility", time.monotonic() + 5, TransportBufferLimits())
        options = redis_streams._client_options(config, context, retry, single_connection_client=False)
        client = native.Redis.from_url(config.url, **options)
        self.addCleanup(client.close)
        self.assertEqual(client.connection_pool.max_connections, 1)
        client.set_response_callback("XREAD", redis_streams._raw_xread)
        with client.pipeline(transaction=True) as pipeline:
            pipeline.client_id()
            pipeline.info("server")
            pipeline.xinfo_stream("events")
            pipeline.xread({"events": "0-0"}, count=1)
            self.assertEqual(len(pipeline.command_stack), 4)


class OptionalInstallTests(unittest.TestCase):
    """Verify one independently installed extra without unrelated SDKs."""

    extra = ""

    def setUp(self):
        assert_installed()
        allowed = EXTRA_MODULES[self.extra]
        for name in OPTIONAL_MODULES:
            if name not in allowed:
                self.assertNotIn(name, sys.modules)
                self.assertIsNone(
                    importlib.util.find_spec(name), f"unrelated optional module installed: {name}"
                )

    def test_independently_installed_extra(self):
        if self.extra == "kafka":
            NativeInstallTests.test_kafka_native_types_and_both_adapter_paths(self)
        elif self.extra == "rabbitmq":
            NativeInstallTests.test_rabbitmq_async_native_message_and_channel_api(self)
            NativeInstallTests.test_rabbitmq_blocking_native_parameters_and_channel_api(self)
        elif self.extra == "redis":
            NativeInstallTests.test_redis_async_real_pool_retry_and_atomic_pipeline_api(self)
            NativeInstallTests.test_redis_blocking_real_pool_retry_and_atomic_pipeline_api(self)
        elif self.extra == "protobuf":
            from google.protobuf.wrappers_pb2 import StringValue

            from event_observer import RawEvent
            from event_observer.codecs.protobuf import ProtobufDecoder

            with patch.object(socket.socket, "connect", forbidden):
                message = StringValue(value="completed")
                decoded = ProtobufDecoder(StringValue).decode(RawEvent(message.SerializeToString(), "events"))
            self.assertIsInstance(decoded, StringValue)
            self.assertEqual(decoded.value, "completed")
        elif self.extra in {"pytest-asyncio", "allure", "contract-tests"}:
            self._pytest_feature()

    def _pytest_feature(self):
        if self.extra == "allure":
            body = """
from event_observer import SyncEventObserver
from event_observer.diagnostics import DiagnosticPolicy, ReportingMode
from event_observer.integrations.allure import AllureReporter
from event_observer.testing import FakeTransport

def test_real_allure_reporter():
    transport = FakeTransport()
    with SyncEventObserver(transport=transport, reporters=[AllureReporter()],
                           diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY)).watch(source="events") as watcher:
        transport.publish(b"completed")
        assert watcher.wait_for(lambda body: body == b"completed", timeout=1) == b"completed"
"""
        else:
            body = """
import pytest
from event_observer import EventObserver
from event_observer.testing import FakeTransport

@pytest.mark.asyncio
async def test_real_async_plugin():
    transport = FakeTransport()
    async with EventObserver(transport=transport).watch(source="events") as watcher:
        transport.publish(b"completed")
        assert await watcher.wait_for(lambda body: body == b"completed", timeout=1) == b"completed"
"""
            if self.extra == "contract-tests":
                body += """
from testcontainers.kafka import KafkaContainer
from event_observer.testing import KafkaAdapterHarness
assert callable(KafkaContainer.start)
assert KafkaAdapterHarness("localhost:9092").adapter is not None
"""
        with tempfile.TemporaryDirectory(prefix=f"observer-extra-{self.extra}-") as directory:
            root = Path(directory)
            (root / "test_feature.py").write_text(body, encoding="utf-8")
            config = root / "pytest.ini"
            config.write_text(
                "[pytest]\naddopts = --strict-markers --import-mode=importlib\n", encoding="utf-8"
            )
            junit = root / "feature.xml"
            command = [
                sys.executable,
                "-I",
                "-m",
                "pytest",
                "-c",
                str(config),
                "test_feature.py",
                "--junitxml",
                str(junit),
                "-o",
                "junit_family=xunit1",
                "-q",
            ]
            if self.extra == "allure":
                command += ["--alluredir", str(root / "allure-results")]
            process = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=60)
            self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
            cases = ET.parse(junit).findall(".//testcase")
            self.assertEqual(len(cases), 1)
            self.assertFalse(
                any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped"))
            )
            if self.extra == "allure":
                results = list((root / "allure-results").glob("*-result.json"))
                self.assertEqual(len(results), 1)
                report = json.loads(results[0].read_text(encoding="utf-8"))
                self.assertEqual(report["status"], "passed")
                self.assertTrue(any(step.get("attachments") for step in report["steps"]))


def unittest_phase(mode, junit, extra=None):
    """Run exact disconnected cases and preserve each outcome in JUnit XML."""
    case = {"stdlib": StdlibInstallTests, "native": NativeInstallTests, "optional": OptionalInstallTests}[
        mode
    ]
    if mode == "optional":
        case.extra = extra
    names = unittest.defaultTestLoader.getTestCaseNames(case)
    if mode == "stdlib":
        names.remove("test_imports_and_constructors_need_only_stdlib")
        names.insert(0, "test_imports_and_constructors_need_only_stdlib")
    suite = ET.Element(
        "testsuite", name=f"installed-{mode}", tests=str(len(names)), failures="0", errors="0", skipped="0"
    )
    failed = 0
    for name in names:
        result = unittest.TestResult()
        started = time.monotonic()
        case(name).run(result)
        node = ET.SubElement(
            suite,
            "testcase",
            classname=case.__name__,
            name=f"test_extra_{extra.replace('-', '_')}" if mode == "optional" else name,
            time=str(time.monotonic() - started),
        )
        for _, detail in result.failures + result.errors:
            failed += 1
            ET.SubElement(node, "failure", message="installed candidate check failed").text = detail
        for _, reason in result.skipped:
            failed += 1
            ET.SubElement(
                node, "failure", message="installed candidate check unexpectedly skipped"
            ).text = reason
    suite.set("failures", str(failed))
    ET.ElementTree(suite).write(junit, encoding="utf-8", xml_declaration=True)
    return len(names), failed


def pytest_only_phase(junit):
    """Collect two copied factory tests with pytest and no optional plugins."""
    fixture = Path(__file__).resolve().parents[1] / "tests/compatibility/pytest_only_fixture.py"
    with tempfile.TemporaryDirectory(prefix="observer-pytest-only-") as directory:
        root = Path(directory)
        shutil.copyfile(fixture, root / "test_installed_factories.py")
        config = root / "pytest.ini"
        config.write_text("[pytest]\naddopts = --strict-markers --import-mode=importlib\n", encoding="utf-8")
        process = subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                "pytest",
                "-c",
                str(config),
                "test_installed_factories.py",
                "--junitxml",
                str(junit),
                "-o",
                "junit_family=xunit1",
                "-q",
            ],
            cwd=root,
            check=False,
            timeout=60,
        )
    tree = ET.parse(junit)
    cases = tree.findall(".//testcase")
    failed = sum(any(case.find(tag) is not None for tag in ("failure", "error", "skipped")) for case in cases)
    if process.returncode or len(cases) != 2:
        failed = max(failed, 1)
    return len(cases), failed


def main():
    """Write local check evidence without asserting release-gate completion."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("stdlib", "pytest-only", "native", "optional"), required=True)
    parser.add_argument("--extra", choices=tuple(EXTRA_MODULES))
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--junitxml", type=Path, required=True)
    args = parser.parse_args()
    if (args.mode == "optional") != (args.extra is not None):
        parser.error("--extra is required only for the optional mode")
    junit = args.junitxml.absolute()
    junit.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": 1,
        "mode": args.mode,
        "extra": args.extra,
        "valid": False,
        "tests": 0,
        "failed": 0,
        "python": platform.python_version(),
        "platform": platform.system(),
        "architecture": platform.machine(),
    }
    try:
        report["tests"], report["failed"] = (
            pytest_only_phase(junit)
            if args.mode == "pytest-only"
            else unittest_phase(args.mode, junit, args.extra)
        )
        report["valid"] = report["tests"] > 0 and report["failed"] == 0
    except (OSError, ValueError, subprocess.TimeoutExpired, ET.ParseError):
        report["error"] = "installed check could not execute or produce valid evidence"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Installed {args.mode} checks: " + ("verified" if report["valid"] else "failed"))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
