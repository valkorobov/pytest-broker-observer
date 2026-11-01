"""Реальная Kafka и AllureReporter: запуск с KAFKA_BOOTSTRAP_SERVERS и --alluredir."""

import asyncio
from collections.abc import Iterator
import os
from uuid import uuid4

import pytest

from event_observer import ScopeStart, SyncEventObserver, TransportSource
from event_observer.diagnostics import DiagnosticPolicy, ReportingMode
from event_observer.errors import EventTimeoutError
from event_observer.integrations.allure import AllureReporter
from event_observer.matchers import header_equals
from event_observer.testing import KafkaAdapterHarness

allure = pytest.importorskip("allure")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        reason="Set KAFKA_BOOTSTRAP_SERVERS to run the Kafka + Allure examples",
    ),
]


@pytest.fixture
def kafka_allure_source(
    request: pytest.FixtureRequest,
) -> Iterator[tuple[SyncEventObserver, KafkaAdapterHarness, TransportSource]]:
    harness = KafkaAdapterHarness(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    try:
        with allure.step("Создать уникальный Kafka topic для теста"):
            source = asyncio.run(harness.create_source(case_id=request.node.name))
        events = SyncEventObserver(
            transport=harness.adapter,
            reporters=[AllureReporter()],
            # Успешное ожидание также оставляет диагностические JSON-вложения.
            diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
        )
        yield events, harness, source
    finally:
        with allure.step("Удалить только Kafka topic этого теста"):
            asyncio.run(harness.cleanup())


@allure.feature("pytest-broker-observer")
@allure.story("Реальная Kafka через Docker Compose")
class TestKafkaAllureReporter:
    @allure.title("Kafka: событие после READY найдено по operation-id")
    def test_kafka_correlated_success(
        self,
        kafka_allure_source: tuple[SyncEventObserver, KafkaAdapterHarness, TransportSource],
    ):
        events, harness, source = kafka_allure_source
        operation_id = uuid4().hex.encode()

        with events.watch(source=source) as watcher:
            operation = watcher.scope(
                header_equals("operation-id", operation_id), start=ScopeStart.from_now()
            )
            with allure.step("Опубликовать completed независимым producer после READY"):
                # В приложении этот вызов заменяет действие тестируемого сервиса.
                receipt = asyncio.run(
                    harness.publish(
                        source,
                        b"completed",
                        key=operation_id,
                        headers=(("operation-id", operation_id),),
                    )
                )
                assert receipt.accepted

            completed = operation.wait_for_event(lambda payload: payload == b"completed", timeout=10)
            with allure.step("Проверить событие и подтверждённый брокером offset"):
                assert completed.payload == b"completed"
                assert completed.raw.source == source.name
                assert completed.raw.key == operation_id
                assert completed.raw.metadata["kafka.partition"] == receipt.metadata["kafka.partition"]
                assert completed.raw.metadata["kafka.offset"] == receipt.metadata["kafka.offset"]

    @allure.title("Kafka: started принят, timeout ожидания completed сохраняет mismatches")
    def test_kafka_expected_timeout(
        self,
        kafka_allure_source: tuple[SyncEventObserver, KafkaAdapterHarness, TransportSource],
    ):
        events, harness, source = kafka_allure_source
        operation_id = uuid4().hex.encode()

        with events.watch(source=source) as watcher:
            operation = watcher.scope(
                header_equals("operation-id", operation_id), start=ScopeStart.from_now()
            )
            with allure.step("Опубликовать started и дождаться broker acknowledgement"):
                receipt = asyncio.run(
                    harness.publish(source, b"started", headers=(("operation-id", operation_id),))
                )
                assert receipt.accepted

            # Сначала доказываем приём настоящей записи, исключая timeout пустого потока.
            assert operation.wait_for(lambda payload: payload == b"started", timeout=10) == b"started"

            # Independent ожидание снова видит started как mismatch; тест остаётся PASSED.
            with pytest.raises(EventTimeoutError):
                operation.wait_for(lambda payload: payload == b"completed", timeout=0.1)
