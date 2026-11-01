"""Fixtures и явное включение намеренно падающего демонстрационного теста."""

import pytest

from event_observer import EventObserver, SyncEventObserver
from event_observer.diagnostics import DiagnosticPolicy, ReportingMode
from event_observer.integrations.allure import AllureReporter
from event_observer.testing import FakeTransport


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.getgroup("broker-observer-examples").addoption(
        "--allure-demo-failure",
        action="store_true",
        default=False,
        help="Run the intentionally failing Allure example (pytest exits with code 1)",
    )


@pytest.fixture
def transport() -> FakeTransport:
    return FakeTransport()


@pytest.fixture
def sync_events(transport: FakeTransport) -> SyncEventObserver:
    return SyncEventObserver(
        transport=transport,
        reporters=[AllureReporter()],
        # SUMMARY создаёт вложения и для успешных ожиданий.
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )


@pytest.fixture
def async_events(transport: FakeTransport) -> EventObserver:
    return EventObserver(
        transport=transport,
        reporters=[AllureReporter()],
        diagnostics=DiagnosticPolicy(mode=ReportingMode.SUMMARY),
    )


@pytest.fixture
def demo_failure_enabled(request: pytest.FixtureRequest) -> None:
    if not request.config.getoption("--allure-demo-failure"):
        pytest.skip("Use --allure-demo-failure to include an intentional red failure in the report")
