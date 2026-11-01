"""Completed summary steps through Allure's public API."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from ..diagnostics import DiagnosticExport
from ..errors import MissingOptionalDependencyError


class AllureReporter:
    def __init__(self) -> None:
        try:
            allure: Any = import_module("allure")
            commons: Any = import_module("allure_commons")
            plugin_manager = commons.plugin_manager
        except ImportError as error:
            raise MissingOptionalDependencyError("Allure reporting requires the 'allure' extra") from error
        self._allure = allure
        self._manager = plugin_manager
        self._test_uuid = self._current_test_uuid()

    def bind(self) -> AllureReporter:
        """Create a watcher-local binding from a reusable reporter prototype."""
        return type(self)()

    def _current_test_uuid(self) -> Any:
        # Allure's listener exposes its reporter through the public plugin
        # manager. Reading the active test prevents a fixture's late reporter
        # from attaching a previous observation to the next test.
        for plugin in self._manager.get_plugins():
            reporter = getattr(plugin, "allure_logger", None)
            get_test = getattr(reporter, "get_test", None)
            if callable(get_test):
                try:
                    test = get_test(None)
                except Exception:
                    continue
                if test is not None:
                    return getattr(test, "uuid", None)
        return None

    def emit(self, report: DiagnosticExport) -> None:
        current = self._current_test_uuid()
        if self._test_uuid is None:
            self._test_uuid = current
        if current is None or current != self._test_uuid:
            return
        # No step context survives an await or spans the lifetime of a watcher.
        with self._allure.step("Event observation summary"):
            attachments = report.attachments or {"observer-summary.json": report.to_json()}
            for name, body in attachments.items():
                self._allure.attach(
                    body,
                    name=name,
                    attachment_type=self._allure.attachment_type.JSON,
                )


__all__ = ["AllureReporter"]
