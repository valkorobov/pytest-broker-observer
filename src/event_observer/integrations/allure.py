"""Completed summary steps through Allure's public API."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from ..diagnostics import DiagnosticExport
from ..errors import MissingOptionalDependencyError


class AllureReporter:
    """Attach completed diagnostic reports to the current Allure test.

    Reusable prototypes bind independent labels and test identities per watcher. No
    Allure step remains open across a wait, an await, or a watcher context lifetime.

    :ivar _allure: Lazily imported Allure module providing step and attachment APIs.
    :ivar _manager: Allure plugin manager used to identify the currently active test.
    :ivar _test_uuid: Test identity captured for this reporter binding.
    :ivar _step_title: Bounded display title for completed summary steps.
    """

    def __init__(self) -> None:
        """Load the optional reporting API and capture the current test context.

        :returns: ``None`` after creating an unbound generic summary reporter.
        :raises MissingOptionalDependencyError: If the Allure integration extra is unavailable.
        """
        try:
            allure: Any = import_module("allure")
            commons: Any = import_module("allure_commons")
            plugin_manager = commons.plugin_manager
        except ImportError as error:
            raise MissingOptionalDependencyError("Allure reporting requires the 'allure' extra") from error
        self._allure = allure
        self._manager = plugin_manager
        self._test_uuid = self._current_test_uuid()
        self._step_title = "Event observation summary"

    def bind(self) -> AllureReporter:
        """Create a watcher-local reporter from a reusable prototype.

        :returns: An independent reporter capturing the active test identity.
        :raises MissingOptionalDependencyError: If Allure cannot be loaded for the new binding.
        """
        return type(self)()

    def bind_observation(self, *, transport: str, source: str) -> AllureReporter:
        """Bind bounded display labels without sharing context between watchers.

        :param transport: Technical transport display name, after any configured alias.
        :param source: Source display name, after any configured alias.
        :returns: A fresh reporter with normalized, bounded summary labels.
        """
        reporter = self.bind()
        # Labels are display text, never adapter repr/config. Bound before
        # normalizing whitespace so unusually large source names stay cheap.
        transport_label = " ".join(transport[:512].split())
        source_label = " ".join(source[:512].split())
        reporter._step_title = f"Event observation summary [{transport_label}: {source_label}]"
        return reporter

    def _current_test_uuid(self) -> Any:
        # Allure's listener exposes its reporter through the public plugin
        # manager. Reading the active test prevents a fixture's late reporter
        # from attaching a previous observation to the next test.
        """Read the active test identity through installed Allure listener plugins.

        :returns: The current test UUID, or ``None`` if no listener exposes an active test.
        """
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
        """Attach one completed report only while its owning test remains active.

        :param report: Immutable prepared attachments or a standalone metadata export.
        :returns: ``None`` after a short summary step, or immediately for an inactive test.
        :raises Exception: If Allure rejects a step or attachment; the observer isolates sink failures.
        """
        current = self._current_test_uuid()
        if self._test_uuid is None:
            self._test_uuid = current
        if current is None or current != self._test_uuid:
            return
        # No step context survives an await or spans the lifetime of a watcher.
        with self._allure.step(self._step_title):
            attachments = report.attachments or {"observer-summary.json": report.to_json()}
            for name, body in attachments.items():
                self._allure.attach(
                    body,
                    name=name,
                    attachment_type=(
                        self._allure.attachment_type.TEXT
                        if name.endswith(".txt")
                        else self._allure.attachment_type.JSON
                    ),
                )


__all__ = ["AllureReporter"]
