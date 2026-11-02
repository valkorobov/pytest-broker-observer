"""Bind per-observation report context independently from either scheduler."""

from collections.abc import Sequence

from .diagnostics import DiagnosticCollector, DiagnosticPolicy, DiagnosticReporter


def bind_observation_reporters(
    reporters: Sequence[DiagnosticReporter],
    policy: DiagnosticPolicy,
    diagnostics: DiagnosticCollector,
    *,
    transport: str,
    source: str,
) -> tuple[DiagnosticReporter, ...]:
    """Bind reporter prototypes with identical alias and failure policies in both modes.

    :param reporters: Caller-supplied prototypes, each independently bound where supported.
    :param policy: Shared disclosure and label-alias policy.
    :param diagnostics: Collector receiving safe binding-failure records.
    :param transport: Canonical transport name for the selected execution backend.
    :param source: Broker source name selected by the watcher.
    :returns: Successfully bound reporter instances without native resources or scheduler state.
    """
    bound = []
    for reporter in reporters:
        try:
            binder = getattr(reporter, "bind_observation", None)
            if callable(binder):
                bound.append(
                    binder(
                        transport=policy.aliases.get(transport, transport),
                        source=policy.aliases.get(source, source),
                    )
                )
            else:
                binder = getattr(reporter, "bind", None)
                bound.append(binder() if callable(binder) else reporter)
        except Exception:
            diagnostics.record("reporter_error", error_code="reporter_bind_failed")
    return tuple(bound)
