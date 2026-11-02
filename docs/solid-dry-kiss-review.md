# SOLID, DRY, and KISS follow-up review

Date: November 2, 2026. Baseline: commit `9408ae5`. The implementation follows
the [staged plan](improvement-plan.md). This document records the separate
review of the completed changes and their validation.

## Original findings and implementation

| Finding | Implemented change | Regression evidence |
| --- | --- | --- |
| Sync commands accepted after unacknowledged cancellation | Prepare a cancellation handle before publication and keep the command in flight until actual task completion. Queued and not-yet-started cancelled commands skip user code. | Blocking matcher, queued cancellation, first-step cancellation, and interruption before/after handoff. |
| Shutdown failures replace primary errors | Preserve ordinary context/operation errors and retain secondary cleanup failures. New caller interruption propagates after bounded cleanup. | Body, transport, startup, stop, dispatch, KeyboardInterrupt, SystemExit, and cancellation cases. |
| Diagnostics bypass injected copying and time | Give the collector the watcher's cloning callable and shared Clock; retain standalone defaults. | Custom non-deepcopyable payload, detached redaction, timestamps, and facade parity. |
| Kafka worker loses native causes | Preserve explicit causes and Python exception chaining across startup, receive, and cleanup failure transfer. | Native doubles cover typed/untyped failures and cleanup without exposing native messages in safe summaries. |
| Matched/mismatch retention policies diverge | Use one bounded first/last buffer; account for evicted active buffers and derive omissions from authoritative expectation totals. | Independent limits, lazy capture, truncation, interleaving, loss-ledger eviction, early report acknowledgement, and ID reuse. |
| Reporter-error bookkeeping diverges | Use one collector helper for failure counts and records; sync report delivery remains in the caller thread. | Failing reporters preserve results and maintain the same increasing counts in both facades. |
| Export repeatedly serializes every intermediate deletion | Select equivalent retained suffixes by bounded binary search, preserving byte accounting and output priority. | Exact legacy drop-order and byte-boundary checks, plus a serialization-work bound for 1,256 retained entries. |

## Documentation

Every named class, function, method, protocol member, overload, and nested
function in `src` has an English Sphinx docstring. The audit found **564
definitions and 539 explicitly declared attributes**, including properties.
Attributes use `:ivar:` and function parameters use `:param:`; return values
and domain errors are described where applicable.

`tools/check_docstrings.py` checks coverage without importing optional clients.
The CI quality job runs it. Presence checks complement semantic review: they
cannot establish that a description is correct or that all possible exceptions
have been enumerated. Lambdas and generated dataclass methods have no source
docstring requirement. Inherited attributes belong to their declaring class.
Documentation-only edits outside the approved behavior changes were compared
as executable ASTs with docstrings removed.

## Validation

| Check | Result |
| --- | --- |
| Tests, pytest examples, and Allure examples without brokers | 618 passed, 1 skipped, 78 integration cases deselected |
| Core with two pytest-xdist workers | 603 passed |
| Final Kafka, RabbitMQ, Redis Streams integration and examples | 71 passed, 7 capability skips, 16 broker-free examples deselected |
| Source documentation audit | 564 definitions and 539 attributes documented |
| Ruff lint/format, pre-commit, Pyrefly, Pyright | Passed |
| Wheel and source distribution, Twine metadata | Passed |
| Clean wheel without optional dependencies | Stdlib-only import, py.typed, marked sync observation, and cleanup passed |

The first complete broker run returned 70 passed, 7 skipped, and 1 failed.
`test_future_timestamp_falls_back_to_high_or_requires_existing` failed while
Kafka AdminClient queried partition leaders: native `_TIMED_OUT` (`-185`).
The original exception is now retained in its cause chain. The isolated recheck
passed, and the final full broker run returned 71 passed. No automatic retry or
timeout increase was added. The initial failure remains part of the evidence;
these local results do not establish absence of intermittent broker failures.

## Independent review outcome

Three independent read-only closing reviews were conducted after the final
validation. They covered the sync/async runtime, cancellation acknowledgement,
exception priorities, diagnostic retention/export, Kafka cause preservation,
Sphinx documentation, source-audit tooling, and this verification record.
No unresolved actionable findings remain within the agreed review scope.

The final design keeps small transport/decoder/reporter protocols, one shared
expectation engine, bounded sampling helpers, and explicit runtime dependencies.
The runtime cancellation bridge tracks actual completion, and diagnostics share
copying, time, sampling, error-accounting, and export-selection policies.

Review during implementation also identified and resolved cancellation before
the first task step, interrupted command handoff, primary-error replacement by
a cleanup dispatch failure, swallowed caller interruption, and loss-ledger
eviction combined with earlier automatic-report acknowledgement. Each behavior
has a focused regression.

The existing `description` wait argument remains reserved and unexported; its
docstring now states that limitation. This work does not add temporal assertions,
reconnect recovery, or guarantees about forcibly stopping blocking user code.
