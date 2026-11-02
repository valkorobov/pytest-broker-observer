# SOLID, DRY, and KISS improvement plan

This plan addresses the review of commit `9408ae5`. Implementation, documentation,
verification, and a separate review of the finished changes are distinct phases.

## 1. Sync runtime and exception priority

- [x] Keep an unacknowledged command in flight after its control timeout and reject
  further ordinary commands while the runtime is unresponsive.
- [x] Preserve the primary operation or transport failure when runtime shutdown
  also fails; retain the secondary failure as diagnostic information.
- [x] Add deterministic regressions using controlled blocking and shutdown failure.

## 2. Dependencies and transport errors

- [x] Preserve the original Kafka worker exception as the explicit and Python
  exception cause without exposing its message in safe diagnostics.
- [x] Use the watcher's payload-copy strategy and shared Clock in diagnostics,
  retaining defaults for standalone collectors.
- [x] Verify custom payload copying, redaction isolation, and injected timestamps.

## 3. Diagnostic policies and bounded export

- [x] Share first/last sample retention and omission accounting between matches
  and mismatches; report truncation consistently.
- [x] Share reporter-error accounting across facades while keeping synchronous
  reporter callbacks in the pytest caller thread.
- [x] Replace repeated full-report serialization during trimming with an efficient
  bounded selection while preserving byte limits, outcome priority, and report
  acknowledgement behavior.
- [x] Cover the policy changes and export work with focused regression tests.

## 4. Sphinx source documentation

- [x] Document every class and named function in `src`, including private,
  nested, asynchronous, protocol, overload, and special methods.
- [x] Use English summaries and Sphinx `:param:`, `:returns:`, and `:raises:`
  fields where applicable; document class and instance attributes with `:ivar:`.
- [x] Audit the source tree for missing docstrings and attribute documentation.
  Lambdas are excluded because Python does not support their docstrings.

## 5. Verification

- [x] Run focused regressions and the complete non-integration suite.
- [x] Run the core suite with two pytest-xdist workers.
- [x] Run affected Kafka, RabbitMQ, and Redis Streams examples against local Compose.
- [x] Run Ruff, Pyrefly, Pyright, source-documentation checks, and package checks.

## 6. Independent review after implementation

- [x] Review the finished runtime and diagnostics changes separately from implementation.
- [x] Reassess SOLID, DRY, KISS, resource bounds, exception priority, and API compatibility.
- [x] Check Sphinx documentation accuracy and completeness.
- [x] Resolve confirmed findings and record the final review and verification results.
