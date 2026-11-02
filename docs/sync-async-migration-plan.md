# Separate synchronous and asynchronous runtimes

Baseline: `431173c`. This plan implements an explicitly blocking, thread-owned
sync backend without creating an asyncio event loop. Async observation remains
available through the existing API. Models and semantic rules stay shared.

## 1. Architecture and compatibility

- [x] Update the architecture decision and SPEC to distinguish execution models.
- [x] Define blocking adapter/session/settlement protocols with finite polling,
  independent failure inspection, and bounded startup/cleanup.
- [x] Keep `SyncEventObserver`, watcher/scope methods, marks, return types, domain
  errors, caller-entry deadlines, and caller-thread Allure delivery.
- [x] Preserve existing async extension interfaces. Built-in adapters may expose
  a lazy `sync_adapter()` provider. Reject async-only third-party adapters with
  an actionable configuration error instead of running a hidden event loop.
- [x] Document native SDK threads separately from the sync coordination thread.

## 2. Shared expectation semantics

- [x] Extract scheduler-independent expectation state, bounded scanning,
  completeness checks, result reservations, final deadline range, and diagnostics.
- [x] Use the shared state machine from the async engine without changing its
  lifecycle or public contracts.
- [x] Cover distinct counting, filter inheritance, marks, result-byte limits,
  partial failure/cancellation, and terminal failure precedence.

## 3. Blocking runtime and synthetic transport

- [x] Replace the coroutine portal with a dedicated worker, bounded commands,
  completion/cancellation signals, and finite receive/control steps.
- [x] Keep native session ownership, decoding, admission, matching, marks,
  snapshots, and diagnostic capture on the worker thread.
- [x] Continue collection while the application/test thread runs other operations.
- [x] Keep waiters pending without blocking the worker's receive path.
- [x] Implement a blocking FakeTransport provider with independent bounded readers.
- [x] Verify that sync startup, observation, reporting, and teardown create no
  asyncio loop or task; verify real cancellation acknowledgement and honest
  abandoned cleanup for uninterruptible user/native code.

## 4. Native transports

- [x] Kafka: share native polling/start-boundary/probe/topology logic between
  async and blocking bridges; preserve native causes and bounded handoff.
- [x] Redis Streams: use synchronous redis-py with the same atomic metadata/read
  transaction, response preservation, cursor rules, and continuity checks.
- [x] RabbitMQ: add a pinned stable blocking client, dedicated subscription,
  heartbeats, cancellation detection, post-admission settlement, and deadline guards.
- [x] Keep optional clients lazy and preserve stdlib-only Core imports.
- [x] Add controlled-native and real-broker checks for each blocking adapter.

## 5. API, examples, contracts, and documentation

- [x] Export clear sync/async interfaces while retaining the existing public names.
- [x] Migrate runtime-private tests to the new coordination model without weakening
  public behavior assertions.
- [x] Exercise transport contracts through both execution models and document
  migration for custom adapters and synthetic test doubles.
- [x] Update pytest/Allure examples, guides, CI, and the source distribution.
- [x] Add English Sphinx docstrings and `:ivar:` descriptions for every new
  named source definition and explicit attribute; run the source audit.

## 6. Verification and independent review

- [x] Run focused regressions and the complete non-integration suite.
- [x] Run two-worker xdist and all three local Compose integration suites/examples.
- [x] Verify caller-thread redacted Allure attachments and one summary per wait.
- [x] Run Ruff, Pyrefly, Pyright, docstring audit, package build/metadata checks,
  and a clean stdlib-only installation with blocking observation.
- [x] Review the final cancellation, thread ownership, failure priority,
  resource limits, compatibility, and no-asyncio guarantees independently.
- [x] Resolve confirmed findings and record actual results and limitations.

## Completion record

All six stages are complete. Measured local results are recorded in
[status.md](status.md#separate-execution-backends-spec-15), and confirmed findings,
fixes, and practical boundaries are in the
[independent review](sync-async-migration-review.md).
