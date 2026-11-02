# Runtime separation review

Reviewed on November 2, 2026 against baseline `431173c`.
The implementation follows the [six-stage plan](sync-async-migration-plan.md)
and [SPEC revision 1.5](../SPEC.md#s31).

## Architecture

The async scheduler retains caller-loop ownership. The blocking scheduler uses
a worker thread, a one-slot command queue, and actual completion signals. It
does not construct an `EventObserver` or create/drive an asyncio runtime.
Shared expectation and admission policies have no scheduler dependency.
Kafka and Redis share their native parsing/boundary algorithms across bridges;
RabbitMQ uses aio-pika and Pika with shared configuration and normalization.
Reporter binding is also shared, while rendering stays on the caller thread.

Independent reviews covered the shared engine and facade, actor cancellation
and ownership, native Kafka/Redis bridges, and blocking RabbitMQ. The final
checks specifically examined failure-before-result commitment, command-slot
retention until actual cancellation acknowledgement, bounded buffers, cleanup
priority, optional imports, and custom adapter compatibility.

## Confirmed findings and fixes

- A malformed allocated blocking session could fail validation before its handle
  was owned for rollback. Ownership is now recorded before validation; a
  regression verifies exactly one synchronous close.
- A command could be published after final queue draining while shutdown was
  still running. Publication is now closed under the handoff lock before draining;
  a barrier regression verifies rejection during slow cleanup.
- An ordinary native close failure could mask an already known terminal
  observation failure. The terminal failure remains primary, with safe cleanup
  notes; new caller interruptions retain their interruption semantics.
- Kafka startup cancellation could leave a late readiness exception unobserved.
  The startup future now has a single cancellation owner; a regression captures
  the event loop's exception handler and verifies native cleanup.
- Kafka and Redis could return READY after their final native startup operation
  crossed the supplied absolute deadline. All four sync/async return boundaries
  now recheck the shared clock and roll back. Controlled-clock regressions fail
  when those guards are removed.
- Core also accepted late custom adapters or initial decoding beyond the
  absolute startup deadline. Both runtimes now check after taking ownership of
  the session and after initial reception/continuity checks, roll back once, and
  emit no READY record. Caller cancellation still outranks simultaneous expiry.
- Native blocking startup timeouts now use the same public
  `ObserverStartupTimeoutError` as async startup, retaining the original cause.
- An async settlement method could pass a callable-only check and return a
  discarded coroutine. Startup now rejects async settlement, and a hidden
  returned coroutine is closed and recorded as terminal configuration failure
  before a matching result can escape.
- A new interruption during failed-startup cleanup could be suppressed beneath
  the startup error. All four supported interruption classes now propagate;
  ordinary cleanup errors preserve the original startup failure. Public generic
  return annotations were also restored during the compatibility review.

The real RabbitMQ fault test initially attempted to delete an exclusive queue
from a separate AMQP connection, then through the management API. Both correctly
rejected exclusive access. The test now identifies the reader-owned connection
through its queue and unique consumer tag, closes only that connection, and
requires typed idle connection failure with broker reply code 320. The queue's
isolation policy was preserved. Controlled-native tests separately cover
unsolicited consumer cancellation.

The expanded full-broker run with administrative connection closure returned
110 passed, 14 capability skips,
and one test-infrastructure failure: RabbitMQ exposed partial management
statistics before the connection name became available. The helper now waits
for both queue-owner and consumer connection identities within its existing
deadline, rejects conflicting identities immediately, and keeps HTTP failures
visible. A controlled metadata progression check and a real isolated rerun
confirmed the correction before the final full run.

## Verification record

Final commands and measured results are recorded in [status.md](status.md).
Native doubles, real brokers, and clean installations are separate evidence;
CI configuration does not stand in for an executed CI matrix.

The final core review has no unresolved findings in the implemented scope.
The complete local suites passed after the last source changes: 778
non-integration cases, 763 core cases with two xdist workers, and 111 real-broker
cases with 14 explicit capability skips. Startup regressions include late
native readiness, custom open, initial decoding, cancellation, rollback, and
absence of a published READY record after expiry.

## Practical boundaries

- Custom async-only adapters need a blocking implementation or `sync_adapter()`
  provider for `SyncEventObserver`; their async use remains supported.
- Python callbacks and native code cannot be forcibly killed. Unacknowledged
  work retains its command slot, and an unjoined worker is reported as abandoned.
- Pika's bounded RPC guard uses a checked native API boundary and the selected
  `>=1.4.4,<1.5` range. Broader client-version support needs separate validation.
- Local verification uses CPython 3.14.7 on macOS arm64 and the pinned Compose
  brokers. Other Python/OS combinations, dependency floors, production ACL/TLS,
  full network faults, and load/RSS gates remain outside these local results.
