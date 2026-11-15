# SPEC requirement traceability

The machine-readable inventory is [traceability.json](traceability.json). It maps
SPEC revision 1.7 to exact existing test functions and explicit missing-check work.
It contains **216 source entries: 88 acceptance criteria, 125 MVP MUST paragraphs
or table rows, two normative-vocabulary entries, and one deferred temporal entry**.
RabbitMQ, Redis Streams, observation marks, split runtimes, and lazy pytest
constructor fixtures are included. [status.md](status.md) records actual runs;
this inventory does not mark RG-01 or any other release gate passed.

## Meaning and validation

Each entry preserves the exact stripped physical SPEC line, its section anchor,
and its UTF-8 SHA-256. IDs `MUST-sNN-NNN` count uppercase `MUST` lines in their
section in source order, including `MUST NOT`; `AC-NN` preserves the published
criterion. Multi-clause paragraphs remain one source entry. A source edit requires
reviewing its mapping and updating its hash; removing or adding an obligation
requires reviewing the inventory. The validator rejects omitted/duplicate/stale
source entries and test references that do not resolve to direct AST definitions.

Every MVP entry has a targeted existing selector, an explicit missing-check work
item with an expected outcome and plan stage, or both. A selector identifies a
check to run, without claiming that every clause of a composite paragraph has
already passed. Rows with no additional design gap still require actual candidate
execution, declared applicability, and review of the complete source obligation.
A source/hash match alone does not establish semantic coverage.

Selectors use source notation `path.py::Class::function` without parameter case
IDs. Direct functions are intentional: inherited reusable broker contract methods
are not invented as locally defined functions. The corresponding integration
modules still run their inherited contract nodes; the candidate evidence must
record the actual collected nodes and declared capability outcomes.

Each JSON check records its expected result, execution modes, platform
applicability, and evidence kind:

| Field | Interpretation |
| --- | --- |
| `unit` | Core, API, synthetic transport, reporting, or verifier regression; no claim about a real broker. |
| `native-double` | A controlled native client or harness; distinct from real-service evidence. |
| `integration` | A real-broker test/example; only a configured, reachable service and a recorded run can supply evidence. |
| `sync`, `async` | Applicable observation facade. `shared` identifies scheduler-neutral behavior or source/API checks. |
| `linux`, `macos`, `windows` | Required target applicability, not already verified platform support. Real-service entries target Linux; stages 2–4 refine exact profile/architecture and feasible platform smoke coverage. |

Missing infrastructure is an integration skip, not a pass. An explicitly
configured but unavailable broker fails. A double, a synthetic verifier fixture,
a command, a dependency range, and a configured CI job cannot substitute for
candidate-bound native or installed-artifact results. Preserve the first failure
separately from diagnostic reruns. The full release evidence contract remains in
[release-gate-evidence.md](release-gate-evidence.md).

Reproduce the structural audit with:

```sh
uv run --locked python tools/check_traceability.py
```

The tables below are a readable view of the JSON mapping. Full requirement text,
per-selector applicability, and expected results remain in the JSON. Work-item
stages refer to [release-gates-plan.md](release-gates-plan.md).

## Acceptance criteria: AC-01–AC-88

| Criterion | Expected outcome from SPEC | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| [AC-01](../SPEC.md#s25) | pytest, Kafka, Protobuf, and Allure are not imported | `tests/test_api.py::test_clean_core_and_plugin_entry_point_import_need_only_the_standard_library` | No additional check-design gap recorded; candidate execution still required |
| [AC-02](../SPEC.md#s25) | No network, tasks, consumer, or runtime thread | `tests/test_api.py::test_clean_core_and_plugin_entry_point_import_need_only_the_standard_library`<br>`tests/test_pytest_plugin.py::test_plugin_factories_remain_lazy_without_optional_dependencies` | No additional check-design gap recorded; candidate execution still required |
| [AC-03](../SPEC.md#s25) | Returns a READY watcher before the body executes | `tests/test_observer.py::test_lazy_ready_history_and_non_consuming_count`<br>`tests/test_sync_runtime.py::test_blocking_observation_never_creates_or_drives_an_asyncio_loop` | No additional check-design gap recorded; candidate execution still required |
| [AC-04](../SPEC.md#s25) | The same readiness semantics | `tests/test_observer.py::test_lazy_ready_history_and_non_consuming_count`<br>`tests/test_reliability.py::test_core_rejects_startup_that_exceeds_its_shared_clock_deadline` | No additional check-design gap recorded; candidate execution still required |
| [AC-05](../SPEC.md#s25) | READY without artificial publication | `tests/integration/test_kafka.py::test_empty_topic_ready_then_two_independent_readers_receive_immediate_publication` | No additional check-design gap recorded; candidate execution still required |
| [AC-06](../SPEC.md#s25) | Found without a startup race | `tests/integration/test_kafka.py::test_empty_topic_ready_then_two_independent_readers_receive_immediate_publication` | No additional check-design gap recorded; candidate execution still required |
| [AC-07](../SPEC.md#s25) | Not discarded | `tests/test_kafka.py::test_latest_boundary_survives_publication_during_readiness`<br>`tests/test_sync_kafka.py::test_latest_replays_publication_during_probe_and_owns_native_calls` | No additional check-design gap recorded; candidate execution still required |
| [AC-08](../SPEC.md#s25) | Explicit vector of start positions | `tests/integration/test_kafka.py::test_empty_topic_ready_then_two_independent_readers_receive_immediate_publication` | No additional check-design gap recorded; candidate execution still required |
| [AC-09](../SPEC.md#s25) | Do not change the selected start policy | `tests/test_kafka.py::test_latest_boundary_survives_publication_during_readiness` | [real-broker-faults](#real-broker-faults) (stage 4) |
| [AC-10](../SPEC.md#s25) | Available history is read; deleted history is not promised | `tests/integration/test_kafka.py::test_earliest_preserves_headers_tombstone_and_explicit_offsets` | No additional check-design gap recorded; candidate execution still required |
| [AC-11](../SPEC.md#s25) | Valid live start boundary | `tests/test_kafka.py::test_offset_at_high_is_valid_and_eof_does_not_end_stream` | [real-kafka-retention-compaction](#real-kafka-retention-compaction) (stage 4) |
| [AC-12](../SPEC.md#s25) | Typed failure, without auto reset | `tests/test_kafka.py::test_invalid_explicit_offsets_fail_without_reset`<br>`tests/integration/test_kafka.py::test_earliest_preserves_headers_tombstone_and_explicit_offsets` | No additional check-design gap recorded; candidate execution still required |
| [AC-13](../SPEC.md#s25) | Documented fallback/require_existing per partition | `tests/test_kafka.py::test_timestamp_missing_fallback_is_explicit`<br>`tests/integration/test_kafka.py::test_future_timestamp_falls_back_to_high_or_requires_existing` | [kafka-partition-leader-first-timeout](#kafka-partition-leader-first-timeout) (stage 4) |
| [AC-14](../SPEC.md#s25) | No promise of a permanent timestamp filter | `tests/integration/test_kafka.py::test_future_timestamp_falls_back_to_high_or_requires_existing` | No additional check-design gap recorded; candidate execution still required |
| [AC-15](../SPEC.md#s25) | No false loss inferred through offset arithmetic | `tests/test_kafka.py::test_replay_preserves_tombstones_headers_and_noncontiguous_offsets` | [real-kafka-retention-compaction](#real-kafka-retention-compaction) (stage 4) |
| [AC-16](../SPEC.md#s25) | Isolation and latest boundary verified | `tests/integration/test_kafka.py::test_read_committed_latest_uses_numeric_high_boundary_across_old_transaction`<br>`tests/test_kafka.py::test_read_committed_boundary_uses_physical_high_instead_of_consumer_lso` | No additional check-design gap recorded; candidate execution still required |
| [AC-17](../SPEC.md#s25) | Fail-closed after detection, rather than silent expansion | `tests/integration/test_kafka.py::test_partition_expansion_is_a_terminal_topology_failure`<br>`tests/test_kafka.py::test_full_handoff_does_not_block_topology_failure_or_close` | No additional check-design gap recorded; candidate execution still required |
| [AC-18](../SPEC.md#s25) | Independent connections/journals/cleanup | `tests/test_kafka.py::test_adapter_sessions_are_independent_and_cancelling_monitor_is_safe`<br>`tests/integration/test_kafka.py::test_empty_topic_ready_then_two_independent_readers_receive_immediate_publication` | [kafka-isolation-startup-first-failure](#kafka-isolation-startup-first-failure) (stage 4) |
| [AC-19](../SPEC.md#s25) | Independently receive the published record | No existing executable selector | [real-multiprocess-native-isolation](#real-multiprocess-native-isolation) (stage 4) |
| [AC-20](../SPEC.md#s25) | Distinguished without data corruption | `tests/test_decoders.py::test_raw_distinguishes_empty_and_null`<br>`tests/test_api.py::test_null_payload_and_empty_bytes_have_distinct_values` | No additional check-design gap recorded; candidate execution still required |
| [AC-21](../SPEC.md#s25) | Preserved in RawEvent without loss | `tests/test_api.py::test_raw_envelope_detaches_and_freezes_headers_and_metadata`<br>`tests/integration/test_kafka.py::test_earliest_preserves_headers_tombstone_and_explicit_offsets` | No additional check-design gap recorded; candidate execution still required |
| [AC-22](../SPEC.md#s25) | Generated class is restored automatically | `tests/test_protobuf_runtime.py::test_protobuf_facades_return_independent_messages` | No additional check-design gap recorded; candidate execution still required |
| [AC-23](../SPEC.md#s25) | Allowed for a compatible message schema | `tests/test_decoders.py::test_protobuf_empty_is_valid_and_null_is_error` | No additional check-design gap recorded; candidate execution still required |
| [AC-24](../SPEC.md#s25) | Documented initialization failure | `tests/test_decoders.py::test_proto2_required_initialization_policy` | No additional check-design gap recorded; candidate execution still required |
| [AC-25](../SPEC.md#s25) | Not advertised as automatic type detection | `tests/test_decoders.py::test_mixed_decoder_uses_explicit_schema_and_keeps_invalid_fatal` | [protobuf-runtime-gencode-matrix](#protobuf-runtime-gencode-matrix) (stage 2) |
| [AC-26](../SPEC.md#s25) | FAIL_FAST / SKIP_UNKNOWN_TYPE strictly according to policy | `tests/test_protobuf_runtime.py::test_unknown_types_are_filtered_but_known_types_remain_complete` | No additional check-design gap recorded; candidate execution still required |
| [AC-27](../SPEC.md#s25) | Not hidden by unknown-type skip | `tests/test_protobuf_runtime.py::test_ambiguous_type_is_fatal_even_under_skip_policy` | No additional check-design gap recorded; candidate execution still required |
| [AC-28](../SPEC.md#s25) | Does not change the Journal or another waiter | `tests/test_observer.py::test_mutation_isolation_predicate_result_and_snapshot`<br>`tests/test_expectation_engine.py::test_each_filter_matcher_and_returned_result_gets_an_independent_payload` | No additional check-design gap recorded; candidate execution still required |
| [AC-29](../SPEC.md#s25) | Does not change the canonical message | `tests/test_observer.py::test_mutation_isolation_predicate_result_and_snapshot`<br>`tests/test_protobuf_runtime.py::test_protobuf_facades_return_independent_messages` | No additional check-design gap recorded; candidate execution still required |
| [AC-30](../SPEC.md#s25) | Typed terminal failure, all waiters notified | `tests/test_observer.py::test_journal_failure_propagates_waiter_and_normal_exit` | No additional check-design gap recorded; candidate execution still required |
| [AC-31](../SPEC.md#s25) | Correct loss ranges and scoped completeness | `tests/test_observer.py::test_retention_loss_persists_for_new_waiters_but_explicit_scope_recovers` | No additional check-design gap recorded; candidate execution still required |
| [AC-32](../SPEC.md#s25) | No unrecorded gaps | `tests/test_expectation_engine.py::test_drop_newest_loss_admitted_after_deadline_does_not_invalidate_eligible_success`<br>`tests/test_observation_marks.py::test_a_mark_after_drop_newest_can_select_an_empty_complete_range` | No additional check-design gap recorded; candidate execution still required |
| [AC-33](../SPEC.md#s25) | Error before decode, without unlimited parsing | `tests/test_protobuf_runtime.py::test_hard_event_size_is_checked_before_decoder` | No additional check-design gap recorded; candidate execution still required |
| [AC-34](../SPEC.md#s25) | Conservatively incomplete, rather than restored completeness | `tests/test_reliability.py::test_loss_ledger_compaction_never_restores_old_completeness` | No additional check-design gap recorded; candidate execution still required |
| [AC-35](../SPEC.md#s25) | Resource error without unbounded state | `tests/test_observer.py::test_waiter_and_result_resource_limits`<br>`tests/test_observation_marks.py::test_scope_marks_preserve_nested_filters_identity_and_scope_quota` | No additional check-design gap recorded; candidate execution still required |
| [AC-36](../SPEC.md#s25) | First matching event is found | `tests/test_observer.py::test_lazy_ready_history_and_non_consuming_count`<br>`tests/test_expectation_engine.py::test_independent_cursors_reuse_history_without_consuming_records` | No additional check-design gap recorded; candidate execution still required |
| [AC-37](../SPEC.md#s25) | The same record is allowed | `tests/test_expectation_engine.py::test_independent_cursors_reuse_history_without_consuming_records` | No additional check-design gap recorded; candidate execution still required |
| [AC-38](../SPEC.md#s25) | N distinct records, one deadline, no repeated counting | `tests/test_observer.py::test_count_does_not_reuse_single_record`<br>`tests/test_expectation_engine.py::test_count_uses_distinct_records_and_preserves_scope_identity_and_effective_lower` | No additional check-design gap recorded; candidate execution still required |
| [AC-39](../SPEC.md#s25) | Counted as a separate record; no hidden deduplication | `tests/test_expectation_engine.py::test_count_uses_distinct_records_and_preserves_scope_identity_and_effective_lower` | No additional check-design gap recorded; candidate execution still required |
| [AC-40](../SPEC.md#s25) | Append between scan and sleep is not lost | `tests/test_reliability.py::test_append_between_empty_scan_and_sleep_cannot_lose_wakeup` | No additional check-design gap recorded; candidate execution still required |
| [AC-41](../SPEC.md#s25) | Explicit waiter error, not timeout | `tests/test_observer.py::test_waiter_errors_do_not_kill_reader_and_have_current_diagnostics`<br>`tests/test_matchers.py::test_predicate_requires_actual_bool`<br>`tests/test_matchers.py::test_callback_exception_is_chained_with_safe_summary` | No additional check-design gap recorded; candidate execution still required |
| [AC-42](../SPEC.md#s25) | Early configuration error | `tests/test_matchers.py::test_field_binding_rejects_typo_before_wait`<br>`tests/test_matchers.py::test_explicit_presence_never_uses_default_value_guessing` | No additional check-design gap recorded; candidate execution still required |
| [AC-43](../SPEC.md#s25) | on_type does not call an inapplicable predicate | `tests/test_matchers.py::test_on_type_never_calls_wrong_type_callback` | No additional check-design gap recorded; candidate execution still required |
| [AC-44](../SPEC.md#s25) | All compositions verified by a truth table | `tests/test_matchers.py::test_all_of_truth_table`<br>`tests/test_matchers.py::test_any_of_truth_table`<br>`tests/test_matchers.py::test_not_truth_table` | No additional check-design gap recorded; candidate execution still required |
| [AC-45](../SPEC.md#s25) | Do not expand the parent interval/filter | `tests/test_observer.py::test_scopes_from_now_and_nested_short_circuit`<br>`tests/test_observation_marks.py::test_scope_marks_preserve_nested_filters_identity_and_scope_quota` | No additional check-design gap recorded; candidate execution still required |
| [AC-46](../SPEC.md#s25) | Atomic runtime boundary, not publisher time | `tests/test_observer.py::test_scopes_from_now_and_nested_short_circuit`<br>`tests/test_observation_marks.py::test_reusable_mark_excludes_history_but_finds_events_admitted_during_the_action` | No additional check-design gap recorded; candidate execution still required |
| [AC-47](../SPEC.md#s25) | Does not lose incompleteness information | `tests/test_observer.py::test_retention_loss_persists_for_new_waiters_but_explicit_scope_recovers` | No additional check-design gap recorded; candidate execution still required |
| [AC-48](../SPEC.md#s25) | Can explicitly select a complete post-gap range | `tests/test_observer.py::test_retention_loss_persists_for_new_waiters_but_explicit_scope_recovers`<br>`tests/test_expectation_engine.py::test_new_lower_boundary_excludes_earlier_loss_without_mutating_journal` | No additional check-design gap recorded; candidate execution still required |
| [AC-49](../SPEC.md#s25) | Does not invoke the predicate again or duplicate the Journal | `tests/test_reporting.py::test_scope_timeout_reports_exclusion_without_repeating_predicate`<br>`tests/test_observation_marks.py::test_scope_marks_preserve_nested_filters_identity_and_scope_quota` | No additional check-design gap recorded; candidate execution still required |
| [AC-50](../SPEC.md#s25) | Correct eligibility and final scan | `tests/test_reliability.py::test_deadline_accepts_pre_and_exact_boundary_but_rejects_late_admission`<br>`tests/test_expectation_engine.py::test_final_scan_includes_exact_deadline_and_finishes_in_bounded_batches_after_deadline` | No additional check-design gap recorded; candidate execution still required |
| [AC-51](../SPEC.md#s25) | Known failure does not lose to success | `tests/test_reliability.py::test_known_failure_in_matcher_prevents_result_commit`<br>`tests/test_expectation_engine.py::test_final_commit_rechecks_terminal_state_after_matcher_success` | No additional check-design gap recorded; candidate execution still required |
| [AC-52](../SPEC.md#s25) | No retroactive result rewrite; normal exit observes failure | `tests/test_observer.py::test_failure_without_waiter_visible_at_exit`<br>`tests/test_allure_context.py::test_background_failure_after_success_is_reported_on_exit` | No additional check-design gap recorded; candidate execution still required |
| [AC-53](../SPEC.md#s25) | No false hard-timeout promise; diagnosable overrun | `tests/test_reliability.py::test_deadline_accepts_pre_and_exact_boundary_but_rejects_late_admission`<br>`tests/test_sync_runtime.py::test_blocked_matcher_does_not_allow_additional_runtime_commands` | [platform-subprocess-interruption](#platform-subprocess-interruption) (stage 4) |
| [AC-54](../SPEC.md#s25) | Context exit does not remain successful | `tests/test_observer.py::test_failure_without_waiter_visible_at_exit` | No additional check-design gap recorded; candidate execution still required |
| [AC-55](../SPEC.md#s25) | Rollback without forgotten resources | `tests/test_reliability.py::test_cancellation_while_transport_open_is_pending_rolls_back`<br>`tests/test_reliability.py::test_cancellation_after_session_open_closes_allocated_session`<br>`tests/test_kafka.py::test_startup_cancellation_rolls_back_native_worker` | [real-broker-faults](#real-broker-faults) (stage 4) |
| [AC-56](../SPEC.md#s25) | Primary error preserved, secondary diagnostic | `tests/test_observer.py::test_cleanup_timeout_and_original_exception_priority`<br>`tests/test_sync_runtime.py::test_known_observation_failure_wins_over_native_cleanup_failure` | No additional check-design gap recorded; candidate execution still required |
| [AC-57](../SPEC.md#s25) | One cleanup without double-free | `tests/test_redis_streams.py::test_close_is_idempotent_stops_polling_and_completes_waiters`<br>`tests/test_sync_rabbitmq.py::test_close_is_idempotent_and_does_not_ack_unsettled_deliveries`<br>`tests/test_rabbitmq.py::test_close_is_idempotent_and_does_not_ack_unsettled_deliveries` | [native-lifecycle-resource-counts](#native-lifecycle-resource-counts) (stage 4) |
| [AC-58](../SPEC.md#s25) | Others continue working | `tests/test_observer.py::test_hundred_waiters_with_half_cancelled` | No additional check-design gap recorded; candidate execution still required |
| [AC-59](../SPEC.md#s25) | Control path is not blocked by data capacity | `tests/test_kafka.py::test_full_handoff_does_not_block_topology_failure_or_close` | No additional check-design gap recorded; candidate execution still required |
| [AC-60](../SPEC.md#s25) | Reader continues receiving messages | `tests/test_observer.py::test_sync_reader_continues_while_service_blocks_and_no_threads_accumulate`<br>`tests/integration/test_kafka_observer.py::test_sync_kafka_protobuf_collection_during_blocking_application_action` | No additional check-design gap recorded; candidate execution still required |
| [AC-61](../SPEC.md#s25) | Value T, not a coroutine/Future | `tests/test_sync_runtime.py::test_blocking_observation_never_creates_or_drives_an_asyncio_loop` | No additional check-design gap recorded; candidate execution still required |
| [AC-62](../SPEC.md#s25) | Documented runtime execution, safe copies | `tests/test_reporting.py::test_sync_reports_in_caller_thread_after_runtime_callbacks`<br>`tests/test_sync_runtime.py::test_decoder_and_matcher_contexts_are_independent_and_never_leak_to_caller` | No additional check-design gap recorded; candidate execution still required |
| [AC-63](../SPEC.md#s25) | Explicit rejection, without nested loop/deadlock | `tests/test_observer.py::test_sync_inside_async_is_refused` | No additional check-design gap recorded; candidate execution still required |
| [AC-64](../SPEC.md#s25) | Affinity error, rather than state corruption | `tests/test_observer.py::test_sync_foreign_thread_and_reentrant_calls_fail_immediately`<br>`tests/test_observer.py::test_foreign_async_loop_refused`<br>`tests/test_reliability.py::test_active_sync_handle_rejects_foreign_process` | No additional check-design gap recorded; candidate execution still required |
| [AC-65](../SPEC.md#s25) | Controlled cancel; no forgotten coroutine with compliant callbacks | `tests/test_sync_runtime.py::test_acknowledged_cancellation_releases_the_runtime_for_next_command`<br>`tests/test_sync_runtime.py::test_interrupt_during_handoff_keeps_a_cancellable_submission` | [platform-subprocess-interruption](#platform-subprocess-interruption) (stage 4) |
| [AC-66](../SPEC.md#s25) | No accumulation of controlled tasks/threads | `tests/test_reliability.py::test_one_hundred_sync_lifecycles_leave_no_runtime_threads` | [native-lifecycle-resource-counts](#native-lifecycle-resource-counts) (stage 4) |
| [AC-67](../SPEC.md#s25) | Same domain results and errors | `tests/test_protobuf_runtime.py::test_protobuf_facades_return_independent_messages`<br>`tests/test_observation_marks.py::test_reusable_mark_excludes_history_but_finds_events_admitted_during_the_action` | [compatibility-matrix](#compatibility-matrix) (stage 2) |
| [AC-68](../SPEC.md#s25) | Immediate ReentrantSyncCallError | `tests/test_observer.py::test_sync_foreign_thread_and_reentrant_calls_fail_immediately`<br>`tests/test_observation_marks.py::test_sync_mark_preserves_thread_affinity_and_reentrant_call_guards` | No additional check-design gap recorded; candidate execution still required |
| [AC-69](../SPEC.md#s25) | Valid bounded JSON and explicit truncation flags | `tests/test_diagnostics.py::test_json_truncation_keeps_valid_json_and_counts`<br>`tests/test_diagnostics.py::test_total_export_budget_is_enforced_across_calls`<br>`tests/test_text_diagnostics.py::test_text_is_truncated_with_a_notice_and_all_files_respect_attachment_limits` | No additional check-design gap recorded; candidate execution still required |
| [AC-70](../SPEC.md#s25) | Payload/key/headers/expected secrets are not disclosed | `tests/test_diagnostics.py::test_default_diagnostics_omit_sensitive_content`<br>`tests/test_matched_diagnostics.py::test_metadata_only_match_capture_never_inspects_or_retains_payload` | [safe-artifact-secret-scan](#safe-artifact-secret-scan) (stage 5) |
| [AC-71](../SPEC.md#s25) | No fallback to raw content | `tests/test_diagnostics.py::test_redactor_failures_do_not_expose_original_payload`<br>`tests/test_reporting.py::test_broken_matched_payload_redactor_preserves_results_and_reader` | No additional check-design gap recorded; candidate execution still required |
| [AC-72](../SPEC.md#s25) | No mixing of parents/steps or lost outcome | `tests/test_allure_context.py::test_concurrent_waiters_each_create_one_distinct_allure_result`<br>`tests/test_allure_context.py::test_shared_allure_prototype_keeps_each_watchers_title_and_attachments` | [allure-runner-golden-matrix](#allure-runner-golden-matrix) (stage 5) |
| [AC-73](../SPEC.md#s25) | Does not change the match result; bounded safe error | `tests/test_reporting.py::test_reporter_errors_preserve_successful_result`<br>`tests/test_diagnostics.py::test_reporter_failure_does_not_escape` | No additional check-design gap recorded; candidate execution still required |
| [AC-74](../SPEC.md#s25) | Safe status; Protobuf continues working | `tests/test_diagnostics.py::test_trace_valid_missing_ambiguous_and_zero_ids`<br>`tests/test_diagnostics.py::test_invalid_trace_context_does_not_raise` | No additional check-design gap recorded; candidate execution still required |
| [AC-75](../SPEC.md#s25) | Correct IDs; export only through opt-in | `tests/test_diagnostics.py::test_trace_ids_require_explicit_export_opt_in`<br>`tests/test_diagnostics.py::test_trace_valid_missing_ambiguous_and_zero_ids` | No additional check-design gap recorded; candidate execution still required |
| [AC-76](../SPEC.md#s25) | Custom adapter integrates without modifying Core | `tests/test_sync_contracts.py::test_blocking_contract_execution_does_not_create_or_run_asyncio`<br>`tests/test_sync_contracts.py::test_undeclared_optional_cases_are_explicit_not_applicable_before_source_creation` | No additional check-design gap recorded; candidate execution still required |
| [AC-77](../SPEC.md#s25) | Traffic does not bypass the proxy through advertised endpoints | No existing executable selector | [advertised-endpoint-proxy](#advertised-endpoint-proxy) (stage 3) |
| [AC-78](../SPEC.md#s25) | A claimed capability cannot be hidden through skip | `tests/test_sync_contracts.py::test_undeclared_optional_cases_are_explicit_not_applicable_before_source_creation`<br>`tests/test_release_gates.py::test_declared_capability_precondition_is_explicitly_not_applicable` | No additional check-design gap recorded; candidate execution still required |
| [AC-79](../SPEC.md#s25) | Public imports and py.typed work | `tests/test_api.py::test_clean_core_and_plugin_entry_point_import_need_only_the_standard_library`<br>`tests/test_api.py::test_both_public_watch_factories_have_raw_decoder_and_message_overloads`<br>`tests/test_release_identity.py::test_each_artifact_requires_its_typing_marker` | [candidate-installed-artifact-matrix](#candidate-installed-artifact-matrix) (stage 7) |
| [AC-80](../SPEC.md#s25) | Clear MissingOptionalDependencyError | `tests/test_diagnostics.py::test_core_import_and_missing_optional_dependencies_in_clean_process`<br>`tests/test_rabbitmq.py::test_missing_optional_client_has_actionable_error` | No additional check-design gap recorded; candidate execution still required |
| [AC-81](../SPEC.md#s25) | Five session-scoped constructor fixtures; no hidden transport, observer, connection, or background work | `tests/test_pytest_plugin.py::test_entry_point_autoloads_factories_for_project_fixtures`<br>`tests/test_pytest_plugin.py::test_plugin_factories_remain_lazy_without_optional_dependencies` | No additional check-design gap recorded; candidate execution still required |
| [AC-82](../SPEC.md#s25) | All constructor fixtures register and sync observation works without pytest-asyncio or native broker clients | `tests/test_pytest_plugin.py::test_async_factory_needs_no_async_pytest_fixture`<br>`tests/test_pytest_plugin.py::test_entry_point_autoloads_factories_for_project_fixtures` | [candidate-installed-artifact-matrix](#candidate-installed-artifact-matrix) (stage 7) |
| [AC-83](../SPEC.md#s25) | Unique IDs, no shared mutable runtime | `tests/test_reliability.py::test_active_sync_handle_rejects_foreign_process`<br>`tests/test_reliability.py::test_active_async_watcher_rejects_foreign_process` | [real-multiprocess-native-isolation](#real-multiprocess-native-isolation) (stage 4) |
| [AC-84](../SPEC.md#s25) | The library's own queues and pending work are bounded | `tests/test_observer.py::test_waiter_and_result_resource_limits`<br>`tests/test_kafka.py::test_burst_cannot_accumulate_unbounded_handoff_or_block_close` | [bounded-resource-benchmark](#bounded-resource-benchmark) (stage 6) |
| [AC-85](../SPEC.md#s25) | Verified files, consistent version/tag/hash | `tests/test_release_gates.py::test_a_modified_distribution_invalidates_tested_artifact_identity`<br>`tests/test_release_gates.py::test_evidence_from_another_candidate_or_attempt_is_rejected`<br>`tests/test_release_gates.py::test_an_unexpected_distribution_cannot_be_uploaded_with_the_approved_pair`<br>`tests/test_release_identity.py::test_consistent_source_and_both_archives_have_verified_identity_and_hashes` | [candidate-archive-inventory-scan](#candidate-archive-inventory-scan) (stage 7) |
| [AC-86](../SPEC.md#s25) | No long-lived PyPI token; protected workflow/environment | No existing executable selector | [owner-publisher-rights-preflight](#owner-publisher-rights-preflight) (stage 7)<br>[owner-authorized-published-artifact-smoke](#owner-authorized-published-artifact-smoke) (stage 8) |
| [AC-87](../SPEC.md#s25) | Implemented and deferred features are not mixed | No existing executable selector | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |
| [AC-88](../SPEC.md#s25) | No internal names/schemas/credentials/source code | No existing executable selector | [candidate-archive-inventory-scan](#candidate-archive-inventory-scan) (stage 7) |

## MVP normative obligations

Every uppercase MVP MUST paragraph/table row has a separate inventory ID below.
These entries supplement the AC table rather than treating AC-01–AC-88 as an
exhaustive list of extension obligations.

### [SPEC section 2](../SPEC.md#s02)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s02-001` | Core operates outside pytest and imports with only the standard library. | `tests/test_api.py::test_clean_core_and_plugin_entry_point_import_need_only_the_standard_library` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s02-002` | Detected incomplete observation and the evaluated boundary remain explicit in diagnostics. | `tests/test_observer.py::test_retention_loss_persists_for_new_waiters_but_explicit_scope_recovers`<br>`tests/test_text_diagnostics.py::test_successful_wait_has_plain_explanation_and_configured_timeout` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 3](../SPEC.md#s03)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s03-001` | A later independent expectation finds already retained matching history. | `tests/test_expectation_engine.py::test_independent_cursors_reuse_history_without_consuming_records` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 4](../SPEC.md#s04)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s04-001` | Ordinary Core import does not import optional libraries or acquire resources. | `tests/test_api.py::test_clean_core_and_plugin_entry_point_import_need_only_the_standard_library` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s04-002` | Constructors/providers remain resource-free and runtime sessions remain independently owned. | `tests/test_pytest_plugin.py::test_plugin_factories_remain_lazy_without_optional_dependencies`<br>`tests/test_sync_kafka.py::test_factory_provider_shares_config_without_starting_clients` | [real-multiprocess-native-isolation](#real-multiprocess-native-isolation) (stage 4) |

### [SPEC section 5](../SPEC.md#s05)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s05-001` | Filtered/decode-failure records remain distinct from a legitimate null raw payload. | `tests/test_protobuf_runtime.py::test_unknown_types_are_filtered_but_known_types_remain_complete`<br>`tests/test_decoders.py::test_raw_distinguishes_empty_and_null` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s05-002` | Payload ownership uses independent copies and an explicit isolation failure. | `tests/test_decoders.py::test_payload_access_copies_mutable_python_and_protobuf`<br>`tests/test_decoders.py::test_payload_access_failure_is_explicit` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 6](../SPEC.md#s06)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s06-001` | Public watch overloads preserve typed raw/decoder/message results. | `tests/test_api.py::test_both_public_watch_factories_have_raw_decoder_and_message_overloads` | [installed-public-consumer-typing](#installed-public-consumer-typing) (stage 2) |
| `MUST-s06-002` | Watch construction is lazy and context entry yields a single-use READY watcher. | `tests/test_observer.py::test_lazy_ready_history_and_non_consuming_count`<br>`tests/test_api.py::test_clean_core_and_plugin_entry_point_import_need_only_the_standard_library` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 7](../SPEC.md#s07)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s07-001` | Timestamp units/timezone and complete explicit partition positions are validated. | `tests/test_api.py::test_timestamp_start_requires_explicit_clock_units_and_timezone`<br>`tests/test_kafka.py::test_invalid_explicit_offsets_fail_without_reset` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s07-002` | Failed/cancelled startup rolls back each allocated native resource. | `tests/test_reliability.py::test_cancellation_after_session_open_closes_allocated_session`<br>`tests/test_kafka.py::test_startup_cancellation_rolls_back_native_worker` | [real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s07-003` | Known later background failures surface at normal exit without rewriting returned results. | `tests/test_observer.py::test_failure_without_waiter_visible_at_exit`<br>`tests/test_allure_context.py::test_background_failure_after_success_is_reported_on_exit` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s07-004` | Offset/control gaps do not by themselves imply event loss. | `tests/test_kafka.py::test_replay_preserves_tombstones_headers_and_noncontiguous_offsets` | [real-kafka-retention-compaction](#real-kafka-retention-compaction) (stage 4) |

### [SPEC section 8](../SPEC.md#s08)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s08-001` | Resource limits are positive finite typed values and event/handoff limits are compatible. | `tests/test_api.py::test_integer_resource_limits_reject_coercion`<br>`tests/test_api.py::test_conflicting_and_unknown_policies_fail_before_io` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s08-002` | Payload sizing is deterministic and bounded; its published whole-envelope formula needs a consistency review. | `tests/test_decoders.py::test_payload_estimate_counts_dataclass_dict_and_inherited_private_slots`<br>`tests/test_decoders.py::test_payload_estimate_handles_cycles` | [published-accounting-formula-check](#published-accounting-formula-check) (stage 6) |
| `MUST-s08-003` | Accounting documentation distinguishes library byte estimates from total process/native RSS. | No existing executable selector | [published-accounting-formula-check](#published-accounting-formula-check) (stage 6)<br>[bounded-resource-benchmark](#bounded-resource-benchmark) (stage 6) |
| `MUST-s08-004` | Hidden transport overflow is terminal rather than silently applying a Journal DROP policy. | `tests/test_kafka.py::test_oversize_handoff_reports_failure_without_drop`<br>`tests/test_sync_redis_streams.py::test_handoff_capacity_loss_is_explicit_and_independently_visible` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s08-005` | Completeness is evaluated for the expectation-specific range and domain. | `tests/test_observation_marks.py::test_losses_before_the_mark_do_not_poison_the_new_complete_interval`<br>`tests/test_expectation_engine.py::test_new_lower_boundary_excludes_earlier_loss_without_mutating_journal` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s08-006` | Strict result commitment rechecks retained range completeness without unbounded private history. | `tests/test_expectation_engine.py::test_known_retention_loss_prevents_strict_success_but_not_explicit_best_effort` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 9](../SPEC.md#s09)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s09-001` | Blocking observation rejects async-only sessions instead of concealing them behind an event loop. | `tests/test_sync_api.py::test_async_only_extension_is_rejected_without_opening_it`<br>`tests/test_sync_api.py::test_blocking_open_cannot_return_a_hidden_coroutine` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s09-002` | Capability/replay documentation distinguishes bounded handoff from native RSS and broker retention. | No existing executable selector | [published-accounting-formula-check](#published-accounting-formula-check) (stage 6) |
| `MUST-s09-003` | Unsupported nullable/duplicate inputs require explicit typed capability preconditions; accepted data remains unchanged. | `tests/test_sync_contracts.py::test_undeclared_optional_cases_are_explicit_not_applicable_before_source_creation`<br>`tests/test_api.py::test_raw_envelope_detaches_and_freezes_headers_and_metadata` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s09-004` | Native sessions and resources have one explicit loop/thread owner and independent cleanup. | `tests/test_kafka.py::test_adapter_sessions_are_independent_and_cancelling_monitor_is_safe`<br>`tests/test_sync_rabbitmq.py::test_native_session_rejects_foreign_thread_and_process` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s09-005` | Startup rollback and repeated cleanup are explicit and idempotent. | `tests/test_reliability.py::test_cancellation_after_session_open_closes_allocated_session`<br>`tests/test_redis_streams.py::test_close_is_idempotent_stops_polling_and_completes_waiters` | [real-broker-faults](#real-broker-faults) (stage 4) |

### [SPEC section 10](../SPEC.md#s10)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s10-001` | Kafka Consumer operations stay serialized on the native worker and off the caller loop. | `tests/test_sync_kafka.py::test_latest_replays_publication_during_probe_and_owns_native_calls`<br>`tests/test_kafka.py::test_sync_core_continues_reading_kafka_during_blocking_application_action` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s10-002` | Readiness confirms fetch viability without requiring business data or closing unrelated open transactions. | `tests/integration/test_kafka.py::test_empty_topic_ready_then_two_independent_readers_receive_immediate_publication`<br>`tests/integration/test_kafka.py::test_read_committed_latest_uses_numeric_high_boundary_across_old_transaction` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s10-003` | Kafka baseline readiness/replay/topology scenarios exist; the complete specified native fault set remains open. | `tests/integration/test_kafka.py::test_empty_topic_ready_then_two_independent_readers_receive_immediate_publication`<br>`tests/integration/test_kafka.py::test_earliest_preserves_headers_tombstone_and_explicit_offsets`<br>`tests/integration/test_kafka.py::test_partition_expansion_is_a_terminal_topology_failure` | [real-broker-faults](#real-broker-faults) (stage 4)<br>[real-kafka-retention-compaction](#real-kafka-retention-compaction) (stage 4)<br>[kafka-isolation-startup-first-failure](#kafka-isolation-startup-first-failure) (stage 4)<br>[kafka-partition-leader-first-timeout](#kafka-partition-leader-first-timeout) (stage 4) |

### [SPEC section 11](../SPEC.md#s11)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s11-001` | Empty compatible Protobuf bytes remain valid while null has a typed decode error. | `tests/test_decoders.py::test_protobuf_empty_is_valid_and_null_is_error` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 12](../SPEC.md#s12)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s12-001` | Explicit on_type guards prevent unsafe mixed-type callbacks; all_of may still collect all branch diagnostics. | `tests/test_matchers.py::test_on_type_never_calls_wrong_type_callback`<br>`tests/test_matchers.py::test_all_evaluates_every_branch_to_collect_diagnostics` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s12-002` | Callback failures remain explicit and independent branches receive isolated payloads. | `tests/test_matchers.py::test_callback_exception_is_chained_with_safe_summary`<br>`tests/test_matchers.py::test_composition_isolates_mutating_user_branches` | [callback-contract-and-timing](#callback-contract-and-timing) (stage 4) |

### [SPEC section 14](../SPEC.md#s14)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s14-001` | A late failure preserves the committed result but fails otherwise normal context exit. | `tests/test_observer.py::test_failure_without_waiter_visible_at_exit`<br>`tests/test_allure_context.py::test_background_failure_after_success_is_reported_on_exit` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s14-002` | Slow callbacks remain controlled pending work; bounded timing diagnostics and documentation need explicit audit. | `tests/test_sync_runtime.py::test_blocked_matcher_does_not_allow_additional_runtime_commands` | [platform-subprocess-interruption](#platform-subprocess-interruption) (stage 4)<br>[callback-contract-and-timing](#callback-contract-and-timing) (stage 4) |

### [SPEC section 15](../SPEC.md#s15)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s15-001` | Exit stops new work, finishes owned waits and cleans up resources under bounded ownership. | `tests/test_observer.py::test_close_wakes_and_finishes_waiter`<br>`tests/test_sync_runtime.py::test_worker_closes_publication_before_entering_slow_final_cleanup` | [native-lifecycle-resource-counts](#native-lifecycle-resource-counts) (stage 4) |
| `MUST-s15-002` | Terminal observation failure remains primary at normal exit over ordinary native cleanup failure. | `tests/test_sync_runtime.py::test_known_observation_failure_wins_over_native_cleanup_failure`<br>`tests/test_observer.py::test_failure_without_waiter_visible_at_exit` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 16](../SPEC.md#s16)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s16-001` | Both schedulers drive neutral admission/expectation rules rather than divergent semantic implementations. | `tests/test_expectation_engine.py::test_engine_imports_no_scheduler_or_threading_module`<br>`tests/test_expectation_engine.py::test_final_commit_rechecks_terminal_state_after_matcher_success` | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |
| `MUST-s16-002` | Sync observation never creates or drives a private asyncio runtime. | `tests/test_sync_runtime.py::test_blocking_observation_never_creates_or_drives_an_asyncio_loop` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s16-003` | Async-only extensions require an explicit blocking provider and never select a hidden loop. | `tests/test_sync_api.py::test_async_only_extension_is_rejected_without_opening_it` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s16-004` | The actor continues collection and owns decoding, Journal updates and business callbacks. | `tests/test_sync_runtime.py::test_worker_keeps_collecting_while_caller_has_no_pending_command`<br>`tests/test_sync_runtime.py::test_decoder_and_matcher_contexts_are_independent_and_never_leak_to_caller` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s16-005` | Native thread/task/handle counts remain observable and agree with documented per-backend ownership. | No existing executable selector | [native-lifecycle-resource-counts](#native-lifecycle-resource-counts) (stage 4)<br>[bounded-resource-benchmark](#bounded-resource-benchmark) (stage 6) |
| `MUST-s16-006` | The command handoff remains bounded with one tracked caller request and original deadline. | `tests/test_sync_runtime.py::test_blocked_matcher_does_not_allow_additional_runtime_commands`<br>`tests/test_sync_runtime.py::test_cancellation_of_a_submission_that_has_not_started_is_acknowledged` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s16-007` | Only actual command completion releases reservations and permits the next request. | `tests/test_sync_runtime.py::test_acknowledged_cancellation_releases_the_runtime_for_next_command`<br>`tests/test_sync_runtime.py::test_cancellation_between_dequeue_and_first_step_skips_operation` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s16-008` | Returned payloads are detached and completed/failing expectations release accounted reservations. | `tests/test_observer.py::test_mutation_isolation_predicate_result_and_snapshot`<br>`tests/test_expectation_engine.py::test_failed_result_copy_releases_its_prechecked_reservation` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s16-009` | Callbacks use runtime ownership independently from the chosen native broker. | `tests/test_sync_runtime.py::test_decoder_and_matcher_contexts_are_independent_and_never_leak_to_caller`<br>`tests/test_reporting.py::test_sync_reports_in_caller_thread_after_runtime_callbacks` | [compatibility-matrix](#compatibility-matrix) (stage 2) |
| `MUST-s16-010` | Thread-affine/reentrant blocking API misuse is rejected and arbitrary callbacks are not preempted. | `tests/test_observer.py::test_sync_foreign_thread_and_reentrant_calls_fail_immediately`<br>`tests/test_observation_marks.py::test_sync_mark_preserves_thread_affinity_and_reentrant_call_guards` | [platform-subprocess-interruption](#platform-subprocess-interruption) (stage 4) |
| `MUST-s16-011` | Startup/per-command contexts are copied without leaking changes or thread-local identity to the caller. | `tests/test_sync_runtime.py::test_decoder_and_matcher_contexts_are_independent_and_never_leak_to_caller` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s16-012` | Domain deadlines differ from control responsiveness, and cancellation acknowledgement tracks actual completion. | `tests/test_sync_runtime.py::test_blocked_matcher_does_not_allow_additional_runtime_commands`<br>`tests/test_sync_runtime.py::test_acknowledged_cancellation_releases_the_runtime_for_next_command` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s16-013` | Prepared reporting cannot mask results or duplicate acknowledged waits; blocked-command reporting needs dedicated coverage. | `tests/test_allure_context.py::test_one_successful_wait_creates_one_allure_step_even_after_close`<br>`tests/test_reporting.py::test_reporter_errors_preserve_successful_result` | [platform-subprocess-interruption](#platform-subprocess-interruption) (stage 4)<br>[allure-runner-golden-matrix](#allure-runner-golden-matrix) (stage 5) |
| `MUST-s16-014` | Common scripts retain corresponding outcomes across both facades. | `tests/test_protobuf_runtime.py::test_protobuf_facades_return_independent_messages`<br>`tests/test_observation_marks.py::test_reusable_mark_excludes_history_but_finds_events_admitted_during_the_action` | [compatibility-matrix](#compatibility-matrix) (stage 2) |
| `MUST-s16-015` | Deterministic sync lifecycle/cancellation cases exist; process-level and real-native coverage remains required. | `tests/test_sync_runtime.py::test_blocking_observation_never_creates_or_drives_an_asyncio_loop`<br>`tests/test_sync_runtime.py::test_interrupt_during_handoff_keeps_a_cancellable_submission`<br>`tests/test_reliability.py::test_one_hundred_sync_lifecycles_leave_no_runtime_threads` | [platform-subprocess-interruption](#platform-subprocess-interruption) (stage 4)<br>[native-lifecycle-resource-counts](#native-lifecycle-resource-counts) (stage 4)<br>[compatibility-matrix](#compatibility-matrix) (stage 2) |

### [SPEC section 17](../SPEC.md#s17)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s17-001` | Wait diagnostics distinguish empty/nonmatching/excluded/error/count/completeness outcomes. | `tests/test_text_diagnostics.py::test_timeout_explanation_distinguishes_missing_and_nonmatching_events`<br>`tests/test_reporting.py::test_scope_timeout_reports_exclusion_without_repeating_predicate`<br>`tests/test_text_diagnostics.py::test_error_and_cancelled_waits_do_not_claim_success` | [allure-runner-golden-matrix](#allure-runner-golden-matrix) (stage 5) |
| `MUST-s17-002` | Concurrent waiters cannot rely on an automatically task-local Allure parent stack. | `tests/test_allure_context.py::test_concurrent_waiters_each_create_one_distinct_allure_result` | [allure-runner-golden-matrix](#allure-runner-golden-matrix) (stage 5) |
| `MUST-s17-003` | Reporter output is bound to its active test and cannot leak into the next context. | `tests/test_diagnostics.py::test_allure_emits_completed_steps_only_for_bound_active_test`<br>`tests/test_allure_context.py::test_shared_allure_prototype_keeps_each_watchers_title_and_attachments` | [allure-runner-golden-matrix](#allure-runner-golden-matrix) (stage 5) |
| `MUST-s17-004` | Matched/mismatched samples share quotas, preserve status/identity, and do not affect wait outcomes. | `tests/test_matched_diagnostics.py::test_content_capture_budget_is_shared_across_matches_mismatches_and_expectations`<br>`tests/test_matched_diagnostics.py::test_large_matched_content_is_omitted_before_the_expectation_outcome_is_dropped`<br>`tests/test_reporting.py::test_cancelled_count_retains_partial_match_content_and_releases_results` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s17-005` | Redaction uses independent bounded JSON copies and fails closed without raw fallback. | `tests/test_diagnostics.py::test_redactor_runs_on_detached_copy_and_size_is_bounded`<br>`tests/test_matched_diagnostics.py::test_nested_non_json_content_is_omitted_without_repr_or_raw_fallback` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 19](../SPEC.md#s19)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s19-001` | Lazy factories require no async plugin; active sessions retain compatible event-loop ownership. | `tests/test_pytest_plugin.py::test_async_factory_needs_no_async_pytest_fixture`<br>`tests/test_observer.py::test_foreign_async_loop_refused` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s19-002` | Five factory fixtures return canonical constructors rather than cached configured objects. | `tests/test_pytest_plugin.py::test_transport_factories_preserve_constructors_and_create_distinct_adapters`<br>`tests/test_pytest_plugin.py::test_observer_factories_preserve_constructors_and_policies` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s19-003` | Pytest registration remains lazy, stdlib-only at entry-point import, and resource-free without hidden fixtures or configuration. | `tests/test_pytest_plugin.py::test_plugin_factories_remain_lazy_without_optional_dependencies`<br>`tests/test_api.py::test_clean_core_and_plugin_entry_point_import_need_only_the_standard_library` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 20](../SPEC.md#s20)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s20-001` | Typed errors retain safe summaries and native cause chains; exhaustive catalog/identity checks remain to be specified. | `tests/test_matchers.py::test_callback_exception_is_chained_with_safe_summary`<br>`tests/test_rabbitmq.py::test_native_errors_are_typed_sanitized_and_chained` | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |

### [SPEC section 21](../SPEC.md#s21)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s21-001` | Synthetic barriers reproduce readiness, end/failure, oversize and startup/cleanup races without hiding adapter behavior. | `tests/test_reliability.py::test_cancellation_while_transport_open_is_pending_rolls_back`<br>`tests/test_observer.py::test_failure_without_waiter_visible_at_exit`<br>`tests/test_protobuf_runtime.py::test_hard_event_size_is_checked_before_decoder` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s21-002` | Kafka proxy tests prove there is no advertised-endpoint bypass. | No existing executable selector | [advertised-endpoint-proxy](#advertised-endpoint-proxy) (stage 3) |
| `MUST-s21-003` | Known markers exercise safe export/redactor behavior; consolidate every required marker location and artifact scan. | `tests/test_diagnostics.py::test_default_diagnostics_omit_sensitive_content`<br>`tests/test_diagnostics.py::test_redactor_failures_do_not_expose_original_payload` | [safe-artifact-secret-scan](#safe-artifact-secret-scan) (stage 5) |

### [SPEC section 22](../SPEC.md#s22)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s22-001` | Every MVP MUST has a precise executable check or an explicit staged missing-check work item before release closure. | No existing executable selector | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |

### [SPEC section 23](../SPEC.md#s23)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s23-001` | Published dependency ranges must have reproducible verified lower boundaries. | No existing executable selector | [verified-dependency-floors](#verified-dependency-floors) (stage 2) |
| `MUST-s23-002` | Published typing markers exist; actual candidate archive contents remain separately checked. | `tests/test_api.py::test_both_public_watch_factories_have_raw_decoder_and_message_overloads`<br>`tests/test_release_identity.py::test_each_artifact_requires_its_typing_marker`<br>`tests/test_release_identity.py::test_archived_license_cannot_differ_from_verified_source` | [candidate-installed-artifact-matrix](#candidate-installed-artifact-matrix) (stage 7)<br>[candidate-archive-inventory-scan](#candidate-archive-inventory-scan) (stage 7) |
| `MUST-s23-003` | Candidate version/source/run/hash and mandatory evidence constrain privileged publication. | `tests/test_release_gates.py::test_evidence_from_another_candidate_or_attempt_is_rejected`<br>`tests/test_release_gates.py::test_a_modified_distribution_invalidates_tested_artifact_identity`<br>`tests/test_release_gates.py::test_every_mandatory_gate_requires_its_own_evidence` | [candidate-archive-inventory-scan](#candidate-archive-inventory-scan) (stage 7)<br>[owner-publisher-rights-preflight](#owner-publisher-rights-preflight) (stage 7) |
| `MUST-s23-004` | Release identity/license/rights and production publication remain explicit owner decisions. | `tests/test_release_identity.py::test_source_identity_cannot_drift_from_owner_approved_release` | [owner-publisher-rights-preflight](#owner-publisher-rights-preflight) (stage 7)<br>[owner-authorized-published-artifact-smoke](#owner-authorized-published-artifact-smoke) (stage 8) |

### [SPEC section 24](../SPEC.md#s24)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s24-001` | Public examples require candidate-installed functional/type checks and accurate implemented/deferred documentation. | `tests/test_pytest_plugin.py::test_entry_point_autoloads_factories_for_project_fixtures` | [installed-public-consumer-typing](#installed-public-consumer-typing) (stage 2)<br>[candidate-installed-artifact-matrix](#candidate-installed-artifact-matrix) (stage 7)<br>[complete-traceability-audit](#complete-traceability-audit) (stage 7) |

### [SPEC section 25](../SPEC.md#s25)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s25-001` | The repository maintains a machine-readable complete requirement-to-check/gap inventory. | No existing executable selector | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |

### [SPEC section 26](../SPEC.md#s26)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s26-001` | An incompatible promised contract preserves a reproducer and requires revised support/SPEC rather than silent weakening. | No existing executable selector | [verified-dependency-floors](#verified-dependency-floors) (stage 2)<br>[complete-traceability-audit](#complete-traceability-audit) (stage 7) |

### [SPEC section 28](../SPEC.md#s28)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s28-001` | RabbitMQ optional imports and native construction are deferred until open. | `tests/test_rabbitmq.py::test_optional_client_is_not_imported_by_module_or_factory`<br>`tests/test_sync_rabbitmq.py::test_provider_is_lazy_and_keeps_canonical_capabilities` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-002` | RabbitMQ URL, binding short-string and bounded integer prefetch configuration is validated without credential disclosure. | `tests/test_rabbitmq.py::test_config_rejects_invalid_inputs_without_disclosing_values`<br>`tests/test_sync_rabbitmq.py::test_invalid_adapter_configuration_is_typed` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-003` | The selected native Pika API enforces lifecycle deadlines and rejects an unsupported boundary. | `tests/test_sync_rabbitmq.py::test_pinned_native_rpc_flush_processes_deadline_and_owned_socket_abort`<br>`tests/test_sync_rabbitmq.py::test_each_startup_rpc_uses_native_deadline_and_aborts_its_owned_stream` | [verified-dependency-floors](#verified-dependency-floors) (stage 2) |
| `MUST-s28-004` | Observation verifies the existing exchange and owns only its dedicated exclusive subscription. | `tests/test_sync_rabbitmq.py::test_ready_requires_every_binding_and_consume_ok_and_uses_only_private_queue`<br>`tests/integration/test_rabbitmq_observer.py::test_observer_does_not_consume_or_delete_an_existing_application_queue` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-005` | Rabbit native operations respect session ownership and blocking observation uses no private loop. | `tests/test_sync_rabbitmq.py::test_native_session_rejects_foreign_thread_and_process`<br>`tests/test_sync_rabbitmq.py::test_driver_does_not_create_or_run_an_asyncio_loop` | [compatibility-matrix](#compatibility-matrix) (stage 2) |
| `MUST-s28-006` | READY follows exchange/QoS/queue/bind/consume confirmation without synthetic business publication. | `tests/test_rabbitmq.py::test_readiness_waits_for_bindings_and_consume_ok`<br>`tests/test_sync_rabbitmq.py::test_ready_requires_every_binding_and_consume_ok_and_uses_only_private_queue` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-007` | Rabbit auth/ACL/source/subscription errors are typed visible failures, not unsupported capability skips. | `tests/test_rabbitmq.py::test_native_errors_are_typed_sanitized_and_chained`<br>`tests/test_sync_rabbitmq.py::test_native_exchange_errors_are_typed_and_partial_connection_closes` | [real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s28-008` | Rabbit observation accepts latest-only exchange subscriptions without invented replay/offset semantics. | `tests/test_rabbitmq.py::test_unsupported_selection_is_rejected_before_connect`<br>`tests/test_sync_rabbitmq.py::test_unsupported_native_start_is_rejected_before_import` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-009` | Rabbit preserves accepted empty bytes and separate routing metadata without claiming null bodies or message keys. | `tests/test_rabbitmq.py::test_deliveries_preserve_empty_body_and_flat_headers_without_auto_ack`<br>`tests/test_sync_rabbitmq.py::test_binary_headers_and_empty_body_are_detached_without_pre_admission_ack` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-010` | Flat native header values normalize without silently losing duplicate/unsupported types. | `tests/test_rabbitmq.py::test_unsupported_header_values_fail_closed`<br>`tests/test_rabbitmq_harness.py::test_binary_headers_round_trip_through_the_installed_native_field_table_codec` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-011` | Typed optional preconditions classify unsupported input without skipping mandatory lifecycle guarantees. | `tests/test_sync_contracts.py::test_undeclared_optional_cases_are_explicit_not_applicable_before_source_creation`<br>`tests/test_release_gates.py::test_declared_capability_precondition_is_explicitly_not_applicable` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-012` | Unsettled deliveries remain accounted under both event/byte bounds and bounded prefetch. | `tests/test_rabbitmq.py::test_pending_settlement_still_occupies_prefetch_capacity`<br>`tests/test_sync_rabbitmq.py::test_outstanding_deliveries_enforce_both_handoff_quotas`<br>`tests/integration/test_sync_contracts.py::test_sync_actor_prefetch_one_collects_an_independently_confirmed_burst_before_waiting` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-013` | Exactly one delivery token is acknowledged only after safe committed admission/filter/discard. | `tests/test_rabbitmq.py::test_core_acknowledges_only_after_admission_or_explicit_discard`<br>`tests/test_rabbitmq.py::test_settlement_accepts_only_safe_explicit_outcomes` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-014` | Rejected admission receives no successful ACK and settlement failure remains terminal. | `tests/test_rabbitmq.py::test_core_failure_never_acknowledges_rejected_delivery`<br>`tests/test_sync_rabbitmq.py::test_ack_failure_is_terminal_retains_token_and_native_cause` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-015` | Unexpected closure/cancellation is visible independently of business delivery and backpressure. | `tests/test_rabbitmq.py::test_failures_are_visible_without_receive_and_survive_waiter_cancellation`<br>`tests/test_sync_rabbitmq.py::test_broker_cancellation_has_priority_over_buffered_deliveries`<br>`tests/integration/test_sync_contracts.py::test_blocking_rabbitmq_detects_administrative_connection_closure_without_business_message` | [real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s28-016` | Owned Rabbit cleanup is bounded/idempotent and failures stay explicit without application resource changes. | `tests/test_rabbitmq.py::test_startup_rollback_and_cancellation_close_partial_resources`<br>`tests/test_sync_rabbitmq.py::test_cleanup_failure_aborts_resources_and_preserves_native_cause` | [real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s28-017` | Rabbit publisher/admin paths own only unique registered test exchanges even after partial failure. | `tests/test_rabbitmq_harness.py::test_harness_cannot_publish_to_or_delete_an_application_exchange`<br>`tests/test_rabbitmq_harness.py::test_partially_created_exchange_remains_owned_for_cleanup` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-018` | Rabbit receipts require native confirmation and unsupported envelopes fail before publishing. | `tests/test_rabbitmq_harness.py::test_publisher_confirmed_receipt_works_across_three_independent_loops`<br>`tests/test_rabbitmq_harness.py::test_unsupported_envelopes_are_rejected_before_a_publisher_connection` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s28-019` | Real Rabbit scenarios complement native doubles without conflating their evidence. | `tests/integration/test_rabbitmq_observer.py::test_sync_rabbitmq_protobuf_correlation_across_independent_publisher_loops`<br>`tests/integration/test_rabbitmq_observer.py::test_async_rabbitmq_binding_filters_routing_and_preserves_unique_headers`<br>`tests/integration/test_rabbitmq_observer.py::test_observer_does_not_consume_or_delete_an_existing_application_queue` | [compatibility-matrix](#compatibility-matrix) (stage 2)<br>[real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s28-020` | The digest-pinned Rabbit profile remains independently selectable with unique test sources. | No existing executable selector | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |
| `MUST-s28-021` | Real Rabbit Allure examples retain redacted matched/mismatched bodies, canonical originals and distinct English summaries. | `examples/allure/test_rabbitmq.py::TestRabbitMQAllureReporter::test_rabbitmq_correlated_success`<br>`examples/allure/test_rabbitmq.py::TestRabbitMQAllureReporter::test_rabbitmq_expected_timeout` | [allure-runner-golden-matrix](#allure-runner-golden-matrix) (stage 5) |
| `MUST-s28-022` | Rabbit CI explicitly configures the broker, runs the selected nodes and preserves separate report evidence. | No existing executable selector | [compatibility-matrix](#compatibility-matrix) (stage 2)<br>[complete-traceability-audit](#complete-traceability-audit) (stage 7) |

### [SPEC section 29](../SPEC.md#s29)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s29-001` | Redis native imports stay lazy and constructors need no connection or optional runtime. | `tests/test_redis_streams.py::test_module_and_factory_do_not_import_optional_client`<br>`tests/test_sync_redis_streams.py::test_provider_preserves_config_capabilities_and_report_title` | [verified-dependency-floors](#verified-dependency-floors) (stage 2) |
| `MUST-s29-002` | Redis configuration validates typed finite native options and does not expose/discover connection credentials. | `tests/test_redis_streams.py::test_config_rejects_unsafe_or_invalid_values_without_disclosure`<br>`tests/test_redis_streams.py::test_config_allows_explicit_tls_options_and_hides_url` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-003` | Redis observes an existing typed stream without seeding or mutating application data. | `tests/test_redis_streams.py::test_empty_existing_stream_is_ready_without_synthetic_writes`<br>`tests/integration/test_redis_streams_observer.py::test_missing_or_non_stream_source_is_rejected_at_startup` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-004` | Native IDs preserve both components, advance exclusively, and reject unrelated start semantics. | `tests/test_api.py::test_native_stream_positions_preserve_the_full_id_and_remain_exclusive`<br>`tests/test_redis_streams.py::test_future_cursor_waits_for_strictly_greater_id`<br>`tests/test_sync_redis_streams.py::test_future_cursor_waits_for_strictly_greater_id`<br>`tests/test_sync_redis_streams.py::test_unsupported_start_does_not_create_native_client` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-005` | Known baseline deletion evidence rejects unavailable explicit starts without shifting the request. | `tests/test_redis_streams.py::test_explicit_cursor_cannot_claim_known_missing_history`<br>`tests/test_sync_redis_streams.py::test_explicit_cursor_rejects_known_missing_history_and_closes_client` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-006` | Redis READY validates a consistent metadata/read path and keeps native-ID diagnostics separate from fabricated offsets. | `tests/test_redis_streams.py::test_readiness_uses_atomic_read_path_and_managed_binary_options`<br>`tests/test_sync_redis_streams.py::test_atomic_readiness_native_options_and_owner_thread` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-007` | Selected payload/header bytes and field identities are preserved or rejected before handoff without coercion. | `tests/test_redis_streams.py::test_invalid_or_duplicate_fields_fail_before_handoff`<br>`tests/test_sync_redis_streams.py::test_duplicate_raw_fields_are_rejected_before_handoff` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-008` | Redis sessions own their native I/O and bounded complete batches without hidden loops or shared active handles. | `tests/test_sync_redis_streams.py::test_atomic_readiness_native_options_and_owner_thread`<br>`tests/test_redis_streams.py::test_handoff_capacity_failure_is_bounded_and_independently_visible` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-009` | Both clients preserve one read-only identity/metadata/XREAD transaction and never acknowledge application groups. | `tests/test_redis_streams.py::test_readiness_uses_atomic_read_path_and_managed_binary_options`<br>`tests/test_sync_redis_streams.py::test_atomic_readiness_native_options_and_owner_thread`<br>`tests/integration/test_redis_streams_observer.py::test_stream_observer_leaves_application_consumer_group_and_pending_entries_untouched` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-010` | Observation leaves application entries/groups untouched and publisher permissions stay separate. | `tests/integration/test_redis_streams_observer.py::test_stream_observer_leaves_application_consumer_group_and_pending_entries_untouched` | [real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s29-011` | New deletion/trim/rewind evidence is terminal before a following candidate can commit. | `tests/test_redis_streams.py::test_new_deletion_is_terminal_before_candidate_handoff_even_when_consumed`<br>`tests/integration/test_redis_streams_observer.py::test_new_deletion_or_trimming_fails_before_handing_off_a_following_entry` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-012` | Connection ID and server identity changes prevent candidate handoff without silent recovery. | `tests/test_redis_streams.py::test_recycled_socket_or_restarted_server_never_hands_off_candidate`<br>`tests/test_sync_redis_streams.py::test_connection_or_server_replacement_is_terminal_before_handoff` | [real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s29-013` | Public Redis documentation states the metadata-equivalent recreation detection limit. | No existing executable selector | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |
| `MUST-s29-014` | Known failures remain terminal and owned resources are closed or honestly reported incomplete. | `tests/test_redis_streams.py::test_terminal_error_is_not_retried_and_failure_signal_survives_cancellation`<br>`tests/test_redis_streams.py::test_cleanup_failure_never_claims_closed`<br>`tests/test_sync_redis_streams.py::test_stalled_cleanup_is_bounded_then_joins_same_worker` | [real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s29-015` | Redis harness initialization/cleanup owns unique streams and fresh per-operation clients. | `tests/test_redis_streams_harness.py::test_partial_stream_creation_is_owned_until_cleanup`<br>`tests/test_redis_streams_harness.py::test_empty_typed_stream_and_receipt_work_across_three_independent_event_loops` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-016` | Redis receipts require accepted XADD IDs and reject unsupported inputs or foreign resources. | `tests/test_redis_streams_harness.py::test_only_valid_xadd_entry_ids_produce_a_receipt`<br>`tests/test_redis_streams_harness.py::test_unsupported_envelopes_are_rejected_before_a_publisher_client`<br>`tests/test_redis_streams_harness.py::test_harness_never_appends_to_or_deletes_a_foreign_stream` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s29-017` | The digest-pinned authenticated Redis profile remains independently selectable and real node applicability stays explicit. | No existing executable selector | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |
| `MUST-s29-018` | Redis examples preserve redacted English Allure content, native originals and one summary per completed wait. | `examples/allure/test_redis_streams.py::TestRedisStreamsAllureReporter::test_redis_stream_correlated_success`<br>`examples/allure/test_redis_streams.py::TestRedisStreamsAllureReporter::test_redis_stream_expected_timeout` | [allure-runner-golden-matrix](#allure-runner-golden-matrix) (stage 5) |

### [SPEC section 30](../SPEC.md#s30)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s30-001` | Marks retain immutable valid watcher-local sequence/time envelopes rather than timestamp-based matching. | `tests/test_observation_marks.py::test_unrepresentable_mark_times_raise_configuration_error`<br>`tests/test_observation_marks.py::test_wrong_type_foreign_and_future_markers_are_configuration_errors` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s30-002` | Mark capture requires an active owner and retains normal lifecycle/affinity checks. | `tests/test_observation_marks.py::test_mark_requires_an_active_watcher_and_remains_readable_after_close`<br>`tests/test_observation_marks.py::test_sync_mark_preserves_thread_affinity_and_reentrant_call_guards` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s30-003` | Mark capture emits no report or extra consumer/scope/waiter/result registration. | `tests/test_observation_marks.py::test_scope_marks_preserve_nested_filters_identity_and_scope_quota`<br>`tests/test_allure_context.py::test_observation_mark_is_quiet_and_since_summary_shows_only_the_safe_lower_bound` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s30-004` | Since intersects the parent lower range and retains parent correlation filters. | `tests/test_observation_marks.py::test_reusable_mark_excludes_history_but_finds_events_admitted_during_the_action`<br>`tests/test_observation_marks.py::test_an_earlier_marker_cannot_expand_a_narrowed_parent_scope` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s30-005` | Per-wait since boundaries preserve original scope identity/snapshots while diagnostics use effective bounds. | `tests/test_observation_marks.py::test_scope_marks_preserve_nested_filters_identity_and_scope_quota`<br>`tests/test_expectation_engine.py::test_count_uses_distinct_records_and_preserves_scope_identity_and_effective_lower` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s30-006` | Invalid/foreign/future marks fail before resource registration and do not bypass terminal state. | `tests/test_observation_marks.py::test_wrong_type_foreign_and_future_markers_are_configuration_errors`<br>`tests/test_expectation_engine.py::test_mark_intersection_rejects_wrong_ownership_type_and_future_position` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s30-007` | Marks do not pin history; relevant known loss blocks strict matching while earlier loss stays outside range. | `tests/test_observation_marks.py::test_loss_after_a_mark_blocks_strict_success_but_allows_explicit_incomplete_search`<br>`tests/test_observation_marks.py::test_losses_before_the_mark_do_not_poison_the_new_complete_interval` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s30-008` | Marks allocate no registration quota and normal expectation/copy/deadline budgets remain active. | `tests/test_observation_marks.py::test_scope_marks_preserve_nested_filters_identity_and_scope_quota`<br>`tests/test_expectation_engine.py::test_shared_result_budget_is_released_after_partial_failure` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s30-009` | Both-mode mark scripts cover history, filters, ownership, quota and retention without timing sleeps. | `tests/test_observation_marks.py::test_reusable_mark_excludes_history_but_finds_events_admitted_during_the_action`<br>`tests/test_observation_marks.py::test_scope_marks_preserve_nested_filters_identity_and_scope_quota`<br>`tests/test_observation_marks.py::test_loss_after_a_mark_blocks_strict_success_but_allows_explicit_incomplete_search` | No additional check-design gap recorded; candidate execution still required |

### [SPEC section 31](../SPEC.md#s31)

| Requirement | Expected behavior/check boundary | Existing exact selectors | Additional missing checks |
| --- | --- | --- | --- |
| `MUST-s31-001` | Explicit namespaces preserve canonical classes and ordinary versus awaitable API results. | `tests/test_sync_api.py::test_async_namespace_preserves_existing_classes`<br>`tests/test_api.py::test_public_blocking_namespace_exposes_the_canonical_contracts_and_native_adapters`<br>`tests/test_api.py::test_both_public_watch_factories_have_raw_decoder_and_message_overloads` | [installed-public-consumer-typing](#installed-public-consumer-typing) (stage 2) |
| `MUST-s31-002` | Async-only adapters require explicit blocking support and cannot invoke a hidden fallback loop. | `tests/test_sync_api.py::test_async_only_extension_is_rejected_without_opening_it`<br>`tests/test_sync_api.py::test_awaitable_provider_is_closed_and_rejected_synchronously` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s31-003` | Blocking open/poll readiness, timeout validation and known-failure precedence remain explicit. | `tests/test_sync_api.py::test_native_startup_timeout_uses_the_shared_public_domain_error`<br>`tests/test_sync_kafka.py::test_poll_rejects_invalid_or_unrepresentable_timeout`<br>`tests/test_sync_rabbitmq.py::test_broker_cancellation_has_priority_over_buffered_deliveries` | [real-broker-faults](#real-broker-faults) (stage 4) |
| `MUST-s31-004` | Failure monitoring remains independent of delivery capacity and native cleanup respects owner/deadline contracts. | `tests/test_sync_contracts.py::test_failure_is_visible_without_publication_or_polling`<br>`tests/test_sync_rabbitmq.py::test_native_session_rejects_foreign_thread_and_process`<br>`tests/test_sync_redis_streams.py::test_cleanup_failure_retains_cause_and_chain` | [native-lifecycle-resource-counts](#native-lifecycle-resource-counts) (stage 4) |
| `MUST-s31-005` | Only settlement-capable transports acknowledge committed outcomes; blocking settlement cannot return hidden async work. | `tests/test_settlement.py::test_settlement_admitted_and_explicit_discard`<br>`tests/test_sync_api.py::test_async_settlement_is_rejected_before_ready_and_rolls_back_native_session` | No additional check-design gap recorded; candidate execution still required |
| `MUST-s31-006` | The schedulers share neutral semantics and native algorithms without treating an async handle as blocking. | `tests/test_expectation_engine.py::test_engine_imports_no_scheduler_or_threading_module`<br>`tests/test_sync_kafka.py::test_read_committed_uses_shared_physical_high_resolution`<br>`tests/test_sync_redis_streams.py::test_atomic_readiness_native_options_and_owner_thread` | [complete-traceability-audit](#complete-traceability-audit) (stage 7) |
| `MUST-s31-007` | Shared and facade regressions cover core separation; required matrix/native fault coverage remains separate. | `tests/test_expectation_engine.py::test_final_commit_rechecks_terminal_state_after_matcher_success`<br>`tests/test_sync_runtime.py::test_decoder_and_matcher_contexts_are_independent_and_never_leak_to_caller`<br>`tests/test_sync_contracts.py::test_blocking_contract_execution_does_not_create_or_run_asyncio` | [compatibility-matrix](#compatibility-matrix) (stage 2)<br>[real-broker-faults](#real-broker-faults) (stage 4) |

## Explicit scope classification

| Source entry | Classification | Rationale |
| --- | --- | --- |
| [MUST-s01-001](../SPEC.md#s01) | `meta` | Section 1 defines requirement vocabulary, not an executable product behavior. |
| [MUST-s01-002](../SPEC.md#s01) | `meta` | Section 1 defines requirement vocabulary, not an executable product behavior. |
| [MUST-s18-001](../SPEC.md#s18) | `deferred` | Section 18 explicitly defines temporal features after 0.1; no MVP implementation claim or release stub is authorized. |

The section 23 packaging sketch is a template, but its surrounding publication,
verified-floor, archive, and owner-authorization MUST paragraphs remain MVP
obligations. They are not exempted as template text. Section 18 alone explicitly
places its temporal APIs after 0.1; no temporal stub or MVP implementation claim
is introduced by this mapping.

## Missing-check work inventory

Work items can appear on multiple obligations. They describe concrete remaining
verification work and do not carry manually edited pass flags. Completion requires
the planned implementation plus candidate-bound successful evidence; status and
release aggregation remain separate from this static inventory.

<a id="complete-traceability-audit"></a>
### `complete-traceability-audit` — stage 7

Review every clause against actual candidate evidence, including the error catalog, implemented/deferred capabilities, public documentation, and architecture boundaries. The stage 1 structural inventory check does not prove these behaviors.

Affected entries: `MUST-s16-001`, `MUST-s20-001`, `MUST-s22-001`, `MUST-s24-001`, `MUST-s25-001`, `AC-87`, `MUST-s26-001`, `MUST-s28-020`, `MUST-s28-022`, `MUST-s29-013`, `MUST-s29-017`, `MUST-s31-006`.

<a id="installed-public-consumer-typing"></a>
### `installed-public-consumer-typing` — stage 2

Extend installed public consumer inference checks to every supported Python/OS/dependency profile and register candidate-bound results. Audit public parameter annotations separately: strict Pyright currently reports unknown member types; stage 1 uses the project basic mode.

Affected entries: `MUST-s06-001`, `MUST-s24-001`, `MUST-s31-001`.

<a id="owner-publisher-rights-preflight"></a>
### `owner-publisher-rights-preflight` — stage 7

Recheck the recorded owner decisions and observed publisher/environment settings before release; retain actual candidate administrative evidence separately from the November 8 preflight record.

Affected entries: `MUST-s23-003`, `MUST-s23-004`, `AC-86`.

<a id="published-accounting-formula-check"></a>
### `published-accounting-formula-check` — stage 6

Reconcile the published raw/decoded/envelope accounting formula with measured retained bytes and document native/process RSS and replay limits separately.

Affected entries: `MUST-s08-002`, `MUST-s08-003`, `MUST-s09-002`.

<a id="compatibility-matrix"></a>
### `compatibility-matrix` — stage 2

Execute the claimed Python/platform/native-client matrix and retain exact versions and candidate-bound results.

Affected entries: `MUST-s16-009`, `MUST-s16-014`, `MUST-s16-015`, `AC-67`, `MUST-s28-005`, `MUST-s28-019`, `MUST-s28-022`, `MUST-s31-007`.

<a id="protobuf-runtime-gencode-matrix"></a>
### `protobuf-runtime-gencode-matrix` — stage 2

Run minimum/current Protobuf runtimes with multiple compatible generated-code generations.

Affected entries: `AC-25`.

<a id="verified-dependency-floors"></a>
### `verified-dependency-floors` — stage 2

Execute isolated declared-minimum/current dependency profiles, retain incompatibility evidence and align metadata/SPEC support claims.

Affected entries: `MUST-s23-001`, `MUST-s26-001`, `MUST-s28-003`, `MUST-s29-001`.

<a id="advertised-endpoint-proxy"></a>
### `advertised-endpoint-proxy` — stage 3

Demonstrate that Kafka traffic uses Toxiproxy after metadata discovery, including advertised endpoints, with no direct bypass.

Affected entries: `MUST-s21-002`, `AC-77`.

<a id="kafka-isolation-startup-first-failure"></a>
### `kafka-isolation-startup-first-failure` — stage 4

Reproduce and diagnose the historical reader-startup TransportConnectionError (code=None) in the two-session isolation test; preserve the original failure and separate diagnostic reruns.

Affected entries: `MUST-s10-003`, `AC-18`.

<a id="kafka-partition-leader-first-timeout"></a>
### `kafka-partition-leader-first-timeout` — stage 4

Diagnose the historical future-timestamp partition-leader _TIMED_OUT (-185); retain the initial failure separately from passing isolated/full diagnostic reruns.

Affected entries: `MUST-s10-003`, `AC-13`.

<a id="native-lifecycle-resource-counts"></a>
### `native-lifecycle-resource-counts` — stage 4

Measure native worker/task/handle counts across repeated normal and fault lifecycles, retaining first failures and diagnostic reruns separately.

Affected entries: `MUST-s15-001`, `MUST-s16-005`, `MUST-s16-015`, `AC-57`, `AC-66`, `MUST-s31-004`.

<a id="platform-subprocess-interruption"></a>
### `platform-subprocess-interruption` — stage 4

Execute actual platform-specific Ctrl-C/subprocess interruption and bounded hang-containment checks.

Affected entries: `MUST-s14-002`, `MUST-s16-010`, `MUST-s16-013`, `MUST-s16-015`, `AC-53`, `AC-65`.

<a id="real-broker-faults"></a>
### `real-broker-faults` — stage 4

Run barrier-driven startup/auth/ACL/network/restart/cleanup faults on the selected real brokers in both execution modes.

Affected entries: `MUST-s07-002`, `MUST-s09-005`, `MUST-s10-003`, `AC-09`, `AC-55`, `MUST-s28-007`, `MUST-s28-015`, `MUST-s28-016`, `MUST-s28-019`, `MUST-s29-010`, `MUST-s29-012`, `MUST-s29-014`, `MUST-s31-003`, `MUST-s31-007`.

<a id="real-kafka-retention-compaction"></a>
### `real-kafka-retention-compaction` — stage 4

Prove retained-offset availability and compaction/control-gap semantics on a real Kafka broker without offset-arithmetic loss inference.

Affected entries: `MUST-s07-004`, `MUST-s10-003`, `AC-11`, `AC-15`.

<a id="real-multiprocess-native-isolation"></a>
### `real-multiprocess-native-isolation` — stage 4

Verify independent native readers and resources in separate pytest-xdist processes on one owned source.

Affected entries: `MUST-s04-002`, `AC-19`, `AC-83`.

<a id="allure-runner-golden-matrix"></a>
### `allure-runner-golden-matrix` — stage 5

Run real pytest/Allure golden checks on selected versions and concurrency contexts while preserving outcomes and parent/test ownership.

Affected entries: `MUST-s16-013`, `MUST-s17-001`, `MUST-s17-002`, `MUST-s17-003`, `AC-72`, `MUST-s28-021`, `MUST-s29-018`.

<a id="safe-artifact-secret-scan"></a>
### `safe-artifact-secret-scan` — stage 5

Scan bounded safe exports and sanitized logs for controlled markers in payload, headers, keys, expected values, descriptions, filenames, and exceptions.

Affected entries: `MUST-s21-003`, `AC-70`.

<a id="bounded-resource-benchmark"></a>
### `bounded-resource-benchmark` — stage 6

Record warmed repeated Core/native load and RSS/resource measurements with hardware, limits, versions, and raw JSON; assert bounded library-owned structures.

Affected entries: `MUST-s08-003`, `MUST-s16-005`, `AC-84`.

<a id="candidate-archive-inventory-scan"></a>
### `candidate-archive-inventory-scan` — stage 7

Inspect actual wheel/sdist file inventories and metadata for unexpected/private artifacts, credentials, reports, caches and unresolved operational placeholders.

Affected entries: `MUST-s23-002`, `MUST-s23-003`, `AC-85`, `AC-88`.

<a id="candidate-installed-artifact-matrix"></a>
### `candidate-installed-artifact-matrix` — stage 7

Execute clean candidate wheel/sdist and minimal-extra installations on the claimed matrix, recording tested hashes with no editable-source shadowing.

Affected entries: `MUST-s23-002`, `MUST-s24-001`, `AC-79`, `AC-82`.

<a id="owner-authorized-published-artifact-smoke"></a>
### `owner-authorized-published-artifact-smoke` — stage 8

After explicit owner approval, verify actual same-run upload, attestations, published file hashes and installed-package smoke.

Affected entries: `MUST-s23-004`, `AC-86`.

## Reliability follow-up ledger

Historical passing rechecks do not erase these first failures. Neither entry is
closed by the traceability work.

| Follow-up | Preserved observation | Required next result |
| --- | --- | --- |
| `kafka-isolation-startup-first-failure` (stage 4; AC-18, MUST-s10-003) | The November 2 full Kafka run returned 22 passed and 1 failed. `tests/integration/test_kafka_contracts.py::TestKafkaAdapterContracts::test_isolation_sessions_both_observe_one_publication` failed during reader startup with `TransportConnectionError` (`code=None`). The isolated recheck passed; the original cause remains unconfirmed. See [status.md](status.md). | Capture a bounded reproducer, native cause/phase and broker/endpoint state under the isolated fault harness; retain the original failure and distinguish a demonstrated fix from a passing rerun. |
| `kafka-partition-leader-first-timeout` (stage 4; AC-13, MUST-s10-003) | The first complete broker run returned 70 passed, 7 skipped, and 1 failed. `tests/integration/test_kafka.py::test_future_timestamp_falls_back_to_high_or_requires_existing` failed during an AdminClient partition-leader query with native `_TIMED_OUT` (`-185`). An isolated recheck and final full run passed. Cause-chain preservation improved; intermittent failure absence was not established. See [solid-dry-kiss-review.md](solid-dry-kiss-review.md). | Preserve first-failure diagnostics and reproduce the leader/startup condition with controlled timing/faults; prove any proposed fix without automatic retries or unexplained timeout increases. |

The previously diagnosed `read_committed` physical-high/LSO discrepancy and
RabbitMQ binary-header codec mismatch have recorded fixes and regressions in
[status.md](status.md). They remain useful regression checks, rather than being
listed as unexplained failures. New failures from the candidate matrix must be
added with their original evidence and stage before release closure.

<a id="callback-contract-and-timing"></a>
### `callback-contract-and-timing` — stage 4

Verify bounded slow-callback diagnostics and owner-thread execution without an unbounded worker pool; document deterministic, short, I/O-free user callback obligations separately from enforced runtime guarantees.

Affected entries: `MUST-s12-002`, `MUST-s14-002`.
