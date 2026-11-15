# Plan to close the 0.1.0 release gates

Prepared on November 8, 2026 against SPEC revision 1.6, sections 21–23 and
RG-01–RG-15. This is an implementation and verification plan, not a statement
that the candidate is ready to publish. Existing local results remain historical
evidence. All mandatory release evidence must identify the candidate it tested.

## Interview decisions

| Decision | Owner's answer |
| --- | --- |
| License | MIT |
| Copyright holder | Valerii Korobov |
| Application-specific broker versions | None; propose representative profiles under SPEC |
| Performance requirements | Establish a measured baseline under SPEC; no invented throughput SLA |
| Benchmark runner | GitHub-hosted; record actual hardware/software for every run |
| CI cadence | Run the complete mandatory set on every PR, as well as release verification |
| Incompatible dependency minimum | May raise it only after a reproducible failing check, updating SPEC and documentation |

The final name and repository are `pytest-broker-observer` and
`valkorobov/pytest-broker-observer`. Stage 1 records the observed pending publisher
and configured environment protections in [release-preflight.json](release-preflight.json).
Actual publication remains unexecuted. Preparing this plan does not authorize a production
PyPI upload. The final release is an explicit owner action after gate closure.

## Working rules and evidence

- Keep the existing sync actor, async ownership, scheduler-independent policies,
  lazy optional dependencies, pytest-only operation, and 80% unit coverage gate.
- Keep the pre-existing uncommitted publishing changes; integrate the gate check
  with that workflow before enabling any release upload.
- Separate required checks from capability-based applicability. A missing service,
  unresolved test, missing artifact, unexpected skip, xfail, or ignored first
  failure cannot count as a passed mandatory check.
- Record test node/check IDs, expected outcomes, platform/runtime/profile,
  source SHA, workflow run/attempt, exact versions, and evidence artifact paths.
- Version the requirements/check inventory in source control. Generate results
  in CI: do not put a commit's own SHA into a tracked file in that same commit,
  and do not use a manually edited `passed` flag as test evidence.
- Gate completion requires both implementation and a recorded successful run.
  Configured jobs, passing doubles, and diagnostic reruns are distinct evidence.
- Build distributions once per candidate. Subsequent checks use those files;
  regenerate their evidence if the source candidate or distribution bytes change.
- Record all results in `docs/status.md`, detailed compatibility manifests, and
  bounded CI artifacts. Do not reset existing Compose data volumes.

## Proposed verification matrix

Profiles below are test targets, not new compatibility claims. Resolve exact
versions/digests and prove image/client compatibility before treating a profile
as supported. Do not use floating `latest` images.

| Dimension | Required checks |
| --- | --- |
| Python | CPython 3.11, 3.12, 3.13, 3.14 |
| Linux | Unit/parity, installed packages, native contracts, faults, Allure, and resources |
| macOS and Windows | Core/sync/parity, installed packages, API/typing, pytest-only and feasible transport smoke checks; record architecture explicitly |
| Kafka | Existing pinned 3.9.2 KRaft profile plus an older 3.8.1 candidate profile |
| RabbitMQ | Existing pinned 4.4.0 profile plus a 4.3.7 candidate profile |
| Redis Streams | Existing pinned 8.2.11 profile plus a 7.0.15 candidate profile to investigate the current Redis 7+ claim |
| Native clients | Isolated declared-minimum and locked/current profiles for confluent-kafka, aio-pika, Pika, and redis-py; both execution backends |
| Protobuf | Declared minimum and representative highest allowed runtime, with multiple compatible generated-code generations |
| Test/report stack | Declared minimum and locked/current pytest, pytest-asyncio, xdist, and Allure profiles, including real-plugin golden checks |
| Minimal installation | Stdlib-only Core, pytest-only sync/factories, and each optional feature in an isolated environment |

Use an explicit inventory of representative combinations rather than an
unbounded Cartesian product. The four Python versions and all claimed platforms
remain mandatory. Dependency/environment markers must reflect any demonstrated
Python-specific compatibility boundary. A raised minimum requires preserving
the failing evidence, a passing replacement profile, and corresponding metadata,
SPEC, changelog, and documentation changes.

## Stage 0 — publication interlock and gate inventory

**Gates:** RG-01, RG-12–RG-15. **Depends on:** none.

- [x] Add a machine-readable inventory of RG-01–RG-15 with required checks,
  applicability, evidence schema, and exact candidate/run identity.
- [x] Add a validator and CI aggregation job. Missing, failed, stale, mismatched,
  or incomplete evidence must block the privileged publication job.
- [x] Make the pending `publish` job depend on that aggregation job. PRs exercise
  the checks without requesting PyPI credentials or performing uploads.
- [x] Preserve existing version/tag and SHA-256 checks. Include compatibility and
  gate evidence alongside distributions, outside the upload-only `dist` directory.
- [x] Test rejection of each absent gate, wrong candidate/run/hash, unexpected
  mandatory skip, and a tampered artifact; validate a complete synthetic evidence
  bundle as a schema test, without claiming that synthetic results close gates.

**Done when:** a green legacy CI alone cannot publish an incomplete candidate;
negative controls fail, and the status record explicitly identifies open gates.

Implementation and local verification are recorded in
[status.md](status.md#release-publication-interlock-plan-stage-0); the producer
schema and CI behavior are described in the
[evidence contract](release-gate-evidence.md). The inventory remains planned, so
no real candidate gate has been marked passed by completing this stage.

## Stage 1 — license, identity, and complete traceability

**Gates:** RG-01, RG-09, RG-12, RG-13. **Depends on:** stage 0 schema.

- [x] Add MIT `LICENSE` with `Copyright (c) 2026 Valerii Korobov`, SPDX metadata,
  explicit license-file inclusion, and verified wheel/sdist license contents.
- [x] Record the owner's name/repository/license decisions and reported publisher
  setup separately from actual remote verification and name availability.
- [x] Check the actual publisher identity, `ci.yml` / `pypi` binding, required
  environment approval and tag protections; record administrative owner
  confirmations and remotely verified facts separately.
- [x] Expand grouped acceptance rows into exact AC-01–AC-88 and MVP MUST/check
  mappings, including RabbitMQ, Redis, marks, split runtimes, and pytest fixtures.
- [x] Identify existing and missing checks, native versus synthetic evidence,
  expected applicability, and every current reliability failure needing follow-up.
- [x] Validate metadata, public exports/overloads, `py.typed`, installed consumer
  typing examples, and public documentation against candidate artifacts.

**Done when:** every mandatory requirement has an existing check or an explicit
missing-check work item with an expected result; unmapped obligations are rejected;
license metadata and archive contents agree. The complete executable evidence
is required at stage 7. External preflight gaps stay recorded until resolved.

Implementation, actual local checks, and remaining evidence limits are recorded
in [status.md](status.md#license-identity-and-traceability-plan-stage-1).
Remote preflight is recorded separately from candidate CI evidence; no release
gate has been marked passed by completing this stage.

## Stage 2 — Python, OS, dependency, and runner compatibility

**Gates:** RG-02, RG-07–RG-10, RG-15. **Depends on:** stage 0 and stage 1 inventory.

- [x] Add the full Linux/macOS/Windows Core and package matrix for Python 3.11–3.14,
  explicitly selecting and recording the interpreter and runner architecture.
- [x] Add isolated minimum/current dependency profiles without importing packages
  from the all-extras development environment or the editable source checkout.
- [x] Test Protobuf runtime/gencode combinations and both broker client paths.
- [x] Resolve incompatible floors with reproducible evidence and approved policy;
  preserve the initial failure and update declared support consistently.
- [x] Run installed wheel/sdist, optional-import, pytest-only/factory, typing,
  public examples, and sync/async parity checks in applicable profiles.
- [x] Emit exact-version manifests and JUnit evidence for every required cell.

**Done when:** every claimed matrix cell has successful candidate evidence and
the published dependency/support declarations match the measured combinations.

All 24 hosted cells passed for `db3913f` with matching wheel and sdist hashes.
The evidence covers all four interpreters in both dependency profiles on Linux,
macOS, and Windows. Stage 7 still requires registering and aggregating the
results for the release candidate. [Verification record](status.md#github-actions-verification)

## Stage 3 — isolated real-broker infrastructure

**Gates:** RG-03, RG-04, RG-10, RG-15. **Depends on:** stages 0–1; informs stage 2.

- [x] Implement Testcontainers fixtures with digest-pinned images, unique names,
  dynamic mapped ports, bounded readiness, and cleanup after partial startup.
- [x] Verify the actual KRaft image/class/entrypoint combination; use an explicit
  generic container configuration where a Kafka-specific class is incompatible.
- [x] Resolve and verify the proposed older broker profiles and platform manifests.
- [x] Run all applicable async and blocking contract nodes on each required profile.
- [x] Add an isolated Toxiproxy network, route Kafka's advertised broker endpoints
  through it and prove there is no direct client bypass after metadata discovery.
- [x] Capture bounded sanitized logs and fault scripts/seeds. Do not stop a shared
  developer broker or delete application topics, queues, exchanges, or streams.

**Done when:** all applicable real contracts pass on the required profiles,
resource ownership/cleanup is proved, and network-fault routing is demonstrated.

All six profiles passed locally on Docker Linux arm64. CI produces
candidate-bound RG-03 evidence from the same packaged wheel. The first hosted
Linux x64 run failed the minimum RabbitMQ profile. The follow-up passed that
profile and exposed a Kafka 3.9.2 leader lookup timeout during startup.
A successful hosted run and subsequent release verification remain required.
[Stage 3 results](status.md#isolated-real-broker-contracts-plan-stage-3)

## Stage 4 — real failures, process interruptions, and leak checks

**Gates:** RG-03–RG-06, RG-11, RG-15. **Depends on:** stage 3.

- [ ] Add both-mode startup-unavailable, invalid-auth/ACL, post-READY disconnect,
  latency/blackhole, broker restart, and failed/stalled cleanup scenarios.
- [ ] Complete Kafka retention/unavailable-position, recreation, compaction/control
  gaps and topology scenarios; retain Redis identity/deletion and Rabbit dedicated
  subscription semantics with explicit capability applicability.
- [ ] Start faults at READY/admission/waiter/full-handoff barriers rather than
  arbitrary sleeps. Assert typed errors, unchanged deadlines, correct priority,
  rollback, and no apparently successful match after known continuity loss.
- [ ] Add subprocess Ctrl-C and hung callback/native-close containment tests.
  Implement actual platform-specific signal handling; do not label forced Windows
  termination as graceful Ctrl-C cleanup.
- [ ] Record live threads/tasks/native handles over repeated normal/fault lifecycles.
  Investigate the recorded Kafka isolation startup and partition-leader failures;
  preserve first failure and diagnostic reruns as separate evidence.

**Done when:** required faults and lifecycle scenarios pass with finite budgets,
expected failures are explicit, normal paths release resources, and no unresolved
mandatory flaky failure is hidden or automatically retried into release success.

## Stage 5 — Allure golden reports and artifact disclosure

**Gates:** RG-06, RG-07, RG-09, RG-12. **Depends on:** stages 2–4 profiles.

- [ ] Add real-plugin golden checks for selected pytest/Allure versions, sync,
  async, xdist, and simultaneous independent watchers. Normalize only dynamic
  IDs/timestamps; keep result/counter/context/disclosure assertions substantive.
- [ ] Verify outcomes with reporting enabled/disabled, one summary per wait,
  correct owner context, `indent=2`, and bounded English text/body attachments.
- [ ] Exercise markers in payload, headers, key, expected values, descriptions,
  filenames, and exception strings; include failed reporters/redactors.
- [ ] Scan built-in safe JSON/text/HTML exports and sanitized container logs for
  marker leaks. Keep intentional FULL_PAYLOAD and local raw traceback boundaries
  explicit rather than promising to redact arbitrary pytest/user tracebacks.
- [ ] Save golden results, manifests, and sanitizer evidence within retention limits.

**Done when:** runner/profile golden cases pass, reports preserve test outcomes,
and safe built-in artifacts contain none of the controlled secret markers.

## Stage 6 — measured performance and mandatory resource bounds

**Gates:** RG-04, RG-05, RG-11. **Depends on:** stages 2–4 infrastructure.

- [ ] Add separate Core and real-broker benchmark commands outside unit coverage.
- [ ] Use 256 B, 4 KiB and 256 KiB payloads, 10,000-event bursts, multiple scopes,
  and 1/10/100 async waiter profiles. For sync, use sequential waits or independent
  owner-thread watchers; never concurrently call one sync handle from many threads.
- [ ] Use a streaming bounded producer/transport or separate producer process for
  RSS checks: FakeTransport's intentionally retained publication history must not
  be mistaken for observer memory growth.
- [ ] On each GitHub-hosted job record CPU/model, RAM, OS/architecture, runner image,
  Python/native/broker versions, source SHA, limits, payload and workload profile.
  Warm up and perform repeated samples on that same allocated runner.
- [ ] Save raw JSON for throughput, admission/matcher/result latency, startup/close,
  CPU/peak RSS, accounted bytes, queue occupancy, pending work, and live threads.
- [ ] Assert existing hard event/byte/command/resource bounds and controlled overload
  behavior. Establish a measured baseline without inventing an absolute speed SLA
  or cross-host percentage regression threshold.

**Done when:** all required profiles have reproducible within-job samples and
environment manifests, resource-bound assertions pass, and performance claims
are limited to the measured hardware/workload. Cross-host runs remain separate.

## Stage 7 — full PR and candidate verification

**Gates:** all RG-01–RG-15 pre-publication requirements. **Depends on:** stages 0–6.

- [ ] Run the complete mandatory matrix, native faults, golden reports, resources,
  and benchmarks on every PR as selected in the interview. Keep ordinary unit
  coverage independent from examples and broker/benchmark runs.
- [ ] Freeze a candidate commit, version/changelog/tag identity and exact profiles.
- [ ] Build wheel from the source archive once, then test the actual artifacts
  across minimal/optional environments and applicable platform/Python profiles.
- [ ] Inspect archive inventories/metadata/licenses/typing; reject private schemas,
  credentials, `.env`, generated reports, caches, unexpected files and placeholders.
  Allow only explicitly identified public synthetic fixtures, not a blanket
  exemption for arbitrary test files containing apparent credentials.
- [ ] Aggregate same-candidate results, distribution hashes, compatibility data,
  expected applicability/counts and first-failure records into the release bundle.
- [ ] Execute the production publication interlock against complete and negative
  cases; ensure the publishing job performs no rebuild or arbitrary package code.
- [ ] Update status/compatibility/traceability with actual GitHub run/artifact links.
- [ ] Complete publisher/name/environment preflight before production approval.

**Done when:** all pre-publication gates pass for the frozen candidate and exact
distribution bytes, with no missing evidence or unresolved mandatory failures.

PR runs execute the complete checks as an early verification. After that code
is ready, the owner starts the candidate release run. That run repeats the
mandatory verification on the tagged commit, builds and verifies the artifacts,
and pauses at the protected `pypi` environment for required owner approval.
Stage 8 resumes that same run and exact artifact. A later separate run is a new
candidate verification, not a way to reuse another run's completed gate report.

## Stage 8 — owner release and published-package verification

**Gates:** final RG-12–RG-14 execution evidence. **Depends on:** stage 7.

- [ ] Verify actual publisher/environment identity and tag restrictions, including
  the owner's required approval policy, against the reviewed release configuration.
- [ ] Present the completed gate report and distribution hashes for the owner's
  explicit production release action. Do not publish merely to test credentials.
- [ ] Run PyPI Trusted Publishing from the protected job, retain attestations,
  and smoke-install the published version in a fresh environment.
- [ ] Check installed name/version, `py.typed`, stdlib-only import, pytest factory
  discovery and marked sync observation; record published file hashes/provenance.
- [ ] Update release notes/status with actual upload and installed-package evidence.

**Done when:** the owner-authorized version is uploaded, its attestations and
hashes match the verified bundle, and published-package smoke checks pass.

## Execution order

Implement stage 0 first, followed by license and traceability. Compatibility
profiles and isolated broker infrastructure can then proceed independently;
faults and Allure/resource evidence build on those profiles. Stage 7 freezes
and reruns the integrated result. Stage 8 is a separate explicit release action.

Completion is not inferred from the number of implemented tests. A remaining
unavailable runner, failed profile, missing remote result, or unperformed
production upload stays visible as an open gate with its exact next action.

## Gate-to-stage checklist

All rows start open for the next candidate; this table maps obligations, not
historical local successes.

| Gate | Main implementation / verification stages |
| --- | --- |
| RG-01 | 0–1: complete obligation inventory; 7: every mandatory check exists and passes |
| RG-02 | 2 and 7: Python/platform parity matrix |
| RG-03 | 3–4 and 7: real-broker contracts and capability applicability |
| RG-04 | 4 and 6–7: readiness, concurrency, loss, cleanup and faults |
| RG-05 | 4 and 6–7: continuous sync collection, process interruption and resource cleanup |
| RG-06 | 4–5 and 7: redaction and bounded safe artifact checks |
| RG-07 | 2 and 5–7: Allure runner/golden/context/outcome verification |
| RG-08 | 1–2 and 7: clean candidate wheel/sdist installations |
| RG-09 | 1–2 and 5–7: typing, public signatures and installed examples |
| RG-10 | 2–3 and 7: dependency minima and actual compatibility matrix |
| RG-11 | 4 and 6–7: resource limits and measured memory behavior |
| RG-12 | 0–1 and 5, 7–8: version, archive and distribution integrity |
| RG-13 | 1 and 7: owner inputs and publisher/environment preflight; 8: drift recheck |
| RG-14 | 0 and 7–8: same-run candidate evidence, artifact hashes and actual upload |
| RG-15 | 0–4 and 7: no hidden mandatory failures, unverified skips or release stubs |
