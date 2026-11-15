# MVP status and publication requirements

This is a working MVP 0.1.0 candidate based on SPEC.md, rather than an
announcement of a published release or one that has passed all release gates.
The specification is maintained in English. Revision 1.7 records the
compatibility profiles and measured dependency floors. Revision 1.6 added lazy
pytest constructor fixtures on top of separate async/blocking execution, reusable
observation marks, Kafka, RabbitMQ, Redis Streams, and policy-controlled message
diagnostics.

Implemented features include separate async and blocking thread runtimes sharing
expectation/admission policies, a bounded
Journal and loss ledger, independent expectations/scopes, raw and Protobuf
decoders/resolvers, three-valued matchers, Kafka, RabbitMQ, and Redis Streams adapters, safe diagnostics
and an explicit Allure reporter, lazy pytest factory fixtures, reusable transport
contracts, packaging, and CI.
`ObservationMark` and `since` add per-wait Journal boundaries without changing
scope definitions or broker positions.

The distribution is named `pytest-broker-observer` as requested by the owner;
the `event_observer` import namespace follows the specification.

## Controlled faults and lifecycle (plan stage 4)

The pinned broker profiles now run native fault checks against the installed
wheel. Both reader modes exercise an unreachable endpoint, authentication and
authorization, interruption after READY, and recovery with a fresh reader.
Kafka also verifies timed waits under delayed or blocked proxy replies,
unavailable retained offsets, cleaner-induced sparse offsets, transaction
control gaps, detected deletion and recreation, and partition expansion.
RabbitMQ and Redis Streams fill the observer's handoff before disconnecting its
native reader. RabbitMQ also exercises a broker-stalled close. Two pytest-xdist
processes reach READY on one owned source before the independent producer
publishes, and each process must receive that publication.
[Fault inventory](../tools/broker_fault_cases.json) · [Running the suite](testing.md#owned-broker-faults-and-process-lifecycle)

On November 15, 2026, all six profiles passed against the same installed
wheel on macOS arm64 with CPython 3.14.7 and Docker Engine 29.8.0
(`linux/arm64`):

| Broker | Dependency profile | Faults | Lifecycle | Independent pytest-xdist readers |
| --- | --- | ---: | ---: | ---: |
| Kafka 3.9.2 | Current | 20 | 2 | 2 |
| Kafka 3.8.1 | Minimum | 20 | 2 | 2 |
| RabbitMQ 4.4.0 | Current | 14 | 2 | 2 |
| RabbitMQ 4.3.7 | Minimum | 14 | 2 | 2 |
| Redis 8.2.11 | Current | 12 | 2 | 2 |
| Redis 7.0.15 | Minimum | 12 | 2 | 2 |

The 116 cases passed without skips. A separate Kafka 3.9.2 replay passed all
30 base contracts and both original Toxiproxy routing proofs. Local manifests,
JUnit, READY identities, and first-failure outputs are under
`artifacts/broker-faults/stage4/`. Wheel and sdist metadata and MIT identity
checks also passed. Docker reported zero remaining fixture-owned containers.

Each real-broker lifecycle check warms the independent producer, measures six
successful lifecycles and four rolled-back starts, and checks the remaining
resources. Local macOS measurements cover Python threads and async tasks.
The Linux CI job also samples native threads and sockets through `/proc`.
The broker-free lifecycle cases check Ctrl+C, hung matchers and close calls,
cleanup error priority, and repeated normal and fault cleanup. All 10 cases
passed with two workers on macOS arm64 and Python 3.14.7. They also passed
under Python 3.11.16 with the exact minimum test dependencies and the installed
wheel from run 43743613831. The complete local suite passed 1,087 cases both
serially and with two workers, with 88.97% combined statement and branch coverage.

The initial Kafka SASL test returned a connection error for invalid credentials.
The worker now services queued native error callbacks when startup metadata
fails, so the SASL failure reaches the caller as an authentication error.
The ACL checks verify a separate authorization error. The initial minimum Kafka
retention check also exposed a reproducible crash in the client's fixture-only
`delete_records()` call. The fixture now performs the same broker operation
through the CLI in its owned container and verifies the returned low watermark.
[Client issue](https://github.com/confluentinc/confluent-kafka-python/issues/2275) · [Upstream fix](https://github.com/confluentinc/confluent-kafka-python/pull/2328)

The first fault artifacts also preserve a RabbitMQ port change after restart,
Toxiproxy closing the socket when a toxic was removed, and a repository-test
import failure under isolated Pytest 8. The fixtures now bind restart-stable
loopback ports, keep the toxic installed until the observer closes, and put the
repository root on pytest's import path. Pytest loads the package from the
installed wheel in the compatibility and broker jobs.

The new CI steps and artifacts still need hosted verification. Linux native
resource counts and Windows console Ctrl+C are required in that verification.
The fault reports will be registered with the release audit at stage 7.

## Isolated real broker contracts (plan stage 3)

On November 15, 2026, all six profiles passed against one installed candidate
wheel on macOS arm64 with CPython 3.14.7 and Docker Desktop Engine 29.8.0
(`linux/arm64`). The current profiles used Testcontainers 4.15.0 and the locked
clients. The older broker profiles used Testcontainers 4.9.0 and the exact
minimum Python 3.14 dependency pins. [Pinned image inventory](../tools/broker_profiles.json)

| Broker | Dependency profile | Async passed | Blocking passed | Declared capability exclusions across both modes |
| --- | --- | ---: | ---: | ---: |
| Kafka 3.9.2 | Current | 15 | 15 | 0 |
| Kafka 3.8.1 | Minimum | 15 | 15 | 0 |
| RabbitMQ 4.4.0 | Current | 11 | 11 | 8 |
| RabbitMQ 4.3.7 | Minimum | 11 | 11 | 8 |
| Redis 8.2.11 | Current | 12 | 12 | 6 |
| Redis 7.0.15 | Minimum | 12 | 12 | 6 |

The 180 registered cases produced 152 passes and 28 matching capability
exclusions. All four additional Kafka routing checks passed with Toxiproxy
2.13.0. Both Kafka versions ran the upstream Apache KRaft entrypoint. Docker
published no Kafka host port, and metadata returned the proxy address.
Disabling the proxy interrupted a warmed native metadata request and each
active reader. Re-enabling it restored discovery before topic cleanup.
[Routing test](../tests/integration/test_kafka_proxy_routing.py)

Registry inspection confirmed each pinned index has Linux amd64 and arm64
manifests. The local runner verified the actual server versions, Docker
image identities, architecture, and assigned loopback ports. Docker Desktop
omitted the live platform manifest descriptor. A separate partial-start
probe on Testcontainers 4.9.0 forced an invalid entrypoint, retained its
diagnostic, and confirmed removal of only the failed fixture container.
The final six-profile run found zero owned containers left behind and identical
sets of pre-existing running containers, Docker networks, and volumes. Local
JUnit, manifests, sanitized log tails, and the summary are under
`artifacts/broker-contracts/stage3/final/`.

CI has a six-cell Linux x64 producer for the 12 RG-03 backend profiles. The
inventory lists every case and the allowed capability exclusions. The jobs
install the same-run wheel and attach the checksummed JUnit and image manifests
to release-gate evidence. A local schema rehearsal replayed the real JUnit with
synthetic CI identity and confirmed the validator accepted all 12 RG-03 inputs
and blocked publication on the other 77 profiles. The remaining fault, reporting,
and resource gates stay open.
[CI workflow](../.github/workflows/ci.yml) · [Gate inventory](release-gates.json)

### First hosted broker matrix run

The November 15, 2026 push run for `7e918139e61f5713767666cb6593b9e9caff20c8`
passed five of six isolated broker profiles and 23 of 24 compatibility cells.
The failed aggregate broker job result also blocked the release audit. The
original artifacts and logs record the first failures. [CI run 43737443845](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43737443845)

- The Compose Kafka job failed before the async Protobuf observer became ready.
  An acknowledged test topic returned `UNKNOWN_TOPIC_OR_PARTITION` during the
  initial metadata lookup. The harness now waits for all partition leaders and
  valid metadata within the creation deadline. The local Kafka 3.9.2 replay
  passed 39 tests and two separate Toxiproxy tests. All 30 contracts and both
  proxy tests also passed on Kafka 3.8.1 with the minimum profile and rebuilt
  wheel. [Kafka job](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43737443845/job/132883057602) · [Kafka Admin API](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/KafkaAdminClient.html#createTopics(java.util.Collection,org.apache.kafka.clients.admin.CreateTopicsOptions))
- The RabbitMQ 4.3.7 blocking isolation case received a publisher `Basic.Nack`
  immediately after closing one of two independent readers. RabbitMQ can remove
  queue bindings asynchronously on automatic deletion. Both healthy reader
  paths now wait for `Queue.DeleteOk` on their own queue before disconnecting.
  The original `Basic.Nack` did not recur in local unfixed stress attempts. The
  corrected minimum profile passed 11 async and 11 blocking cases on both the
  native arm64 image and the pinned amd64 image under Docker emulation. Each mode
  recorded four declared capability exclusions. [RabbitMQ job](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43737443845/job/132883184913) · [RabbitMQ queue deletion implementation](https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.7/deps/rabbit/src/rabbit_amqqueue_process.erl#L350-L421)
- The minimum macOS Python 3.11 cell missed the 50 ms scheduling budget for the
  follow-up command in the sync cancellation test. The intentional cancellation
  still uses 50 ms and the recovery assertion now has its own 2 s budget.
  The complete minimum macOS arm64 cell passed on Python 3.11.16 with the rebuilt
  wheel and sdist. Its 18 JUnit reports contain 2,263 passing case executions.
  [macOS job](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43737443845/job/132883185106)

The local unit suite passed 1,076 cases with 89.05% combined statement and branch
coverage. The two-worker run also passed all 1,076 cases.

### Hosted follow-up for Kafka startup

The November 15, 2026 push run for `20cc574b011f11607eba3ace3410af4cdf325f8a`
passed all 24 compatibility cells, including minimum macOS Python 3.11, and
five of six isolated broker profiles, including minimum RabbitMQ 4.3.7.
[CI run 43739791962](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43739791962)

- In the isolated Kafka 3.9.2 profile, the async isolation test failed during
  startup in the dedicated AdminClient's `ListOffsets` call. The native error
  reported `Failed to query partition leaders` and the other 14 async cases
  passed. The profile stopped before its blocking and Toxiproxy checks.
  [Isolated Kafka job](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43739791962/job/132890332749)
- The Compose Kafka contracts and pytest examples passed. The Allure
  expected-timeout example hit the same offset query's 1 s request timeout
  while entering the watcher, before it could publish. Report generation and
  upload succeeded. [Compose Kafka job](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43739791962/job/132890188845)

A cold librdkafka handle can miss its first asynchronous leader metadata
refresh while the broker connection is opening. Its next forced refresh can
fall outside the configured request timeout. The shared Kafka worker now
explicitly fetches the topic's metadata on the same AdminClient before
`ListOffsets`, with auto-creation disabled and the existing timeout limits
preserved. Unit regressions exercise the request order in both execution modes
and verify typed authorization failure before assignment.
[Initial refresh](https://github.com/confluentinc/librdkafka/blob/v2.16.0/src/rdkafka_metadata.c#L1351-L1366) · [Leader query interval](https://github.com/confluentinc/librdkafka/blob/v2.16.0/src/rdkafka_partition.c#L3709-L3726)

Local verification on macOS arm64 with CPython 3.14.7 and Docker Linux arm64
passed 41 Compose Kafka cases, including both Allure examples. The 32 skips
were other broker cases without configured endpoints. Each isolated profile
passed 15 async contracts, 15 blocking contracts, and two Toxiproxy checks with
the same installed wheel. Kafka 3.9.2 used confluent-kafka 2.16.1. Kafka 3.8.1
used the exact Python 3.14 minimum, 2.12.1. The unit suite passed 1,077 cases
with 89.03% combined statement and branch coverage. Local manifests, JUnit,
and Allure results are under `artifacts/broker-contracts/actions-43739791962-fixes/`.

The release audit for this run was invalid because a required producer job
failed. It reported `ready: false`. The diagnostics and audit are retained
locally under `artifacts/broker-contracts/actions-43739791962-attempt-1/`.
[Release audit](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43739791962/job/132892692162)

### Hosted verification of stage 3

On November 15, 2026, both Kafka CI paths passed for
`a89b5078621adbd7122a983e2cfe2d00acac9842`, including Compose Allure examples.
All six isolated broker profiles passed on Linux amd64 with CPython 3.14.10.
The 12 sync and async contract reports recorded 152 passes and 28 registered
capability exclusions. Four additional Toxiproxy checks passed, two on each
Kafka version. [CI run 43742126666](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43742126666)

The gate audit confirmed all 12 RG-03 profiles for this commit and the first
workflow attempt. A local replay of the downloaded evidence produced a
byte-identical audit report. The wheel and sdist checksums matched the
packaging artifact and all broker manifests. Stage 3 verification is complete
for this candidate. The audit reported `ready: false`, with 77 profiles still
open across 14 gates. The overall workflow failed the Windows compatibility
cell described below. [Release audit](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43742126666/job/132899893870)

The downloaded broker manifests, JUnit reports, release audit, and Windows
first-failure artifact are under
`artifacts/broker-contracts/actions-43742126666-attempt-1/`.

## Python, platform, and dependency compatibility (plan stage 2)

The [compatibility CI job](ci-compatibility.md) defines 24 required cells:
CPython 3.11–3.14 on Linux x64, macOS arm64, and Windows x64 with both exact
[minimum and locked-current profiles](compatibility-profiles.json). Every cell
uses the same wheel/sdist pair from its packaging job and checks the hashes
before and after execution.

The November 8 [floor evidence](dependency-floor-evidence.json) preserves the
failed originals and passing replacements. Allure 2.13.0 failed with pytest
8.3.0; 2.13.5 passed. Kafka 2.6.0 lacked a Python 3.14 wheel; 2.12.1 installed
and passed the disconnected SDK probe. Kafka 2.6.0 remains the floor for Python
3.11–3.13. RabbitMQ's isolated extra exposed an undeclared `typing_extensions`
import in aio-pika 9.5.0–9.5.2; 9.5.3 passed without that dependency. Direct
session tests now close their caller-owned iterators, so the reproduced
unawaited-coroutine failures did not require raising pytest-asyncio's floor.
The initial runner also rejected valid standalone pytest JUnit with an empty
class name; it now uses the file/name identity and retains strict result checks.

The local follow-up on November 15 verified the wheel/sdist build, MIT identity,
distribution metadata, `twine check`, the SPEC traceability inventory, Pyright,
and the pre-commit hooks. The broker-free development suite passed **1,061 tests**
(114 integration cases deselected) at **88.96%** combined statement/branch
coverage, measured before pytest plugin loading. The test helper now blocks
broker connections after asyncio's internal socket setup; two regressions
simulate the Windows startup sequence on macOS. Git attributes preserve the
exact license and Protobuf fixture bytes on Windows checkouts.

All **eight local cells** passed on macOS arm64 (Darwin 27.0.0) using
uv 0.12.19 and Pyright 1.1.414. All cells used the same newly built candidate
wheel and sdist:

| CPython | Exact minimum | Locked current |
| --- | --- | --- |
| 3.11.16 | Passed | Passed |
| 3.12.14 | Passed | Passed |
| 3.13.15 | Passed | Passed |
| 3.14.7 | Passed | Passed |

Each cell recorded 18 JUnit reports and **2,233 passing case executions** with
no failures or skips: 1,061 unit tests from the installed wheel, the same suite
with two xdist workers, 54 parity cases from the installed sdist, 15 examples per
artifact, and 27 installed-package and isolated-extra checks. The examples used
the actual Allure plugin; the opt-in intentional failure and broker tests were
deselected. Installed consumer typing passed for both artifacts, and its
deliberately incorrect-type control failed as expected. The local audit compared
both artifact hashes in every manifest and independently checked each JUnit hash
and result count. Manifests, reports, and `summary.json` are retained locally under
`artifacts/compatibility/local-2026-11-15/final/`.

### GitHub Actions verification

All **24 cells** passed on the first attempt on November 15, 2026, for commit
`db3913fdf8f4463e24748cb861cac0c0e4f7e249`. Both dependency profiles passed
on each listed interpreter. [CI run 43734651319](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43734651319)

| Runner | CPython patch versions | Passed cells |
| --- | --- | ---: |
| Linux x64 | 3.11.18, 3.12.16, 3.13.18, 3.14.10 | 8/8 |
| macOS arm64 | 3.11.9, 3.12.10, 3.13.17, 3.14.10 | 8/8 |
| Windows x64 | 3.11.9, 3.12.10, 3.13.17, 3.14.10 | 8/8 |

All 24 manifests matched the commit, run, attempt, and SHA-256 hashes of the
wheel and sdist produced by the packaging job. The artifact audit verified
432 JUnit files with 53,592 passing case executions and confirmed the typing
check and its negative control for both package forms in every cell. The Core,
packaging, quality, Allure example, Kafka, RabbitMQ, and Redis Streams jobs also
passed. Downloaded evidence is retained locally under
`artifacts/compatibility/actions-43734651319-attempt-1/`.

The push workflow completed successfully. Its `release-gates-report` recorded
`ready: false` with 89 outstanding profiles across 15 gates. Stage 2 matrix
verification is complete. Registering the mandatory cases and producing the
release evidence still belong to stage 7.

#### Windows reporting test follow-up

The first attempt for `a89b507` passed 23 of 24 compatibility cells. Windows x64
with CPython 3.11.9 and current dependencies passed the serial unit run.
In the two-worker run, 1,076 tests passed and the scope exclusion reporting
test recorded zero excluded events where it expected one. The test started a
10 ms deadline immediately after FakeTransport queued a publication, so
admission could occur after the deadline.
[Windows job](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43742126666/job/132897547284)

The scope exclusion, redacted mismatch, and partial count reporting tests now
wait for the expected events to reach the Journal before starting their
10 ms timeout checks. An injected 50 ms delivery delay reproduced the missing
diagnostics in all five affected test variants on macOS arm64. With the
admission checks, all five passed under the same delay. The full local
two-worker unit run passed 1,077 cases with CPython 3.14.7. The first failure
and controlled replay remain available under the run artifacts and
`artifacts/compatibility/actions-43742126666-test-fix/reporting-delay/`.

The complete current-dependency check on macOS arm64 with CPython 3.11.16
passed with the downloaded wheel and sdist from the hosted run and the
corrected tests. Its 18 JUnit reports contain 2,265 passing case executions,
including both runs of the 1,077 unit tests. The results are under
`artifacts/compatibility/actions-43742126666-test-fix/macos-arm64-cpython-3.11-current/`.

The next hosted run, for `024989a14bd02e0ccc67b5050724debc2cb4ad80`, passed all
41 check jobs on the first attempt. Windows x64 with CPython 3.11.9 and current
dependencies passed all 1,077 unit cases in both the serial and two-worker
runs, including the previously failing scope exclusion case.
[Windows job](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43743613831/job/132902116476)

All 24 compatibility cells passed. A local artifact audit checked the exact
candidate and distribution hashes, 432 JUnit reports with 54,360 passing case
executions, and both installed typing checks with their negative controls in
each cell. The downloaded reports are under
`artifacts/compatibility/actions-43743613831-attempt-1/`.
[CI run 43743613831](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43743613831)

The run also passed all six isolated broker jobs, all four Kafka proxy checks,
and both Kafka CI paths. The release audit accepted the 12 RG-03 profiles for
this commit. It reported `ready: false` with 77 profiles open across 14 gates.
The locally replayed report matched the hosted report byte for byte. The audit,
broker manifests, and distributions are under
`artifacts/broker-contracts/actions-43743613831-attempt-1/`.
[Release audit](https://github.com/valkorobov/pytest-broker-observer/actions/runs/43743613831/job/132904314365)

## License, identity, and traceability (plan stage 1)

The owner-selected MIT license is now present in `LICENSE`, with copyright
`2026 Valerii Korobov`, SPDX metadata, explicit license-file inclusion, and public
repository/issue URLs. The SPEC administrative addendum replaces the working
distribution name with `pytest-broker-observer`; its behavioral revision stays 1.6.
The [preflight record](release-preflight.json) distinguishes owner decisions,
observed remote settings, historical CI, and unexecuted production checks.

Authenticated inspection on November 8, 2026 confirmed:

- A pending PyPI publisher bound to `valkorobov/pytest-broker-observer`, workflow
  `ci.yml`, environment `pypi`. The public project JSON returned 404. A pending
  publisher does not reserve a name or demonstrate a successful OIDC upload.
- GitHub environment `pypi` now requires reviewer `valkorobov`, allows self-review,
  and permits deployment only from tags matching `v*`, as authorized by the owner.
  A REST API readback confirmed the settings. These are deployment restrictions;
  the repository rulesets endpoint returned no repository tag-protection rules.
- The latest observed green GitHub run was
  [42476213696](https://github.com/valkorobov/pytest-broker-observer/actions/runs/42476213696),
  for earlier SHA `f507dd298cd4f0e3a2fe91ada931a6de4a936035`.
  It does not verify the current stage 0/1 changes.

The [traceability inventory](traceability.json) covers all 88 AC criteria and
128 physical MUST lines: 125 MVP obligations, two vocabulary entries, and one
deferred temporal entry. Each MVP row identifies directly defined tests or
explicit staged missing checks, expected outcomes, modes, and target platforms.
The [readable mapping](acceptance.md) preserves both historical Kafka first
failures as unresolved stage 4 work; passing diagnostic reruns do not erase them.
CI now checks the inventory and candidate identity, and analyzes a copied public
consumer against both clean installed distributions with no source-checkout path.

Local verification on November 8, 2026, CPython 3.14.7 / macOS arm64:

- 69 identity and 28 traceability CLI regressions passed, including omitted/stale
  requirements, invalid selectors, changed MIT terms, mismatched Python/optional
  dependency metadata, duplicate identity fields, missing typing markers,
  unexpected artifacts, links, and unsafe archive paths.
- The two-worker broker-free suite passed 988 cases. Combined package unit
  statement/branch coverage remained 88.86%, measured before plugin loading.
- The focused pytest plugin suite passed all 12 cases. Broker-free pytest/Allure
  examples passed 15 cases; the intentional demonstration failure was explicitly
  skipped and 12 broker examples were deselected.
- Pre-commit, Pyright, and the Sphinx audit passed (709 definitions / 679 attributes).
- Wheel built from sdist, Twine, and exact source/archive MIT/identity checks passed.
  Clean wheel/sdist installations each passed stdlib-only imports, `py.typed`,
  installed origin, loop-free sync observation, and both pytest-only factory smokes.
- Installed consumer analysis passed for raw/custom-decoder/message-type overloads,
  sync/async contexts, scopes, payload/event/count results. Each artifact analyzed
  one consumer file; an intentionally incorrect `assert_type` produced the expected
  error. Analysis uses the project's `basic` mode. An exploratory `strict` run
  reported 24 unknown-member diagnostics from incomplete public parameter
  annotations; stricter typing and the full profile matrix remain stage 2 work.

Independent review identified missing optional dependency/range comparison and
`Requires-Python` comparison in the first checker draft. Both were corrected and
covered by negative tests. Clean-install checks also caught venv-interpreter
symlink resolution and macOS path-alias issues in the new typing helper; both
were corrected before final verification.

Stage 1 completes license/identity preparation and the existing-or-missing check
inventory. It does not close RG-01–RG-15: candidate-bound producer evidence,
the full matrix, fault/resource/report checks, final administrative rechecks,
and an owner-authorized release are still required. No release, push, or PyPI
upload was performed.

## Release publication interlock (plan stage 0)

The [release gate inventory](release-gates.json), stdlib-only validator
`tools/check_release_gates.py`, and unprivileged `release-gates` CI job now
protect the pending publication workflow. The initial inventory had 15
planned checks and 89 profiles across RG-01–RG-15. Stage 3 registered the
12 RG-03 profiles for execution.

The verifier binds the repository, source SHA, workflow run/attempt, version/tag,
successful GitHub producer job, exact distribution hashes, and hashed JUnit
cases. Hidden rerun failures, unexecuted cases, unregistered skips, missing cases,
ambiguous documents, traversal/symlinks and altered files block publication.
Registered capability skips retain ordered precondition evidence. An unrelated
passing case cannot replace all required cases being skipped.

Release runs require strict success and publish only `verified-release`, with
the original distribution bytes, compatibility data, raw declared evidence,
evaluated gate report and hash manifest. Non-release runs audit the expected
open checks without creating that bundle; malformed evidence still fails.
See [the evidence contract](release-gate-evidence.md).

Local verification on November 8, 2026, CPython 3.14.7 / macOS arm64:

- 79 release-validator CLI regressions passed. Complete green fixtures are
  explicitly synthetic schema tests and do not close real gates.
- The actual planned inventory with simulated green legacy job results remained
  blocked for all 15 gates / 89 profiles. Audit exited 0, strict verification
  exited 1, and neither created a publication bundle.
- The full broker-free unit suite passed 891 cases under coverage; two-worker
  xdist also passed 891. Combined package coverage was 88.86%.
- Pre-commit, Pyright, and the source audit passed (709 definitions / 679 attributes).
- Wheel/sdist build and Twine checks passed. The source archive includes the
  validator/inventory/evidence documentation/tests and excludes generated results.
- Both clean installed artifacts passed stdlib-only, typing-marker, loop-free
  observation and pytest-only factory smoke checks (2 cases per installation).
- Local workflow inspection verified the strict dependency path, same-run bundle
  selection, SHA-pinned actions and isolated publishing permissions.

Independent review findings around hidden first failures, skipped required cases,
runtime/profile identity and ordered capability metadata were corrected. No
GitHub run or PyPI upload was performed; completing this interlock does not
complete the remaining release gates or authorize publication.

## PyPI release workflow

The `CI` workflow now handles published GitHub Releases. A release tag must
match `v<project.version>`. Publication depends on every existing CI job,
including the Python 3.11–3.14 unit/coverage matrix, packaging smoke tests,
quality checks, Allure examples, and all three broker suites. The `publish`
job uses the `pypi` environment and OIDC Trusted Publishing; its input is the
packaging job's verified wheel/sdist artifact with checked SHA-256 hashes.
The publishing action generates distribution attestations.

Local verification on November 8, 2026 with CPython 3.14.7:

- YAML structure, release-only upload, all-job dependencies, isolated OIDC
  permissions, and valid/invalid tag checks passed.
- Original distribution checksums passed; deliberate artifact modification
  was rejected by the same checksum command used in CI.
- Wheel/sdist builds and Twine metadata checks passed. The first offline build
  used an empty temporary cache; the successful build used the available
  standard local cache.
- Both isolated wheel and sdist installations passed stdlib-only imports,
  typing-marker and loop-free observation checks. With only pytest 9.2.0 added,
  each installation passed both installed factory smoke tests.
- The focused pytest plugin suite passed 12 cases; two-worker xdist passed
  812 broker-free cases. Pre-commit, Pyright, and the 709-definition /
  679-attribute Sphinx audit passed.

No package was uploaded. This historical workflow verification preceded the
stage 1 license and administrative preflight above; an actual release CI/OIDC
result remains unverified. Follow
[the publishing guide](publishing.md), using workflow filename `ci.yml` and
environment `pypi`. The remaining publication gates below still apply.

## Pytest constructor fixtures (SPEC 1.6)

The existing `event_observer` entry point now registers five session-scoped
fixtures: `kafka_transport_factory`, `rabbitmq_transport_factory`,
`redis_streams_transport_factory`, `sync_observer_factory`, and
`async_observer_factory`. They return the existing public constructors.
Projects configure their own fixtures; watch contexts still own active resources.
The entry-point module remains stdlib-only, broker adapter imports are deferred
until fixture resolution, and native clients remain deferred until observation.

Local verification on November 4, 2026 used CPython 3.14.7 on macOS arm64,
pytest 9.2.0, the locked optional clients, and the pinned Compose Kafka 3.9.2,
RabbitMQ 4.4.0, and Redis 8.2.11 services:

| Check | Result |
| --- | --- |
| Broker-free unit suite | 812 passed; includes 12 new plugin regression cases |
| Combined unit statement/branch coverage | 88.86%; both pytest integration modules have 100% coverage |
| Unit suite with two xdist workers | 812 passed |
| Broker-free pytest/Allure examples | 15 passed, 1 opt-in demonstration skipped |
| Updated sync/async factory examples on all three brokers | 6 passed |
| Full broker integration suite plus pytest/Allure integration examples | 112 passed, 14 explicit capability skips |
| Clean wheel/sdist installation without extras | Stdlib-only imports, typing marker, and blocking wheel observation passed |
| Installed-wheel fixtures with pytest 9.2.0 only | 2 passed; all factories resolve and construct objects without optional clients or background work |
| Installed-sdist fixtures with pytest 8.3.0 only | Same 2 smoke tests passed |
| Pytest-only runnable examples from the installed wheel | 5 passed, 2 optional async/Protobuf modules skipped |
| Pre-commit, Pyright, source docstring audit | Passed; 709 definitions and 679 attributes documented |
| Wheel/sdist build and Twine metadata | Passed |

`tests/test_pytest_plugin.py` covers constructor identity and policies, fixture
scope composition, overrides, explicit load/disable, autoload, custom transports,
and isolated no-resource construction with optional imports blocked. Packaging
CI now exercises fixture discovery from an installed wheel rather than source
imports alone. Factory recipes are in the
[pytest examples](../examples/pytest/README.md#factory-fixtures-and-project-configuration).

These local runs do not establish the full Python/platform/dependency-floor
matrix or a GitHub Actions result. The historical results below remain scoped
to their original runs.

## Unit coverage gate

The complete `event_observer` package now has an enforced **80%** unit coverage
threshold, including transport adapters and public testing helpers. Coverage.py
measures both statements and branches; its combined percentage is the gate.
Each Python 3.11–3.14 Core CI job runs only broker-free `tests` for this
measurement, then runs usage examples separately and uploads HTML/XML/JSON
reports. See [the coverage guide](testing.md#unit-coverage).

Local verification on November 2, 2026 with CPython 3.14.7 and Coverage.py 7.17.0:

| Metric | Corrected baseline | After new unit tests |
| --- | --- | --- |
| Combined statements/branches | 86.98% | **88.68%** |
| Executable statements | 89.38% | **91.10%** (5,250 / 5,763) |
| Branch destinations | 79.42% | **81.06%** (1,485 / 1,832) |
| Broker-free unit tests | 763 passed | **800 passed** |

Coverage starts before pytest loads entrypoints, measuring the passive
plugin's Core imports. A preliminary `pytest --cov` run missed that execution
and emitted `module-not-measured`; its 75% result is not the baseline above.
No custom source omissions/exclusions were added. Existing Coverage.py default
exclusions remain unchanged, and broker/example data is not combined into the
unit measurement.

The added 35 Kafka harness tests cover acknowledgement and ambiguous/failed
publication, binary/null/duplicate headers, native budgets and off-loop work,
unique owned topics, partial creation, timeout, cleanup retry, and protection
of foreign resources. That helper now covers all 112 statements and 30 branch
destinations. Two public API regressions also cover canonical re-exports and
raw decoding; isolated import checks verify lazy blocking-adapter construction.

The configured gate rejected a deliberately incomplete 16.51% measurement with
exit code 2. The complete 800-case suite passed under coverage and with two
xdist workers; separate usage examples passed 15 cases with one opt-in
demonstration skipped. Pre-commit and Pyright passed. HTML/XML/JSON generation
passed locally. The CI configuration was reviewed independently; execution on
GitHub remains unconfirmed because the repository has no configured remote.

## Separate execution backends (SPEC 1.5)

The [six-stage migration plan](sync-async-migration-plan.md) is complete.
`event_observer.aio` exposes the async classes; `event_observer.sync` uses a
blocking observation worker with no created or driven asyncio runtime. Existing
top-level imports remain available. Both runtimes share expectation/admission
rules, and all three built-in adapters provide native blocking sessions.
Custom async-only adapters require a blocking implementation or an explicit
`sync_adapter()` provider; see [the migration guide](sync-async.md).

Final local verification on November 2, 2026 used CPython 3.14.7, macOS arm64,
the pinned Kafka 3.9.2, RabbitMQ 4.4.0, and Redis 8.2.11 Compose services,
and Pika 1.4.4 for blocking RabbitMQ:

| Check | Result |
| --- | --- |
| Complete non-integration suite plus pytest/Allure examples | 778 passed, 1 opt-in demonstration skipped, 125 integration nodes deselected |
| Core suite with two xdist workers | 763 passed |
| Async/blocking native contracts, integration tests, pytest and Allure examples | 111 passed, 14 explicit capability skips, 16 non-integration nodes deselected |
| Allure broker artifacts | 6 passed scenarios, 9 distinct wait summaries, 27 JSON attachments with `indent=2`; English text and redacted received bodies verified |
| Pre-commit: Ruff, format, Pyrefly; separate Pyright | Passed; Pyright reported 0 errors and 0 warnings |
| Sphinx source audit | 704 named definitions and 679 explicit attributes documented |
| Wheel/sdist builds and Twine metadata | Passed |
| Clean wheel/sdist without extras | Imports, `py.typed`, and blocking observation passed; wheel observation also forbids asyncio runtime creation |
| Plain pytest without pytest-asyncio or native clients | 1 passed |

Independent review and confirmed fixes are recorded in the
[migration review](sync-async-migration-review.md). Findings included startup
deadline overruns, invalid-session rollback, shutdown publication ordering,
settlement coroutine rejection, cancellation acknowledgement, and primary-error
priority. The final review has no unresolved findings within the implemented
scope. Initial RabbitMQ fault-test failures and their corrections remain in
that record; the final full-broker run passed without automatic retry.

These local checks do not verify the full CI matrix, minimum dependency
versions, production ACL/TLS, all network faults, or load/RSS release gates.

## Observation marks

Call `watcher.mark()` before an application action and pass the result as
`since=mark` to any wait method, including scoped waits. Capture is an ordinary
method in both facades; an async wait is still awaited. The frozen marker
records watcher identity, next Journal sequence, and informational UTC and
monotonic times. Timeout begins with the wait call, and a scoped wait uses the
later of its existing lower boundary and the mark, preserving its filters.

The marker is reusable within one watcher and does not create a scope,
consumer, seek, replay, or report. Capture consumes no scope/waiter/result
quota; subsequent expectations retain normal waiter/result limits without
registering another scope. Snapshots keep
their existing definitions; expectation diagnostics report the effective
lower bound, including `Journal lower bound` in the text explanation.
Markers do not pin history, and strict retention checks remain in force.
Foreign-watcher markers and creation after closure are rejected.

The runnable sync/async examples establish old-event admission without sleeps,
capture a mark, perform the action, and reuse that mark after a new matching
event is already retained. Run the focused usage checks with:

```shell
uv run pytest -c pyproject.toml examples/pytest/test_sync.py examples/pytest/test_async.py -k mark -v
```

The regression scope also includes ownership, lifecycle, parent-boundary
intersection, all wait variants, snapshots, quotas, and retention loss. A mark
selects Journal admission order, rather than broker publication time; delayed
earlier publications can still be admitted afterward. Keep correlation in
place. See [the guide](scopes.md#reusable-observation-marks).

Local marker validation on November 2, 2026 returned 575 passed, 1 skipped,
and 78 deselected without brokers; the core suite passed 560 tests with two
xdist workers. All 11 pytest and Allure examples passed against the running
Compose Kafka, RabbitMQ, and Redis Streams services. Each marked success report
contained one summary, the effective Journal lower bound, formatted JSON, and
a redacted received body. Pre-commit, Pyright, wheel/sdist builds, and Twine
metadata checks passed.

## Reproducible checks

The SOLID/DRY/KISS hardening and Sphinx source documentation were checked on
November 2, 2026: 618 non-integration tests passed, 1 was skipped, and 78 were
deselected; core xdist passed 603. The final broker run passed 71 cases with 7
capability skips. The source audit covers 564 definitions and 539 attributes.
See the [implementation plan](improvement-plan.md) and [follow-up review](solid-dry-kiss-review.md)
for changes, checks, and the first Kafka partition-leader timeout retained in
the verification record.

Local run on November 1, 2026: CPython 3.14.7, macOS arm64.
Exact dependency versions: [compatibility.json](compatibility.json).

| Check | Result |
| --- | --- |
| Unit/Core/parity/contracts/reliability/reporting | 337 passed |
| pytest-xdist, two processes | 337 passed |
| Ruff lint/format and Pyright | No errors or warnings |
| Wheel + sdist, Twine metadata check | Built, metadata check PASSED |
| Clean wheel installation without extras | Stdlib-only import, `py.typed`, and sync observation passed |
| Clean sdist installation without extras | Import and `py.typed` passed |
| Plain pytest without pytest-asyncio/Kafka/Protobuf/Allure | 1 passed |
| Runnable pytest examples | 9 passed without a broker; separate Kafka example — 1 passed; in a pytest-only environment, 4 passed, 3 skipped |
| Real Kafka 3.9.2 / KRaft / arm64 | 22 passed: shared contracts, replay, topology, transactions, sync/async + Protobuf |
| Local Kafka through Compose | 23 passed including the pytest example; a custom host port and data persistence after `down`/`up` verified |
| Pytest examples with real AllureReporter | 4 passed, 1 skipped; opt-in demonstration — 4 passed, 1 intentional FAILED; JSON attachments and redaction verified |
| HTML Allure 3.22.1 / Node.js 24.19.0 | Regular and demonstration reports built as standalone HTML |
| Kafka examples with AllureReporter / Compose | 2 passed on a real broker; without a Kafka address — 2 skipped |
| Allure report readability | JSON with `indent=2` and limits; titles with transport and topic/stream, aliases, and watcher isolation verified |
| Expectation text explanations | `expectation-result.txt` with outcome reason, timing, and counters; safe export, UTF-8 limits, and text/plain verified |
| Automatic reports without duplicates | New expectations are sent once; errors after success and full manual export are preserved, async/sync and concurrent waiters verified |
| Core with pytest and Allure examples without a broker | 350 passed, 1 skipped, 25 deselected |

The English-language migration was checked on November 2, 2026: examples,
documentation, and report labels use English; Unicode byte-limit coverage is
preserved. Allure generation in local commands and CI selects English explicitly.

Matched-content diagnostics were checked on November 2, 2026: 384 tests passed
with 1 skipped and 25 integration scenarios deselected; the core suite passed
371 tests with two xdist workers. Both updated Kafka Allure examples passed on
the local Compose broker. Their HTML report contains redacted received bodies,
three distinct expectation summaries, formatted JSON, and English labels.
Pre-commit, Pyright, package builds, and Twine checks passed. Regression coverage
includes metadata-only defaults, failed redactors, Protobuf/raw content,
partial timeout/cancellation, first/last samples, and shared byte budgets.

A separate full Kafka run on November 2 returned 22 passed and 1 failed:
`test_isolation_sessions_both_observe_one_publication` failed during reader
startup with `TransportConnectionError` (`code=None`). Its isolated recheck
passed, but the original failure's cause remains unconfirmed. The two updated
Allure examples do not replace validation of that broader integration suite.

RabbitMQ support was checked on November 2, 2026 with RabbitMQ 4.4.0,
aio-pika 9.6.2, aiormq 6.9.4, and pamqp 3.3.0. The pinned Compose profile
passed its health check. Contracts, observer integration tests, and sync/async
pytest examples returned 17 passed and 4 explicit `NOT_APPLICABLE` skips
(earliest replay, null bodies, duplicate headers, and message keys). Both
RabbitMQ Allure examples passed with redacted bodies and unique expectation
summaries. Broker inspection after teardown found no remaining test queues.

The first integration run exposed pamqp's binary-header requirement; the
harness now encodes bytes as native bytearrays, and the adapter detaches them
back to bytes. A real codec roundtrip covers binary, UTF-8, empty, and null
header values. Flow control was checked with 50 confirmed publications,
prefetch=1, and a one-event handoff. The full non-integration run returned
449 passed, 1 skipped, and 48 deselected; the core xdist run passed 436 tests.
Controlled native adapter and harness checks cover startup failure/cancellation,
acknowledgement order, decode/overflow failures, consumer/connection loss,
independent monitors, deadlines, and owned-resource cleanup. Production ACL/TLS,
network fault, restart, and multi-version broker coverage remain release gates.

Redis Streams support was checked on November 2, 2026 with Redis 8.2.11 and
redis-py 8.1.0. The digest-pinned authenticated Compose profile passed its
health check. Shared contracts, observer integration checks, and pytest
examples returned 25 passed and 3 explicit `NOT_APPLICABLE` skips (null bodies,
duplicate/null header values, and separate message keys). Both Allure examples
passed with redacted bodies, distinct expectation summaries, and English HTML.
Broker inspection after teardown found no remaining test keys.

Real checks covered independent cursors, available-history replay, exclusive
native IDs, a deleted stream tail, binary headers, Protobuf, preservation of an
application consumer group and its pending entries, conservative XDEL/XTRIM
failure, and targeted observer connection loss without a business message.
Controlled-native tests additionally cover atomic read-only snapshots, server
and connection identity changes, cursor availability/future-ID policies,
bounded buffers, validation, cancellation, and cleanup. The final non-broker
run passed 548 tests with 1 skipped and 78 deselected; core xdist passed 535.
Pre-commit, Pyright, wheel/sdist builds, and clean imports remain part of the
candidate checks. Dependency floors, ACL/TLS, cluster/Sentinel, fault/load,
multi-version testing, and metadata-equivalent stream recreation between polls
are not covered by these local results.

The first real-broker run revealed a difference in `get_watermark_offsets`
under `read_committed`: the client returned LSO instead of the physical high
watermark. The fix uses worker-owned AdminClient ListOffsets with
READ_UNCOMMITTED for the boundary while retaining READ_COMMITTED for the
reader. A separate native regression and a repeated full integration run
confirm the fix; the initial failure was not hidden by automatic retry.

These results apply to the local candidate. They do not replace the full CI
matrix, dependency floor verification, or the remaining release gates below.

```sh
uv sync --all-groups --all-extras
uv run pytest -m 'not integration'
uv run pytest -n 2 -m 'not integration'
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv build --no-sources
uv run twine check dist/*
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest tests/integration/test_kafka*.py -v
RABBITMQ_URL=amqp://observer:observer@127.0.0.1:5672/ uv run pytest tests/integration/test_rabbitmq*.py -v
REDIS_URL=redis://:observer@127.0.0.1:6379/0 uv run pytest tests/integration/test_redis_streams*.py -v
```

Running the Allure examples, the separate demonstration failure, and viewing HTML:
[examples/allure](../examples/allure/README.md).

Without `KAFKA_BOOTSTRAP_SERVERS`, `RABBITMQ_URL`, or `REDIS_URL`, the corresponding real
broker tests explicitly skip. A specified but unreachable broker causes a failure.
These skips do not prove broker compatibility. Native-client unit tests use a controlled implementation
of its interface and are identified separately from real-broker tests.

## Remaining work before publication

The [release gate execution plan](release-gates-plan.md) maps these remaining
items to RG-01–RG-15, implementation stages, and measurable completion criteria.
The November 8, 2026 interview selected MIT with Valerii Korobov as copyright
holder, the complete mandatory checks on every PR, and GitHub-hosted benchmark
baselines with actual runner characteristics recorded. Stage 1 implemented the
license and recorded the administrative preflight; full candidate verification
remains incomplete.

- Recheck the observed pending publisher, name status, and deployment protections
  before final publication; configuration can change independently of the code.
- Register the verified compatibility checks as release evidence in stage 7.
- Verify stage 4 on GitHub-hosted runners, including native Linux resource
  counts, Windows console interruption, and the remaining broker-specific
  cases recorded in the acceptance inventory.
- RSS/throughput/latency benchmarks with a fixed environment and budgets.
- Remaining Allure reporting contexts: examples verified locally with the real
  pytest plugin and HTML, a separate CI job prepared; the full runner matrix
  still requires confirmation.
- Actual release CI/OIDC execution, distribution attestations, published hashes,
  and installed smoke checks from the owner-authorized release.

The MIT license is included in both distributions. Public publication remains
blocked by the open release gates.

Mapping SPEC areas to tests: [acceptance.md](acceptance.md). Extending and running
transport contracts: [testing.md](testing.md).
