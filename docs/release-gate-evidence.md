# Candidate evidence and the publication interlock

Stage 0 of the [release plan](release-gates-plan.md) is implemented by
`tools/check_release_gates.py` and the `release-gates` CI job. The static
[inventory](release-gates.json) describes every RG-01–RG-15 requirement and its
required producer/profile pairs. It is not a manually maintained pass ledger.

The initial inventory has 15 checks and 89 profiles, all marked `planned`,
with empty case inventories. This accurately blocks release publication until
the later plan stages implement and execute the missing checks. Existing local
test results or green legacy CI jobs cannot close these entries.

## CI behavior

The packaging job uploads `release-distributions` on successful PR, push,
manual, and release runs. It contains `dist/` and `SHA256SUMS`. The unprivileged
`release-gates` job waits for the existing CI jobs, downloads that same run's
distributions and any `release-gate-evidence-*` artifacts, and records GitHub's
actual `needs.<job>.result` values.

On non-release runs, `--mode audit` reports incomplete/planned checks with
`ready: false`. That expected open state does not fail an incremental PR.
Malformed or inconsistent existing inputs still fail the audit. The unit suite
always tests the strict validator and its rejection paths. This temporary audit
mode does not replace the full mandatory PR checks implemented in later stages.

On a release, the default `enforce` mode requires every registered check/profile
to be ready and verified. Any absent, stale, failed, malformed, or inapplicable
mandatory result exits nonzero and blocks `publish`. The privileged job cannot
start or request its `pypi` environment until this dependency succeeds.

A strict success assembles `verified-release`, containing:

- The unchanged two files in `dist/`.
- `release-gates.json`, the evaluated candidate-bound report.
- `compatibility.json`, containing the observed producer environments and counts.
- Declared hashed JUnit/supporting files and their input evidence documents in
  `evidence/<bundle>/`.
- `SHA256SUMS` covering the distributions and retained metadata/evidence.

The privileged job has no checkout, package execution, or rebuilding step. It
downloads only `verified-release` from this run, verifies its manifest, and passes
only `dist/` to the PyPI action. Other files remain reviewable evidence, not PyPI
upload inputs. Reports and bundles are retained for seven days.

## Inventory contract

The inventory schema version is 1. Every gate has `id`, `title`, and nonempty
`checks`. Each globally unique check defines:

- `id` and `producer`: stable identifiers and the exact trusted CI job ID.
- `implementation`: `planned` or `ready`. Changing this to `ready` declares
  implementation availability; it does not establish test success.
- Nonempty `profiles` with unique `id`, `required_cases`, and
  `allowed_not_applicable` lists.

Required JUnit identities have the form `classname::name`, including parameter
suffixes. Empty required-case lists block publication. A capability exception
must register exactly `node_id`, `reason`, and `precondition` in the profile;
the node must be one of its required cases. Other skips and xfails are rejected.

## Producer contract

A producer uploads an artifact named `release-gate-evidence-<unique-profile-id>`.
Its root contains `evidence.json` and all referenced files. Matrix cells must
use distinct artifact names. The tool supports both the single-artifact flat
layout and multiple-artifact directories produced by download-artifact v5.

This abbreviated example illustrates the schema; it is not real passing evidence:

```json
{
  "schema_version": 1,
  "candidate": {
    "repository": "valkorobov/pytest-broker-observer",
    "sha": "<full source SHA>",
    "run_id": "<GitHub run ID>",
    "run_attempt": "<GitHub run attempt>",
    "version": "0.1.0",
    "tag": "v0.1.0"
  },
  "producer": "<registered CI job ID>",
  "checks": [
    {
      "id": "<registered check ID>",
      "profile": "<registered profile ID>",
      "environment": {
        "python": "3.14.7",
        "platform": "Linux",
        "versions": {"pytest": "9.2.0"}
      },
      "junit": {"path": "results.xml", "sha256": "<SHA-256>"},
      "artifacts": [],
      "distributions": {
        "pytest_broker_observer-0.1.0-py3-none-any.whl": "<SHA-256>",
        "pytest_broker_observer-0.1.0.tar.gz": "<SHA-256>"
      }
    }
  ]
}
```

Real input requires the exact current repository, source SHA, run ID/attempt,
project version and release tag from the verifier's trusted context. Non-release
producer documents use `tag: null`. Placeholder identities and unknown schemas,
fields, checks, profiles or producers are rejected. Every producer must be a
direct dependency of `release-gates`, and its GitHub result must be `success`.
Each check binds the exact distribution hashes: future producers therefore
receive the packaging artifact before generating candidate evidence.

The `broker-contracts` matrix implements the RG-03 producer. Each server
profile uploads `release-gate-evidence-broker-<profile>` with the async and
blocking JUnit files and an image and dependency manifest. Kafka also attaches
the JUnit proof for its advertised endpoint. The producer checks the installed
wheel's origin and the packaging manifest before and after the tests.
[Producer job](../.github/workflows/ci.yml) · [Broker profiles](../tools/broker_profiles.json)

Runtime versions must be concrete; unknown/latest values are rejected. Explicit
Linux/macOS/Windows and CPython labels must agree with the environment. Python
versions include a patch number; platform values are `Linux`, `Darwin`, or
`Windows`. Additional hardware/benchmark metadata belongs in hashed supporting
JSON files rather than undeclared evidence keys.

The verifier parses actual JUnit case outcomes and suite counters. A check needs
at least one successfully executed **required** case. Unrelated passing tests
cannot compensate for all registered cases being inapplicable. Hidden first
failures (`flakyFailure`, `rerunFailure` and corresponding error forms), unexecuted
statuses, duplicate identities, and failed/errored cases are rejected.

Allowed capability skips must match the registered reason and the ordered
`contract_precondition` / `contract_applicability` property pairs emitted by the
shared contracts. Earlier capabilities must be `APPLICABLE`; the final failed
capability must be the registered `NOT_APPLICABLE` precondition. Keep JUnit
properties with `-o junit_family=xunit1`.

All evidence paths are relative to their bundle, without traversal or symlinks.
Referenced file bytes must match their recorded hashes. The output bundle is
newly created only after strict success, and copied bytes are checked again.
Audit mode never assembles a publication bundle.

## Trust boundary and extending the checks

The verifier validates lineage and results from **trusted CI producers**. It
does not turn arbitrary authored JUnit into authenticated test execution or
prove that a trusted producer's implementation is correct. Producer code and
inventory changes remain reviewable source changes. Synthetic success fixtures
in `tests/test_release_gates.py` test the validator only; they never close a real
gate. No repository variable, secret, or `passed` flag overrides missing evidence.

For each later plan stage, implement a producer, register its required cases and
applicability, connect it as a `release-gates` dependency, and emit actual hashed
results. Re-run the complete candidate workflow after source/profile changes;
do not combine old attempts or select artifacts from another successful run.

The static inventory SHA is recorded in the evaluated report. A candidate is
publishable only after all checks pass on its exact distribution bytes. Real
GitHub execution, environment protections, OIDC upload and published-package
verification remain separate work; local schema tests do not establish them.
