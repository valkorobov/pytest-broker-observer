# Installed-package compatibility CI

The `compatibility` job has **24 required cells**: CPython 3.11–3.14, three
platforms, and minimum/current dependency profiles. Every push, PR, manual run,
and release includes the matrix. `fail-fast: false` preserves other cells after
a failure; no cell has `continue-on-error`. Both release aggregation and
publication depend on this job.

| Platform | Explicit runner | Requested architecture |
| --- | --- | --- |
| Linux | `ubuntu-24.04` | x64 |
| macOS | `macos-15` | arm64 |
| Windows | `windows-2025` | x64 |

Runner labels select OS families, not immutable images. Each manifest records
the actual OS release, architecture, Python patch version, GitHub image version,
and tooling. See GitHub's [runner documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job).
Other architectures, PyPy, and free-threaded Python are outside this matrix.

## Profiles and execution

[compatibility-profiles.json](compatibility-profiles.json) supplies exact direct
pins. `tools/compatibility_profiles.py` verifies complete dimensions/dependencies,
extras, applicable metadata bounds, true minima, and consistency with `uv.lock`.
`minimum` uses exact direct lower bounds and records resolved transitives.
`current` uses lock-file direct versions and constrains selected registry
transitives to their lock versions; it does not resolve whatever is newest on
PyPI. Failed installs never silently upgrade direct pins.

The [floor evidence](dependency-floor-evidence.json) preserves original failures,
passing replacements, yanked test-pin status, and test iterator corrections.
Exact yanked floors are compatibility probes, not deployment recommendations.
Changing a floor requires corresponding metadata, profile, SPEC, and changelog
changes. Kafka requires binary wheels to avoid undeclared native build tooling.

The packaging job builds once. Every cell downloads the same wheel/sdist files
from `release-distributions`, verifies `SHA256SUMS` before and after execution,
and records both hashes. Targets are newly created outside the checkout with
the selected interpreter. They never install editably or run `uv sync`.
Python probes use `-I`; origin checks reject source imports and base interpreters.
See uv's [explicit target environments](https://docs.astral.sh/uv/pip/environments/).

Both artifact forms receive independent checks for:

1. Stdlib-only import, lazy construction, `py.typed`, loop-free blocking
   observation, and async observation on the caller's loop.
2. Installation of the actual `pytest` extra, followed by two factory/sync tests
   without pytest-asyncio or native broker SDKs.
3. The complete exact dependency profile, dependency consistency, real
   disconnected Kafka/aio-pika/Pika/Redis APIs, and installed consumer type
   inference with an intentionally incorrect-type negative control.
4. Functional pytest/Allure examples with real plugins and saved Allure results.
   Broker examples and the opt-in intentional red demonstration are explicitly
   deselected from this broker-free job.

The wheel environment runs the entire broker-free unit suite, then repeats it
with two xdist workers. The sdist environment additionally runs selected sync/async
API, mark, and generated-message parity tests. Runtime and unraisable-exception
warnings fail these runs; upstream deprecations remain visible. Three real
[Protobuf gencode generations](../tests/compatibility/protobuf/README.md) cover
compatible decoding and explicit rejection of newer code by an older runtime,
without skips or xfails.

Seven further clean wheel environments individually install `kafka`, `rabbitmq`,
`redis`, `protobuf`, `pytest-asyncio`, `allure`, and `contract-tests`. Each verifies
that unrelated known optional modules are absent. Async pytest and Allure run
actual plugin tests. SDK smoke does not open broker connections; real broker
contracts/faults remain stages 3–4.

Two existing unit probes deliberately inspect checkout source: the stdlib
import blocker and missing-optional-dependency subprocess probe. They remain
source checks; independent installed/minimal probes above cover distributions.
The installed suite also uses checkout tests/tools/docs as inputs.

The separate four-version Linux coverage job still starts coverage before
pytest plugin loading and requires 80% combined package statement/branch coverage.
Examples, SDK smoke, and brokers do not contribute to that measurement.

## Evidence and local reproduction

Each `compatibility-<platform-arch>-python-<version>-<profile>` artifact retains
`manifest.json`, exact requirement/constraint inputs, actual package versions
and origins, per-phase JSON/JUnit, bounded command logs, typing results, and
example Allure results. Source SHA/run/attempt are recorded when supplied by
GitHub. JUnit files are hashed. Empty, skipped, failed, retried, duplicate, or
inconsistent mandatory results fail validation. A failed cell stops at its first
unsuccessful command; a timeout preserves partial output. Upload runs after
failure too. No rerun plugin masks a failed original execution.

These manifests are not the final RG producer attestations. Gate registration
and successful full candidate evidence remain stage 7 requirements.

Extract a `release-distributions` artifact into a directory containing `dist/`
and `SHA256SUMS`, select a matching local interpreter, and run from the checkout:

```sh
uv run --locked --all-groups --all-extras python tools/run_compatibility.py \
  --profile minimum --expected-python 3.14 \
  --expected-platform macos --expected-architecture arm64 \
  --artifacts /path/to/extracted-release-distributions \
  --output artifacts/compatibility/local
```

Use the actual host platform/architecture; the single-line equivalent works in
PowerShell. Target paths do not depend on `/bin`, `/dev/null`, or activation.
The coordinating environment supplies uv/Pyright; tested environments stay
independent of its installed packages.

To reproduce the original Allure failure, create an isolated environment with
pytest 8.3.0, pytest-asyncio 0.25.0, and allure-pytest 2.13.0. Explicitly select
`tests/compatibility/floor_reproducer.py` with the root pytest config and
`--alluredir` outside the checkout. Both tests error in the fixture API.
Changing only Allure to 2.13.5 makes them pass. Exact observed versions and other
reproductions are in [dependency-floor-evidence.json](dependency-floor-evidence.json).

Configured CI is separate from successful execution. Actual local results and
outstanding remote verification are recorded in [status.md](status.md).
