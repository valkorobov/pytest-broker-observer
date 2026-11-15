# Publishing a version to PyPI

The `CI` workflow in `.github/workflows/ci.yml` publishes to **pypi.org** when
a GitHub Release is published. The tag must be exactly `v<project.version>`:
the current candidate uses `v0.1.0`. Pushes, pull requests, and manual CI runs
perform verification without uploading a version.

## One-time setup

1. Push the repository and workflow to the intended GitHub repository.
2. In that repository's **Settings → Environments**, create `pypi`.
   Configure release tag restrictions and any required reviewer approval there.
3. Configure a GitHub Actions Trusted Publisher in PyPI with these exact fields:

   | Field | Value |
   | --- | --- |
   | PyPI project | `pytest-broker-observer` |
   | Owner | `valkorobov` |
   | Repository | `pytest-broker-observer` |
   | Workflow filename | `ci.yml` |
   | Environment | `pypi` |

For the first upload, create a pending publisher under
[PyPI account publishing settings](https://pypi.org/manage/account/publishing/).
For an existing project, use its **Manage → Publishing** page instead.
A pending publisher creates the project upon successful first upload; it does
not reserve the name beforehand. See PyPI's documentation for
[new projects](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
and [existing projects](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).

The workflow uses GitHub OIDC and grants `id-token: write` only to the publishing
job. No PyPI API-token secret is needed. The publishing action also generates
attestations for both distributions; see
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

## Verified preflight configuration

On November 8, 2026, authenticated inspection confirmed a **pending** PyPI
publisher with the exact binding above. The public project JSON returned 404;
this observation is not a reservation or proof that a first upload will succeed.
No package was uploaded.

At the owner's request, GitHub environment `pypi` was configured with required
reviewer `valkorobov`, self-review allowed, and one deployment policy accepting
only tags matching `v*`. A REST API readback confirmed these settings.
These deployment restrictions do not protect repository tags against creation,
movement, or deletion; the repository rulesets endpoint returned an empty list.
Exact release tag/version matching remains enforced by CI.

The [preflight record](release-preflight.json) separates owner decisions,
observed settings, historical CI, and unexecuted publication checks. Recheck
remote settings before the final release because they can change independently
of source control. Subsequent GitHub Actions runs are tracked in the project
status. [GitHub Actions verification](status.md#github-actions-verification)

## Publishing a release

The publishing job waits for all CI jobs and the strict `release-gates`
interlock. Its [inventory](release-gates.json) currently declares the remaining
checks as planned, so production publication is blocked. Complete the
[release gates plan](release-gates-plan.md) and generate actual candidate evidence
as described in the [evidence contract](release-gate-evidence.md).

1. Complete the release gates in [status.md](status.md#remaining-work-before-publication)
   and recheck the administrative preflight. The owner-selected MIT license is
   included in source and both candidate distributions.
2. Set `project.version` in `pyproject.toml`, update `uv.lock` with `uv lock`,
   and update the changelog and recorded verification. Commit and push changes.
3. Create a tag matching that version, for example `v0.1.0`, at the intended
   commit. Publish a GitHub Release for that tag.
4. Watch the `CI` run triggered by the `release` event. Once all gates pass,
   approve the `pypi` deployment as `valkorobov`; self-review is allowed.

Every release runs the Python 3.11–3.14 unit/coverage matrix, the 24 installed
[compatibility cells](ci-compatibility.md), quality and packaging checks, and
all three broker jobs. A mismatched tag or failed
required job blocks publication. Packaging builds the wheel from the sdist,
checks metadata and clean installations, then uploads the exact distributions
with `SHA256SUMS`. The gate job verifies that artifact and every required result,
then assembles `verified-release` with compatibility and gate evidence. The
publishing job downloads only that same-run verified bundle and checks its hashes
before uploading; it does not rebuild the package.

Both `release-distributions` and successful `verified-release` bundles, plus the
`release-gates-report`, are retained for seven days. Release runs
are queued for the same tag rather than cancelled mid-publication, and have
separate concurrency groups from ordinary push CI runs.

After success, verify the version on
[the project page](https://pypi.org/project/pytest-broker-observer/) and install it:

```shell
python -m pip install pytest-broker-observer==0.1.0
```

PyPI does not permit overwriting an uploaded distribution. If an upload partly
succeeds, inspect PyPI before retrying; the workflow deliberately does not skip
existing files. A code correction requires a new version and matching tag.
Prerelease tags must match their declared version too, such as `v0.2.0rc1`.

Local builds and smoke tests do not upload anything or prove the OIDC setup.
Record an actual GitHub/PyPI result in `docs/status.md` after the first release.
