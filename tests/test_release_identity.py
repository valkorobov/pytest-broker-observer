"""Release identity is derived from the actual source and candidate archives."""

import base64
from copy import deepcopy
import csv
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import warnings
from zipfile import ZipFile, ZipInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "tools/check_release_identity.py"
DIST_NAME = "pytest_broker_observer-0.1.0"
WHEEL_NAME = f"{DIST_NAME}-py3-none-any.whl"
SDIST_NAME = f"{DIST_NAME}.tar.gz"
METADATA = """Metadata-Version: 2.4
Name: pytest-broker-observer
Version: 0.1.0
Summary: Synthetic release identity fixture
Author: Valerii Korobov
Project-URL: Repository, https://example.test/observer
License-Expression: MIT
License-File: LICENSE
Requires-Python: >=3.11,<3.15
Provides-Extra: pytest
Requires-Dist: pytest<10,>=8.3; extra == 'pytest'

Synthetic public fixture.
"""
PROJECT = """[project]
name = "pytest-broker-observer"
version = "0.1.0"
license = "MIT"
license-files = ["LICENSE"]
authors = [{name = "Valerii Korobov"}]
requires-python = ">=3.11,<3.15"
dependencies = []

[project.urls]
Repository = "https://example.test/observer"

[project.optional-dependencies]
pytest = ["pytest>=8.3,<10"]
"""


@pytest.fixture
def candidate(tmp_path):
    license_text = (ROOT / "LICENSE").read_bytes()
    source = tmp_path / "source"
    source.mkdir()
    project = source / "pyproject.toml"
    project.write_text(PROJECT)
    license_file = source / "LICENSE"
    license_file.write_bytes(license_text)
    distributions = tmp_path / "dist"
    distributions.mkdir()
    info = f"{DIST_NAME}.dist-info"
    wheel_entries = [
        ("event_observer/__init__.py", b'__version__ = "0.1.0"\n'),
        ("event_observer/py.typed", b""),
        (f"{info}/METADATA", METADATA.encode()),
        (f"{info}/WHEEL", b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n"),
        (f"{info}/licenses/LICENSE", license_text),
    ]
    sdist_entries = [
        (f"{DIST_NAME}/LICENSE", license_text),
        (f"{DIST_NAME}/PKG-INFO", METADATA.encode()),
        (f"{DIST_NAME}/pyproject.toml", PROJECT.encode()),
        (f"{DIST_NAME}/src/event_observer/__init__.py", b'__version__ = "0.1.0"\n'),
        (f"{DIST_NAME}/src/event_observer/py.typed", b""),
    ]
    report = tmp_path / "report.json"

    def build():
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Duplicate name:.*", category=UserWarning)
            with ZipFile(distributions / WHEEL_NAME, "w") as archive:
                rows = []
                for name, content in wheel_entries:
                    archive.writestr(name, content)
                    encoded = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=").decode()
                    path = name.filename if isinstance(name, ZipInfo) else name
                    rows.append((path, "sha256=" + encoded, len(content)))
                record = io.StringIO()
                writer = csv.writer(record)
                writer.writerows(rows)
                writer.writerow((f"{info}/RECORD", "", ""))
                archive.writestr(f"{info}/RECORD", record.getvalue())
        with tarfile.open(distributions / SDIST_NAME, "w:gz") as archive:
            for name, content in sdist_entries:
                entry = name if isinstance(name, tarfile.TarInfo) else tarfile.TarInfo(name)
                if entry.isfile():
                    entry.size = len(content)
                    archive.addfile(entry, io.BytesIO(content))
                else:
                    archive.addfile(entry)

    def replace(entries, suffix, replacement):
        index = next(index for index, (name, _) in enumerate(entries) if str(name).endswith(suffix))
        name, content = entries[index]
        entries[index] = (name, replacement(content))

    def run():
        return subprocess.run(
            [
                sys.executable,
                "-I",
                str(CHECKER),
                "--project",
                str(project),
                "--license",
                str(license_file),
                "--dist-dir",
                str(distributions),
                "--report",
                str(report),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

    build()
    return {
        "project": project,
        "license": license_file,
        "dist": distributions,
        "wheel": wheel_entries,
        "sdist": sdist_entries,
        "report": report,
        "build": build,
        "replace": replace,
        "run": run,
    }


def assert_rejected(candidate):
    result = candidate["run"]()
    assert result.returncode == 1, result.stdout + result.stderr
    report = json.loads(candidate["report"].read_text())
    assert report["ready"] is False
    assert report["errors"]
    assert "SECRET_IDENTITY" not in result.stdout + result.stderr
    assert "SECRET_IDENTITY" not in candidate["report"].read_text()


def test_consistent_source_and_both_archives_have_verified_identity_and_hashes(candidate):
    result = candidate["run"]()
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(candidate["report"].read_text())
    assert report["ready"] is True and report["errors"] == []
    assert report["distribution"] == "pytest-broker-observer"
    assert report["version"] == "0.1.0"
    assert report["artifacts"] == {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in candidate["dist"].iterdir()
    }


@pytest.mark.parametrize(
    "original,replacement",
    [
        ('name = "pytest-broker-observer"', 'name = "SECRET_IDENTITY"'),
        ('license = "MIT"', 'license = "Apache-2.0"'),
        ('license-files = ["LICENSE"]', "license-files = []"),
        ('name = "Valerii Korobov"', 'name = "Another Owner"'),
        ('requires-python = ">=3.11,<3.15"\n', ""),
        ('requires-python = ">=3.11,<3.15"', 'requires-python = ">=3.12,<3.15"'),
        ("dependencies = []", 'dependencies = ["pytest>=8"]'),
    ],
)
def test_source_identity_cannot_drift_from_owner_approved_release(candidate, original, replacement):
    candidate["project"].write_text(PROJECT.replace(original, replacement))
    assert_rejected(candidate)


@pytest.mark.parametrize("change", ["missing", "holder", "permission", "disclaimer"])
def test_source_license_must_preserve_owner_and_complete_mit_terms(candidate, change):
    license_file = candidate["license"]
    text = license_file.read_text()
    if change == "missing":
        license_file.unlink()
    elif change == "holder":
        license_file.write_text(text.replace("Valerii Korobov", "Another Owner"))
    elif change == "permission":
        license_file.write_text(text.replace("Permission is hereby granted", "Permission is withheld"))
    else:
        license_file.write_text(text.replace("WITHOUT WARRANTY", "WITH WARRANTY"))
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
@pytest.mark.parametrize(
    "original,replacement",
    [
        (b"Name: pytest-broker-observer", b"Name: SECRET_IDENTITY"),
        (b"Version: 0.1.0", b"Version: 0.2.0"),
        (b"Author: Valerii Korobov", b"Author: Another Owner"),
        (b"Author: Valerii Korobov", b"Author: Valerii Korobov\nAuthor: Valerii Korobov"),
        (
            b"Project-URL: Repository, https://example.test/observer",
            b"Project-URL: Repository, https://SECRET_IDENTITY.test",
        ),
        (b"Project-URL: Repository, https://example.test/observer", b""),
        (b"License-Expression: MIT", b"License-Expression: Apache-2.0"),
        (b"License-File: LICENSE", b"License-File: missing.txt"),
        (b"License-Expression: MIT", b"License-Expression: MIT\nLicense-Expression: MIT"),
        (b"License-File: LICENSE", b"License-File: LICENSE\nLicense-File: LICENSE"),
        (b"Provides-Extra: pytest", b"Requires-Dist: mandatory-secret-dependency>=1\nProvides-Extra: pytest"),
    ],
)
def test_archive_metadata_must_match_version_license_and_optional_dependency_boundaries(
    candidate, archive, original, replacement
):
    suffix = "/METADATA" if archive == "wheel" else "/PKG-INFO"
    candidate["replace"](candidate[archive], suffix, lambda content: content.replace(original, replacement))
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
@pytest.mark.parametrize(
    "requirement",
    [
        b"Requires-Dist: mandatory-dependency>=1; python_version >= '3.11'",
        b"Requires-Dist: mandatory-dependency>=1; extra == 'pytest' or python_version >= '3.11'",
    ],
)
def test_dependency_markers_cannot_make_an_apparent_extra_install_into_core(candidate, archive, requirement):
    suffix = "/METADATA" if archive == "wheel" else "/PKG-INFO"
    candidate["replace"](
        candidate[archive],
        suffix,
        lambda content: content.replace(b"Provides-Extra: pytest", requirement + b"\nProvides-Extra: pytest"),
    )
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
@pytest.mark.parametrize(
    "change",
    ["substituted-package", "changed-version-range", "missing-requirement", "duplicate-requirement"],
)
def test_declared_optional_dependency_contents_cannot_drift_in_built_metadata(candidate, archive, change):
    original = b"Requires-Dist: pytest<10,>=8.3; extra == 'pytest'\n"
    replacements = {
        "substituted-package": b"Requires-Dist: another-package<10,>=8.3; extra == 'pytest'\n",
        "changed-version-range": b"Requires-Dist: pytest>=8; extra == 'pytest'\n",
        "missing-requirement": b"",
        "duplicate-requirement": original + original,
    }
    suffix = "/METADATA" if archive == "wheel" else "/PKG-INFO"
    candidate["replace"](
        candidate[archive], suffix, lambda content: content.replace(original, replacements[change])
    )
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
@pytest.mark.parametrize("change", ["missing", "duplicated", "changed-range"])
def test_archive_python_support_must_match_the_explicit_source_range(candidate, archive, change):
    original = b"Requires-Python: >=3.11,<3.15\n"
    replacements = {
        "missing": b"",
        "duplicated": original + original,
        "changed-range": b"Requires-Python: >=3.12,<3.15\n",
    }
    suffix = "/METADATA" if archive == "wheel" else "/PKG-INFO"
    candidate["replace"](
        candidate[archive], suffix, lambda content: content.replace(original, replacements[change])
    )
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("content", [b"*", b"*\n"])
def test_uv_housekeeping_ignore_file_does_not_add_a_release_artifact(candidate, content):
    (candidate["dist"] / ".gitignore").write_bytes(content)
    result = candidate["run"]()
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(candidate["report"].read_text())
    assert report["ready"] is True
    assert set(report["artifacts"]) == {WHEEL_NAME, SDIST_NAME}


def test_unknown_hidden_distribution_file_cannot_hide_as_uv_housekeeping(candidate):
    (candidate["dist"] / ".gitignore").write_bytes(b"SECRET_IDENTITY")
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
def test_archived_license_cannot_differ_from_verified_source(candidate, archive):
    candidate["replace"](
        candidate[archive], "/LICENSE", lambda content: content.replace(b"Valerii Korobov", b"Another Owner")
    )
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
def test_each_artifact_requires_its_typing_marker(candidate, archive):
    candidate[archive][:] = [
        (name, data) for name, data in candidate[archive] if not str(name).endswith("/py.typed")
    ]
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
def test_duplicate_archive_entries_are_not_accepted_as_one_license(candidate, archive):
    duplicate = next(entry for entry in candidate[archive] if str(entry[0]).endswith("/LICENSE"))
    candidate[archive].append(deepcopy(duplicate))
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
def test_archive_paths_cannot_escape_the_distribution_root(candidate, archive):
    candidate[archive].append(("../SECRET_IDENTITY", b"untrusted data"))
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("archive", ["wheel", "sdist"])
def test_linked_archive_members_cannot_replace_the_verified_license(candidate, archive):
    entries = candidate[archive]
    index = next(index for index, (name, _) in enumerate(entries) if str(name).endswith("/LICENSE"))
    original_name = entries[index][0]
    if archive == "wheel":
        entry = ZipInfo(original_name)
        entry.create_system = 3
        entry.external_attr = 0o120777 << 16
        entries[index] = (entry, b"../../SECRET_IDENTITY")
    else:
        entry = tarfile.TarInfo(original_name)
        entry.type = tarfile.SYMTYPE
        entry.linkname = "../../SECRET_IDENTITY"
        entries[index] = (entry, b"")
    candidate["build"]()
    assert_rejected(candidate)


def test_sdist_pyproject_metadata_must_match_the_source_project(candidate):
    candidate["replace"](
        candidate["sdist"],
        "/pyproject.toml",
        lambda content: content.replace(b'license = "MIT"', b'license = "Apache-2.0"'),
    )
    candidate["build"]()
    assert_rejected(candidate)


@pytest.mark.parametrize("change", ["missing-wheel", "extra-artifact", "wrong-version-filename"])
def test_only_the_expected_candidate_distributions_can_be_verified(candidate, change):
    wheel = candidate["dist"] / WHEEL_NAME
    if change == "missing-wheel":
        wheel.unlink()
    elif change == "extra-artifact":
        (candidate["dist"] / "unexpected.whl").write_bytes(b"not verified")
    else:
        wheel.rename(candidate["dist"] / WHEEL_NAME.replace("0.1.0", "0.2.0"))
    assert_rejected(candidate)
