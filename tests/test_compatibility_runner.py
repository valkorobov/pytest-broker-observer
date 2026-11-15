"""Compatibility evidence cannot silently accept altered or incomplete execution."""

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest


@pytest.fixture
def runner(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(root / "tools"))
    spec = importlib.util.spec_from_file_location("compatibility_runner", root / "tools/run_compatibility.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def artifacts(tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    for name in ("pytest_broker_observer-0.1.0-py3-none-any.whl", "pytest_broker_observer-0.1.0.tar.gz"):
        (dist / name).write_bytes(b"synthetic distribution for checksum validation")
    manifest = tmp_path / "SHA256SUMS"
    manifest.write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  dist/{path.name}\n"
            for path in sorted(dist.iterdir())
        )
    )
    return tmp_path


def test_only_the_exact_verified_distribution_pair_can_be_used(runner, artifacts):
    paths, hashes = runner.verify_artifacts(artifacts)
    assert len(paths) == len(hashes) == 2


@pytest.mark.parametrize("change", ["tampered", "missing", "extra", "duplicate", "traversal", "hidden"])
def test_modified_or_ambiguous_candidate_files_are_rejected(runner, artifacts, change):
    path = next((artifacts / "dist").glob("*.whl"))
    manifest = artifacts / "SHA256SUMS"
    if change == "tampered":
        path.write_bytes(b"modified")
    elif change == "missing":
        path.unlink()
    elif change == "extra":
        (artifacts / "dist" / "unexpected.whl").write_bytes(b"extra")
    elif change == "duplicate":
        manifest.write_text(manifest.read_text() + manifest.read_text().splitlines()[0] + "\n")
    elif change == "hidden":
        (artifacts / "dist" / ".gitignore").write_bytes(b"unverified hidden content")
    else:
        manifest.write_text(manifest.read_text().replace("dist/", "../dist/"))
    with pytest.raises(ValueError):
        runner.verify_artifacts(artifacts)


def test_uv_housekeeping_file_does_not_change_the_candidate_hashes(runner, artifacts):
    (artifacts / "dist" / ".gitignore").write_bytes(b"*\n")
    assert len(runner.verify_artifacts(artifacts)[0]) == 2


@pytest.mark.parametrize("result", ["failure", "error", "skipped", "rerunFailure", "flakyFailure"])
def test_mandatory_junit_rejects_failed_skipped_or_retried_cases(runner, tmp_path, result):
    path = tmp_path / "results.xml"
    path.write_text(f'<testsuite><testcase classname="case" name="a"><{result}/></testcase></testsuite>')
    with pytest.raises(ValueError):
        runner.require_complete_junit(path)


@pytest.mark.parametrize(
    "body",
    [
        "<error/>",
        "<failure/>",
        "<skipped/>",
        '<testcase classname="case" name="a"/>',
    ],
)
def test_green_case_cannot_hide_a_suite_error_or_duplicate_identity(runner, tmp_path, body):
    path = tmp_path / "results.xml"
    path.write_text(f'<testsuite><testcase classname="case" name="a"/>{body}</testsuite>')
    with pytest.raises(ValueError):
        runner.require_complete_junit(path)


@pytest.mark.parametrize("attribute", ['errors="1"', 'failures="1"', 'skipped="1"', 'tests="2"'])
def test_inconsistent_suite_counters_are_rejected(runner, tmp_path, attribute):
    path = tmp_path / "results.xml"
    path.write_text(f'<testsuite {attribute}><testcase classname="case" name="a"/></testsuite>')
    with pytest.raises(ValueError):
        runner.require_complete_junit(path)


def test_nonempty_complete_junit_is_accepted(runner, tmp_path):
    path = tmp_path / "results.xml"
    path.write_text(
        '<testsuites tests="1"><testsuite tests="1" errors="0" failures="0" skipped="0">'
        '<testcase classname="case" name="a"/></testsuite></testsuites>'
    )
    assert runner.require_complete_junit(path) == 1


def test_standalone_pytest_cases_can_use_file_identity_without_classname(runner, tmp_path):
    path = tmp_path / "results.xml"
    path.write_text(
        '<testsuite tests="2"><testcase classname="" file="test_copied.py" name="a"/>'
        '<testcase classname="" file="test_copied.py" name="b"/></testsuite>'
    )
    assert runner.require_complete_junit(path) == 2


def test_first_failed_command_keeps_its_output_without_retry(runner, tmp_path):
    cell = runner.Cell({}, tmp_path, tmp_path, "uv", "pyright")
    with pytest.raises(ValueError):
        cell.run("first-failure", [sys.executable, "-c", "print('original failure'); raise SystemExit(7)"])
    report = json.loads((tmp_path / "manifest.json").read_text())
    assert report["successful"] is False
    assert len(report["commands"]) == 1
    assert report["commands"][0]["returncode"] == 7
    assert (tmp_path / "first-failure.log").read_text() == "original failure\n"


def test_timed_out_command_preserves_its_partial_output(runner, tmp_path):
    cell = runner.Cell({}, tmp_path, tmp_path, "uv", "pyright")
    with pytest.raises(ValueError):
        cell.run(
            "timeout",
            [sys.executable, "-c", "import time; print('started', flush=True); time.sleep(30)"],
            timeout=1,
        )
    report = json.loads((tmp_path / "manifest.json").read_text())
    assert report["commands"][0]["timed_out"] is True
    assert (tmp_path / "timeout.log").read_text() == "started\n"
