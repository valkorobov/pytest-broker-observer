"""Checks for broker profile registration and the evidence handoff."""

from copy import deepcopy
from importlib import import_module, metadata
import json
from pathlib import Path
import re
import subprocess
import tomllib
from types import SimpleNamespace
from xml.etree import ElementTree

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT))
    return import_module("tools.run_broker_contracts")


def test_workflow_and_release_inventory_cover_each_broker_version_in_both_modes(runner):
    configured = runner.read_json(runner.PROFILE_FILE)["profiles"]
    assert set(configured) == {"kafka", "rabbitmq", "redis-streams"}
    assert all(len(profiles) == 2 for profiles in configured.values())
    ids = {profile["id"] for profiles in configured.values() for profile in profiles}
    assert len(ids) == 6

    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    broker_job = re.split(r"\n  [\w-]+:\n", workflow.split("\n  broker-contracts:\n", 1)[1], maxsplit=1)[0]
    selected = re.findall(r"^ {10}- ([\w.-]+)$", broker_job, re.MULTILINE)
    assert len(selected) == 6 and set(selected) == ids

    gates = runner.inventory_checks(runner.read_json(ROOT / "docs/release-gates.json"))
    registered = {
        profile_id: data for (check, profile_id), data in gates.items() if check == "broker-contracts"
    }
    assert set(registered) == {
        f"linux-{profile_id}-{mode}" for profile_id in ids for mode in ("async", "sync")
    }
    assert all(
        gate == "RG-03" and check["producer"] == "broker-contracts" and profile["required_cases"]
        for gate, check, profile in registered.values()
    )


@pytest.fixture
def contract_junit(runner, tmp_path):
    gates = runner.inventory_checks(runner.read_json(ROOT / "docs/release-gates.json"))
    profile = gates["broker-contracts", "linux-rabbitmq-4.3.7-async"][2]
    blocked = {rule["node_id"] for rule in profile["allowed_not_applicable"]}
    required = next(case for case in profile["required_cases"] if case not in blocked)
    rule = deepcopy(profile["allowed_not_applicable"][0])
    expected = {"required_cases": [required, rule["node_id"]], "allowed_not_applicable": [rule]}
    suite = ElementTree.Element("testsuite")
    for node in expected["required_cases"]:
        classname, name = node.split("::", 1)
        ElementTree.SubElement(suite, "testcase", classname=classname, name=name)
    properties = ElementTree.SubElement(suite[1], "properties")
    ElementTree.SubElement(properties, "property", name="contract_precondition", value=rule["precondition"])
    ElementTree.SubElement(properties, "property", name="contract_applicability", value="NOT_APPLICABLE")
    ElementTree.SubElement(suite[1], "skipped", type="pytest.skip", message=rule["reason"])
    return expected, suite, tmp_path / "broker.xml"


def test_registered_capability_is_the_only_accepted_not_applicable_result(runner, contract_junit):
    expected, suite, report = contract_junit
    ElementTree.ElementTree(suite).write(report, encoding="utf-8")
    assert runner.contract_report(report, expected) == {"tests": 2, "passed": 1, "not_applicable": 1}


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ("extra-case", "differ from the registered contract suite"),
        ("unexpected-skip", "Unexpected mandatory skip"),
        ("changed-applicability", "registered capability expectation changed"),
    ],
)
def test_broker_run_fails_when_actual_cases_or_capabilities_change(runner, contract_junit, change, error):
    expected, suite, report = contract_junit
    if change == "extra-case":
        ElementTree.SubElement(suite, "testcase", classname="unregistered", name="test_extra")
    elif change == "unexpected-skip":
        ElementTree.SubElement(suite[0], "skipped", message="broker configuration missing")
    else:
        suite[1].remove(suite[1].find("skipped"))
    ElementTree.ElementTree(suite).write(report, encoding="utf-8")
    with pytest.raises(runner.GateError, match=error):
        runner.contract_report(report, expected)


def test_candidate_requires_the_installed_wheel_to_match_the_verified_archive(runner, tmp_path, monkeypatch):
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    version = project["version"]
    dist = tmp_path / "dist"
    dist.mkdir()
    wheel = dist / f"pytest_broker_observer-{version}-py3-none-any.whl"
    sdist = dist / f"pytest_broker_observer-{version}.tar.gz"
    wheel.write_bytes(b"synthetic wheel")
    sdist.write_bytes(b"synthetic sdist")
    hashes = {path.name: runner.digest(path) for path in (wheel, sdist)}
    (tmp_path / "SHA256SUMS").write_text(
        "".join(f"{sha}  dist/{name}\n" for name, sha in hashes.items()), encoding="utf-8"
    )

    target = tmp_path / "target"
    site = target / "site-packages"
    info = site / f"pytest_broker_observer-{version}.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text(f"Name: pytest-broker-observer\nVersion: {version}\n", encoding="utf-8")
    (site / "event_observer").mkdir()
    (site / "event_observer/__init__.py").touch()
    origin = info / "direct_url.json"
    installed = metadata.PathDistribution(info)
    monkeypatch.setattr(runner, "sys", SimpleNamespace(prefix=str(target)))
    monkeypatch.setattr(runner.importlib.metadata, "distribution", lambda name: installed)

    origin.write_text(
        json.dumps({"url": wheel.as_uri(), "archive_info": {"hashes": {"sha256": hashes[wheel.name]}}}),
        encoding="utf-8",
    )
    assert runner.checked_candidate(tmp_path) == (version, hashes)

    origin.write_text(
        json.dumps({"url": wheel.as_uri(), "archive_info": {"hashes": {"sha256": "0" * 64}}}),
        encoding="utf-8",
    )
    with pytest.raises(runner.GateError, match="Installed wheel hash mismatch"):
        runner.checked_candidate(tmp_path)


@pytest.mark.parametrize("oversized", [False, True])
def test_raw_broker_junit_is_never_retained_as_a_ci_artifact(runner, tmp_path, monkeypatch, oversized):
    environment = {"RABBITMQ_URL": "amqp://synthetic-reader:synthetic-password@127.0.0.1:5672/"}
    credentials = b"synthetic-reader:synthetic-password"

    def run(command, **kwargs):
        path = Path(command[command.index("--junitxml") + 1])
        assert not path.is_relative_to(tmp_path)
        if oversized:
            with path.open("wb") as stream:
                stream.write(credentials)
                stream.truncate(16 * 1024 * 1024 + 1)
        else:
            path.write_bytes(b"<testsuite>" + credentials + b"</testsuite>")
        return subprocess.CompletedProcess(command, 0, stdout=credentials, stderr=b"")

    monkeypatch.setattr(runner.subprocess, "run", run)
    if oversized:
        with pytest.raises(runner.GateError, match="JUnit exceeded"):
            runner.run_pytest([], environment, tmp_path, "async")
        assert not (tmp_path / "async.xml").exists()
    else:
        report = runner.run_pytest([], environment, tmp_path, "async")
        assert report.read_bytes() == b"<testsuite>[redacted]:[redacted]</testsuite>"
    assert (tmp_path / "async.log").read_bytes() == b"[redacted]:[redacted]"
