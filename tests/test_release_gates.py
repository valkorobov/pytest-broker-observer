"""Release publication requires complete, authenticatable candidate evidence.

Passing fixtures below are synthetic schema/validator exercises. They never
mark the production inventory or a real release candidate as verified.
"""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
from xml.etree import ElementTree

import pytest

ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "tools/check_release_gates.py"
INVENTORY = ROOT / "docs/release-gates.json"
DISTRIBUTIONS = (
    "pytest_broker_observer-0.1.0-py3-none-any.whl",
    "pytest_broker_observer-0.1.0.tar.gz",
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


@pytest.fixture
def release(tmp_path):
    candidate = {
        "repository": "owner/repository",
        "sha": "a" * 40,
        "run_id": "123",
        "run_attempt": "1",
        "version": "0.1.0",
        "tag": "v0.1.0",
    }
    inventory = deepcopy(json.loads(INVENTORY.read_text()))
    inventory_path = tmp_path / "inventory.json"
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    for name in DISTRIBUTIONS:
        (dist_dir / name).write_bytes(f"synthetic validator fixture: {name}".encode())
    hashes = {name: digest(dist_dir / name) for name in DISTRIBUTIONS}
    (tmp_path / "SHA256SUMS").write_text("".join(f"{value}  dist/{name}\n" for name, value in hashes.items()))
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\nname = "pytest-broker-observer"\nversion = "0.1.0"\n')
    jobs = {}
    documents = {}
    bundles = {}
    for gate in inventory["gates"]:
        gate["checks"] = gate["checks"][:1]
        check = gate["checks"][0]
        case_id = f"release.{check['id']}::required"
        check["implementation"] = "ready"
        check["profiles"] = [
            {"id": "synthetic-profile", "required_cases": [case_id], "allowed_not_applicable": []}
        ]
        bundle = evidence_dir / f"release-gate-evidence-{check['producer']}"
        bundle.mkdir()
        classname, name = case_id.split("::")
        suite = ElementTree.Element("testsuite", tests="1", failures="0", errors="0", skipped="0")
        ElementTree.SubElement(suite, "testcase", classname=classname, name=name)
        ElementTree.ElementTree(suite).write(bundle / "results.xml", encoding="utf-8", xml_declaration=True)
        documents[gate["id"]] = {
            "schema_version": 1,
            "candidate": deepcopy(candidate),
            "producer": check["producer"],
            "checks": [
                {
                    "id": check["id"],
                    "profile": "synthetic-profile",
                    "environment": {"python": "3.14.7", "platform": "Linux", "versions": {"pytest": "9.2.0"}},
                    "junit": {"path": "results.xml", "sha256": digest(bundle / "results.xml")},
                    "artifacts": [],
                    "distributions": dict(hashes),
                }
            ],
        }
        bundles[gate["id"]] = bundle
        jobs[check["producer"]] = {"result": "success"}
    jobs_path = tmp_path / "jobs.json"
    report_path = tmp_path / "report.json"
    bundle_dir = tmp_path / "approved"

    def save():
        write_json(inventory_path, inventory)
        write_json(jobs_path, jobs)
        for gate_id, document in documents.items():
            write_json(bundles[gate_id] / "evidence.json", document)

    def update_junit(gate_id, change):
        path = bundles[gate_id] / "results.xml"
        tree = ElementTree.parse(path)
        change(tree.getroot())
        tree.write(path, encoding="utf-8", xml_declaration=True)
        documents[gate_id]["checks"][0]["junit"]["sha256"] = digest(path)

    def run(*, mode=None, inventory_override=None):
        args = [
            sys.executable,
            str(VALIDATOR),
            "--inventory",
            str(inventory_override or inventory_path),
            "--evidence-dir",
            str(evidence_dir),
            "--dist-dir",
            str(dist_dir),
            "--job-results",
            str(jobs_path),
            "--project",
            str(project),
            "--repository",
            candidate["repository"],
            "--sha",
            candidate["sha"],
            "--run-id",
            candidate["run_id"],
            "--run-attempt",
            candidate["run_attempt"],
            "--tag",
            candidate["tag"],
            "--report",
            str(report_path),
            "--bundle-dir",
            str(bundle_dir),
        ]
        if mode is not None:
            args.extend(["--mode", mode])
        return subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=15)

    save()
    return SimpleNamespace(
        candidate=candidate,
        inventory=inventory,
        inventory_path=inventory_path,
        evidence_dir=evidence_dir,
        dist_dir=dist_dir,
        hashes=hashes,
        project=project,
        jobs=jobs,
        jobs_path=jobs_path,
        documents=documents,
        bundles=bundles,
        report_path=report_path,
        bundle_dir=bundle_dir,
        save=save,
        update_junit=update_junit,
        run=run,
    )


def assert_blocked(release, result):
    assert result.returncode == 1, result.stdout + result.stderr
    report = json.loads(release.report_path.read_text())
    assert report["ready"] is False
    assert report["errors"]
    assert not release.bundle_dir.exists()
    return report


def test_complete_synthetic_evidence_produces_the_exact_verified_bundle(release):
    supporting = release.bundles["RG-01"] / "observed-versions.json"
    supporting.write_text('{"pytest": "9.2.0"}\n')
    release.documents["RG-01"]["checks"][0]["artifacts"] = [
        {"path": supporting.name, "sha256": digest(supporting)}
    ]
    release.save()
    result = release.run()
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(release.report_path.read_text())
    assert report["ready"] is True
    assert report["errors"] == []
    assert report["candidate"] == release.candidate
    assert report["distributions"] == release.hashes
    for name, expected in release.hashes.items():
        assert digest(release.bundle_dir / "dist" / name) == expected
    assert json.loads((release.bundle_dir / "release-gates.json").read_text())["ready"] is True
    compatibility = json.loads((release.bundle_dir / "compatibility.json").read_text())
    assert compatibility
    manifest = (release.bundle_dir / "SHA256SUMS").read_text().splitlines()
    for line in manifest:
        expected, filename = line.split(maxsplit=1)
        assert digest(release.bundle_dir / filename) == expected
    assert any("release-gates.json" in line for line in manifest)
    assert any("compatibility.json" in line for line in manifest)
    manifest_paths = [line.split(maxsplit=1)[1] for line in manifest]
    for source in [*release.bundles.values()]:
        for original in source.iterdir():
            matches = [
                release.bundle_dir / filename
                for filename in manifest_paths
                if Path(filename).name == original.name
                and digest(release.bundle_dir / filename) == digest(original)
            ]
            assert matches, f"Declared raw evidence was not retained: {original.name}"


def test_one_downloaded_artifact_may_have_its_evidence_at_the_input_root(release):
    combined = {
        "schema_version": 1,
        "candidate": deepcopy(release.candidate),
        "producer": "shared-job",
        "checks": [],
    }
    raw_files = {}
    for gate in release.inventory["gates"]:
        gate["checks"][0]["producer"] = "shared-job"
        evidence = deepcopy(release.documents[gate["id"]]["checks"][0])
        filename = f"results-{gate['id']}.xml"
        raw_files[filename] = (release.bundles[gate["id"]] / "results.xml").read_bytes()
        evidence["junit"]["path"] = filename
        combined["checks"].append(evidence)
    shutil.rmtree(release.evidence_dir)
    release.evidence_dir.mkdir()
    for filename, content in raw_files.items():
        (release.evidence_dir / filename).write_bytes(content)
    write_json(release.evidence_dir / "evidence.json", combined)
    write_json(release.inventory_path, release.inventory)
    write_json(release.jobs_path, {"shared-job": {"result": "success"}})
    result = release.run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(release.report_path.read_text())["ready"] is True


@pytest.mark.parametrize("gate_id", [f"RG-{index:02}" for index in range(1, 16)])
def test_every_mandatory_gate_requires_its_own_evidence(release, gate_id):
    del release.documents[gate_id]
    shutil.rmtree(release.bundles[gate_id])
    release.save()
    assert_blocked(release, release.run())


def test_the_real_unimplemented_inventory_cannot_be_closed_by_synthetic_results(release):
    assert_blocked(release, release.run(inventory_override=INVENTORY))


@pytest.mark.parametrize("change", ["missing-gate", "duplicate-gate", "empty-required-cases"])
def test_changing_the_requirements_catalogue_cannot_silently_drop_obligations(release, change):
    if change == "missing-gate":
        release.inventory["gates"].pop()
    elif change == "duplicate-gate":
        release.inventory["gates"][-1] = deepcopy(release.inventory["gates"][0])
    else:
        release.inventory["gates"][0]["checks"][0]["profiles"][0]["required_cases"] = []
    release.save()
    assert_blocked(release, release.run())


def test_audit_reports_open_inventory_without_creating_a_publishable_bundle(release):
    for bundle in release.bundles.values():
        shutil.rmtree(bundle)
    result = release.run(mode="audit", inventory_override=INVENTORY)
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(release.report_path.read_text())
    assert report["ready"] is False
    assert report["errors"]
    assert not release.bundle_dir.exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("repository", "someone/else"),
        ("sha", "b" * 40),
        ("run_id", "456"),
        ("run_attempt", "2"),
        ("version", "0.2.0"),
        ("tag", "v0.2.0"),
    ],
)
def test_evidence_from_another_candidate_or_attempt_is_rejected(release, field, value):
    release.documents["RG-01"]["candidate"][field] = value
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("field", ["profile", "id"])
def test_unregistered_check_or_profile_cannot_supply_a_required_result(release, field):
    release.documents["RG-01"]["checks"][0][field] = "unexpected-profile"
    release.save()
    assert_blocked(release, release.run())


def test_another_producer_cannot_claim_a_registered_check(release):
    release.documents["RG-01"]["producer"] = "another-job"
    release.save()
    assert_blocked(release, release.run())


def test_repeated_profile_evidence_cannot_replace_another_required_profile(release):
    release.documents["RG-01"]["checks"].append(deepcopy(release.documents["RG-01"]["checks"][0]))
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize(
    "field,value",
    [("python", "3.14"), ("platform", "unknown"), ("versions", {"pytest": "latest"}), ("versions", {})],
)
def test_environment_metadata_requires_concrete_observed_runtime_and_tool_versions(release, field, value):
    release.documents["RG-01"]["checks"][0]["environment"][field] = value
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("profile_id", ["linux-cpython-3.11", "windows-cpython-3.14"])
def test_registered_profile_metadata_must_match_its_python_and_platform(release, profile_id):
    release.inventory["gates"][0]["checks"][0]["profiles"][0]["id"] = profile_id
    release.documents["RG-01"]["checks"][0]["profile"] = profile_id
    release.save()
    assert_blocked(release, release.run())


def test_concrete_runtime_metadata_satisfies_its_matching_registered_profile(release):
    profile_id = "linux-cpython-3.14"
    release.inventory["gates"][0]["checks"][0]["profiles"][0]["id"] = profile_id
    release.documents["RG-01"]["checks"][0]["profile"] = profile_id
    release.save()
    result = release.run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(release.report_path.read_text())["ready"] is True


@pytest.mark.parametrize("outcome", ["failure", "cancelled", "skipped", None])
def test_test_report_cannot_override_the_actual_ci_job_outcome(release, outcome):
    producer = release.documents["RG-01"]["producer"]
    if outcome is None:
        del release.jobs[producer]
    else:
        release.jobs[producer]["result"] = outcome
    release.save()
    assert_blocked(release, release.run())


def test_a_modified_distribution_invalidates_tested_artifact_identity(release):
    (release.dist_dir / DISTRIBUTIONS[0]).write_bytes(b"modified after verification")
    assert_blocked(release, release.run())


def test_a_missing_distribution_is_not_a_complete_release(release):
    (release.dist_dir / DISTRIBUTIONS[1]).unlink()
    assert_blocked(release, release.run())


def test_an_unexpected_distribution_cannot_be_uploaded_with_the_approved_pair(release):
    (release.dist_dir / "another_project-0.1.0-py3-none-any.whl").write_bytes(b"unregistered")
    assert_blocked(release, release.run())


def test_a_false_tested_distribution_digest_is_rejected(release):
    release.documents["RG-01"]["checks"][0]["distributions"][DISTRIBUTIONS[0]] = "0" * 64
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("change", ["missing", "tampered", "false-digest"])
def test_junit_evidence_must_exist_and_match_its_declared_digest(release, change):
    report = release.bundles["RG-01"] / "results.xml"
    if change == "missing":
        report.unlink()
    elif change == "tampered":
        report.write_text("<testsuite />")
    else:
        release.documents["RG-01"]["checks"][0]["junit"]["sha256"] = "0" * 64
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("kind", ["failure", "error"])
def test_a_failed_junit_case_blocks_even_if_summary_counters_claim_success(release, kind):
    def fail(suite):
        ElementTree.SubElement(suite.find("testcase"), kind, message="synthetic failure")

    release.update_junit("RG-01", fail)
    release.save()
    assert_blocked(release, release.run())


def test_a_required_case_missing_from_a_passing_junit_report_is_rejected(release):
    release.inventory["gates"][0]["checks"][0]["profiles"][0]["required_cases"].append(
        "release.required::missing"
    )
    release.save()
    assert_blocked(release, release.run())


def test_duplicate_test_identities_cannot_satisfy_the_required_inventory(release):
    def duplicate(suite):
        suite.append(deepcopy(suite.find("testcase")))
        suite.set("tests", "2")

    release.update_junit("RG-01", duplicate)
    release.save()
    assert_blocked(release, release.run())


def test_junit_counter_mismatch_is_not_accepted_as_complete_execution(release):
    release.update_junit("RG-01", lambda suite: suite.set("tests", "100"))
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("element", ["flakyFailure", "flakyError", "rerunFailure", "rerunError"])
def test_a_successful_rerun_does_not_erase_the_first_mandatory_failure(release, element):
    release.update_junit(
        "RG-01",
        lambda suite: ElementTree.SubElement(suite.find("testcase"), element, message="initial failure"),
    )
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("status", ["notrun", "unknown"])
def test_unexecuted_or_unknown_case_status_is_not_a_passing_result(release, status):
    release.update_junit("RG-01", lambda suite: suite.find("testcase").set("status", status))
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("skip_type", [None, "pytest.xfail"])
def test_unexpected_skips_and_xfails_are_not_passing_gate_evidence(release, skip_type):
    def skip(suite):
        suite.set("skipped", "1")
        node = ElementTree.SubElement(suite.find("testcase"), "skipped", message="missing infrastructure")
        if skip_type:
            node.set("type", skip_type)

    release.update_junit("RG-01", skip)
    release.save()
    assert_blocked(release, release.run())


def allow_not_applicable(release, *, reason="NOT_APPLICABLE: synthetic unsupported capability"):
    profile = release.inventory["gates"][0]["checks"][0]["profiles"][0]
    classname = profile["required_cases"][0].split("::")[0]
    optional_id = f"{classname}::optional"
    profile["required_cases"].append(optional_id)
    profile["allowed_not_applicable"] = [
        {
            "node_id": optional_id,
            "reason": reason,
            "precondition": "supports_null_payload=True",
        }
    ]

    def skip(suite):
        suite.set("tests", "2")
        suite.set("skipped", "1")
        case = ElementTree.SubElement(suite, "testcase", classname=classname, name="optional")
        properties = ElementTree.SubElement(case, "properties")
        ElementTree.SubElement(
            properties, "property", name="contract_precondition", value="supports_null_payload=True"
        )
        ElementTree.SubElement(properties, "property", name="contract_applicability", value="NOT_APPLICABLE")
        ElementTree.SubElement(case, "skipped", message=reason)

    release.update_junit("RG-01", skip)


def test_declared_capability_precondition_is_explicitly_not_applicable(release):
    allow_not_applicable(release)
    release.save()
    result = release.run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(release.report_path.read_text())["ready"] is True


@pytest.mark.parametrize("earlier_result", ["applicable", "already-inapplicable", "wrong-order"])
def test_combined_capability_contracts_preserve_ordered_precondition_results(release, earlier_result):
    allow_not_applicable(release)

    def prepend_precondition(suite):
        properties = suite.findall("testcase")[-1].find("properties")
        precondition = ElementTree.Element(
            "property", name="contract_precondition", value="supports_headers=True"
        )
        applicability = ElementTree.Element(
            "property",
            name="contract_applicability",
            value="NOT_APPLICABLE" if earlier_result == "already-inapplicable" else "APPLICABLE",
        )
        properties.insert(0, precondition if earlier_result != "wrong-order" else applicability)
        properties.insert(1, applicability if earlier_result != "wrong-order" else precondition)

    release.update_junit("RG-01", prepend_precondition)
    release.save()
    result = release.run()
    if earlier_result == "applicable":
        assert result.returncode == 0, result.stdout + result.stderr
        assert json.loads(release.report_path.read_text())["ready"] is True
    else:
        assert_blocked(release, result)


def test_an_unrelated_pass_does_not_replace_all_inapplicable_required_cases(release):
    allow_not_applicable(release)
    profile = release.inventory["gates"][0]["checks"][0]["profiles"][0]
    # Only the optional case is registered as required; the remaining real pass
    # must not authorize a check whose entire required set was not applicable.
    profile["required_cases"] = profile["required_cases"][1:]
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("change", ["reason", "precondition", "applicability", "xfail"])
def test_not_applicable_requires_the_registered_reason_and_property_evidence(release, change):
    allow_not_applicable(release)

    def corrupt(suite):
        case = suite.findall("testcase")[-1]
        if change == "reason":
            case.find("skipped").set("message", "unreachable test service")
        elif change == "xfail":
            case.find("skipped").set("type", "pytest.xfail")
        else:
            name = "contract_precondition" if change == "precondition" else "contract_applicability"
            case.find(f"properties/property[@name='{name}']").set("value", "unexpected")

    release.update_junit("RG-01", corrupt)
    release.save()
    assert_blocked(release, release.run())


@pytest.mark.parametrize("document", ["inventory", "evidence", "jobs"])
def test_malformed_json_does_not_become_successful_evidence(release, document):
    path = {
        "inventory": release.inventory_path,
        "evidence": release.bundles["RG-01"] / "evidence.json",
        "jobs": release.jobs_path,
    }[document]
    path.write_text("{ invalid json")
    assert_blocked(release, release.run())


def test_duplicate_json_keys_cannot_hide_a_failed_or_stale_candidate(release):
    path = release.bundles["RG-01"] / "evidence.json"
    text = path.read_text()
    path.write_text(text.replace('"schema_version": 1,', '"schema_version": 0, "schema_version": 1,', 1))
    assert_blocked(release, release.run())


def test_evidence_paths_cannot_escape_their_downloaded_bundle(release):
    release.documents["RG-01"]["checks"][0]["junit"]["path"] = "../outside.xml"
    (release.evidence_dir / "outside.xml").write_text((release.bundles["RG-01"] / "results.xml").read_text())
    release.save()
    assert_blocked(release, release.run())


def test_supporting_artifact_contents_are_verified_too(release):
    artifact = release.bundles["RG-01"] / "compatibility.json"
    artifact.write_text('{"synthetic": true}')
    release.documents["RG-01"]["checks"][0]["artifacts"] = [
        {"path": artifact.name, "sha256": digest(artifact)}
    ]
    release.save()
    artifact.write_text('{"changed": true}')
    assert_blocked(release, release.run())


def test_symlinked_evidence_is_not_a_verified_bundle_file(release):
    path = release.bundles["RG-01"] / "results.xml"
    outside = release.evidence_dir.parent / "outside.xml"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    try:
        path.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("This platform does not permit a test-created symlink")
    assert_blocked(release, release.run())
