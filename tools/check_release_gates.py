"""Evaluate candidate-bound release evidence before granting publication access.

This tool runs without package imports or publishing credentials. Inventory
entries describe required checks; outcomes are derived from hashed JUnit files
and the caller's GitHub job results rather than supplied pass flags.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sys
import tomllib
from xml.etree import ElementTree


GATES = {f"RG-{number:02}" for number in range(1, 16)}
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024


class GateError(ValueError):
    """Reject malformed or inconsistent release evidence."""


def require(condition, message):
    """Reject a failed invariant with a safe, caller-provided summary.

    :param condition: Invariant that must hold.
    :param message: Safe diagnostic without raw evidence contents.
    """
    if not condition:
        raise GateError(message)


def unique_object(pairs):
    """Reject duplicate JSON keys instead of accepting the last supplied value.

    :param pairs: Object entries supplied by the JSON decoder.
    :returns: An unambiguous object.
    """
    result = {}
    for key, value in pairs:
        require(key not in result, "Duplicate JSON key")
        result[key] = value
    return result


def read_json(path):
    """Read a bounded ordinary JSON document without exposing parser payloads.

    :param path: Document to read.
    :returns: Decoded JSON data.
    """
    require(path.is_file() and not path.is_symlink(), "Missing or linked JSON document")
    require(path.stat().st_size <= MAX_DOCUMENT_BYTES, "JSON document exceeds the size budget")
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise GateError("Invalid JSON document") from error


def identifier(value):
    """Validate a non-secret inventory/profile identifier.

    :param value: Identifier from an inventory or evidence document.
    :returns: The validated identifier.
    """
    require(isinstance(value, str) and IDENTIFIER.fullmatch(value), "Invalid inventory identifier")
    return value


def keys(value, expected):
    """Require an exact object shape, rejecting hidden result overrides.

    :param value: JSON object under validation.
    :param expected: Allowed and required keys.
    """
    require(isinstance(value, dict) and set(value) == set(expected), "Invalid evidence object fields")


def digest(path):
    """Hash an ordinary file incrementally.

    :param path: File whose bytes must match the verified release input.
    :returns: Lowercase SHA-256 digest.
    """
    require(path.is_file() and not path.is_symlink(), "Missing or linked evidence file")
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def checked_file(root, entry):
    """Resolve a hashed supporting file strictly inside its evidence bundle.

    :param root: Bundle root directory.
    :param entry: Exact path and SHA-256 object.
    :returns: The verified file path.
    """
    keys(entry, ("path", "sha256"))
    name, expected = entry["path"], entry["sha256"]
    require(isinstance(name, str) and "\\" not in name, "Invalid evidence file path")
    relative = PurePosixPath(name)
    require(
        name and not relative.is_absolute() and all(part not in {".", ".."} for part in name.split("/")),
        "Evidence path escapes its bundle",
    )
    require(all(IDENTIFIER.fullmatch(part) for part in relative.parts), "Invalid evidence file path")
    require(isinstance(expected, str) and SHA256.fullmatch(expected), "Invalid evidence digest")
    path = root
    for part in relative.parts:
        path = path / part
        require(not path.is_symlink(), "Linked evidence path")
    require(path.resolve().is_relative_to(root.resolve()), "Evidence path escapes its bundle")
    require(digest(path) == expected, "Evidence digest mismatch")
    return path


def inventory_checks(document):
    """Validate all fifteen gates and expand their required check/profile pairs.

    :param document: Static requirements inventory, never a result ledger.
    :returns: Mapping from check/profile pairs to gate, check and profile definitions.
    """
    keys(document, ("schema_version", "distribution", "gates"))
    require(
        type(document["schema_version"]) is int and document["schema_version"] == 1,
        "Unknown inventory schema",
    )
    require(document["distribution"] == "pytest-broker-observer", "Unknown distribution")
    require(isinstance(document["gates"], list), "Invalid gate inventory")
    seen_gates, seen_checks, expanded = set(), set(), {}
    for gate in document["gates"]:
        keys(gate, ("id", "title", "checks"))
        gate_id = gate["id"]
        require(
            isinstance(gate_id, str) and gate_id in GATES and gate_id not in seen_gates,
            "Unknown or duplicate gate",
        )
        seen_gates.add(gate_id)
        require(isinstance(gate["title"], str) and gate["title"].strip(), "Missing gate title")
        require(isinstance(gate["checks"], list) and gate["checks"], "Gate has no required checks")
        for check in gate["checks"]:
            keys(check, ("id", "producer", "implementation", "profiles"))
            check_id = identifier(check["id"])
            identifier(check["producer"])
            require(check_id not in seen_checks, "Duplicate check identifier")
            seen_checks.add(check_id)
            require(check["implementation"] in {"planned", "ready"}, "Invalid check implementation state")
            require(
                isinstance(check["profiles"], list) and check["profiles"], "Check has no required profiles"
            )
            for profile in check["profiles"]:
                keys(profile, ("id", "required_cases", "allowed_not_applicable"))
                profile_id = identifier(profile["id"])
                pair = (check_id, profile_id)
                require(pair not in expanded, "Duplicate check profile")
                cases = profile["required_cases"]
                require(
                    isinstance(cases, list) and all(isinstance(case, str) and case for case in cases),
                    "Invalid required cases",
                )
                require(len(cases) == len(set(cases)), "Duplicate required case")
                allowed = profile["allowed_not_applicable"]
                require(isinstance(allowed, list), "Invalid capability applicability list")
                seen_allowed = set()
                for item in allowed:
                    keys(item, ("node_id", "reason", "precondition"))
                    require(
                        all(isinstance(value, str) and value for value in item.values()),
                        "Invalid applicability rule",
                    )
                    require(
                        item["node_id"] in cases and item["node_id"] not in seen_allowed,
                        "Invalid applicability case",
                    )
                    seen_allowed.add(item["node_id"])
                expanded[pair] = (gate_id, check, profile)
    require(seen_gates == GATES, "Inventory must contain every RG-01 through RG-15")
    return expanded


def distribution_hashes(directory, version):
    """Verify the exact two distribution files and their packaging manifest.

    :param directory: Downloaded same-run distribution directory.
    :param version: Expected project version.
    :returns: Filename-to-digest mapping for the verified files.
    """
    require(directory.is_dir() and not directory.is_symlink(), "Missing distribution directory")
    expected = {
        f"pytest_broker_observer-{version}-py3-none-any.whl",
        f"pytest_broker_observer-{version}.tar.gz",
    }
    require(
        {path.name for path in directory.iterdir()} == expected,
        "Missing, extra or incorrectly named distribution",
    )
    hashes = {name: digest(directory / name) for name in sorted(expected)}
    manifest = directory.parent / "SHA256SUMS"
    require(manifest.is_file() and not manifest.is_symlink(), "Missing packaging checksum manifest")
    require(manifest.stat().st_size <= 8192, "Oversized packaging checksum manifest")
    entries = {}
    for line in manifest.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        require(len(parts) == 2 and SHA256.fullmatch(parts[0]), "Invalid packaging checksum manifest")
        name = parts[1].removeprefix("*")
        require(
            name.startswith("dist/") and name[5:] in expected and name not in entries,
            "Invalid packaging checksum path",
        )
        entries[name] = parts[0]
    require(
        entries == {f"dist/{name}": value for name, value in hashes.items()}, "Distribution checksum mismatch"
    )
    return hashes


def check_junit(path, profile):
    """Derive check outcomes from actual JUnit cases and explicit capability rules.

    :param path: Hashed JUnit file.
    :param profile: Required case IDs and allowed capability preconditions.
    :returns: Measured case counts.
    """
    require(path.stat().st_size <= MAX_DOCUMENT_BYTES, "JUnit document exceeds the size budget")
    try:
        root = ElementTree.parse(path).getroot()
    except ElementTree.ParseError as error:
        raise GateError("Invalid JUnit document") from error
    require(root.tag in {"testsuites", "testsuite"}, "Unknown JUnit root")
    require(
        not any(list(root.iter(tag)) for tag in ("flakyFailure", "flakyError", "rerunFailure", "rerunError")),
        "JUnit contains a hidden first failure or rerun",
    )
    require(
        not list(root.iter("failure")) and not list(root.iter("error")),
        "JUnit contains failed or errored cases",
    )
    cases = list(root.iter("testcase"))
    require(cases, "JUnit contains no cases")
    allowed = {item["node_id"]: item for item in profile["allowed_not_applicable"]}
    seen, passed, skipped = set(), set(), 0
    for case in cases:
        require(case.get("classname") and case.get("name"), "JUnit case has no stable identity")
        require(case.get("status") in {None, "run", "passed"}, "JUnit case was not executed successfully")
        require(not {"xfail", "wasxfail"}.intersection(case.attrib), "JUnit contains an expected failure")
        require(
            all(child.tag in {"properties", "skipped", "system-out", "system-err"} for child in case),
            "Unsupported JUnit outcome",
        )
        node_id = f"{case.get('classname')}::{case.get('name')}"
        require(node_id not in seen, "Duplicate JUnit case")
        seen.add(node_id)
        skip = case.findall("skipped")
        require(len(skip) <= 1, "Duplicate JUnit skip result")
        if skip:
            rule = allowed.get(node_id)
            # A contract can check several capabilities sequentially. Retain
            # ordered property pairs instead of trusting an ambiguous last value.
            properties = [
                (item.get("name"), item.get("value"))
                for item in case.findall("properties/property")
                if item.get("name") in {"contract_precondition", "contract_applicability"}
            ]
            ordered = bool(properties) and len(properties) % 2 == 0
            if ordered:
                ordered = all(
                    properties[index][0] == "contract_precondition"
                    and bool(properties[index][1])
                    and properties[index + 1][0] == "contract_applicability"
                    and properties[index + 1][1]
                    == ("NOT_APPLICABLE" if index + 2 == len(properties) else "APPLICABLE")
                    for index in range(0, len(properties), 2)
                )
            require(
                rule is not None
                and ordered
                and "xfail" not in skip[0].get("type", "").lower()
                and skip[0].get("message") == rule["reason"]
                and properties[-2][1] == rule["precondition"],
                "Unexpected mandatory skip or xfail",
            )
            skipped += 1
        else:
            passed.add(node_id)
    require(set(profile["required_cases"]) <= seen, "Required JUnit cases are missing")
    require(set(profile["required_cases"]) & passed, "A check needs a successfully executed required case")
    for suite in root.iter():
        if suite.tag not in {"testsuites", "testsuite"}:
            continue
        actual = list(suite.iter("testcase"))
        expected = {
            "tests": len(actual),
            "failures": 0,
            "errors": 0,
            "skipped": sum(bool(case.findall("skipped")) for case in actual),
        }
        for key, count in expected.items():
            if key in suite.attrib:
                require(suite.get(key) == str(count), "JUnit suite counters disagree with case results")
    return {"tests": len(cases), "passed": len(cases) - skipped, "not_applicable": skipped}


def check_environment(environment, profile_id):
    """Require concrete non-secret runtime and tool version metadata.

    :param environment: Environment observed by the producer job.
    :param profile_id: Registered profile whose explicit platform/Python must match.
    """
    keys(environment, ("python", "platform", "versions"))
    require(
        isinstance(environment["python"], str)
        and re.fullmatch(r"3\.(?:11|12|13|14)\.[0-9]+(?:[a-z][0-9]+)?", environment["python"]),
        "Missing concrete supported Python version",
    )
    require(environment["platform"] in {"Linux", "Darwin", "Windows"}, "Unknown runtime platform")
    for prefix, platform in (("linux-", "Linux"), ("macos-", "Darwin"), ("windows-", "Windows")):
        if profile_id.startswith(prefix):
            require(environment["platform"] == platform, "Runtime platform disagrees with required profile")
    python = re.search(r"cpython-(3\.(?:11|12|13|14))(?:$|-)", profile_id)
    if python:
        require(
            environment["python"].startswith(python[1] + "."),
            "Python version disagrees with required profile",
        )
    require(isinstance(environment["versions"], dict) and environment["versions"], "Missing tool versions")
    for name, version in environment["versions"].items():
        identifier(name)
        require(
            isinstance(version, str) and re.fullmatch(r"[0-9][A-Za-z0-9.+_-]*", version),
            "Missing concrete tool version",
        )


def write_json(path, document):
    """Write a reviewable JSON artifact, creating its parent directory.

    :param path: Output file.
    :param document: JSON-serializable report.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def evaluate(args):
    """Collect all required profile outcomes without allowing one pass flag to authorize upload.

    :param args: Parsed CLI options and expected GitHub context.
    :returns: Candidate report with readiness, observed evidence, and blocking errors.
    """
    report = {
        "schema_version": 1,
        "ready": False,
        "candidate": {},
        "distributions": {},
        "gates": {},
        "errors": [],
    }
    candidate = {
        "repository": args.repository,
        "sha": args.sha,
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "version": "",
        "tag": args.tag or None,
    }
    report["candidate"] = candidate
    try:
        require(bool(re.fullmatch(r"[\w.-]+/[\w.-]+", args.repository or "")), "Missing repository identity")
        require(bool(re.fullmatch(r"[0-9a-f]{40}", args.sha or "")), "Invalid candidate source SHA")
        require(
            all(re.fullmatch(r"[1-9][0-9]*", value or "") for value in (args.run_id, args.run_attempt)),
            "Invalid workflow run identity",
        )
        project = tomllib.loads(args.project.read_text(encoding="utf-8"))["project"]
        require(project["name"] == "pytest-broker-observer", "Unknown project distribution")
        candidate["version"] = identifier(project["version"])
        require(args.tag in {"", f"v{candidate['version']}"}, "Release tag and project version disagree")
        if args.mode == "enforce":
            require(args.tag == f"v{candidate['version']}", "Publication requires a matching release tag")
        required = inventory_checks(read_json(args.inventory))
        report["inventory_sha256"] = digest(args.inventory)
        report["distributions"] = distribution_hashes(args.dist_dir, candidate["version"])
        jobs = read_json(args.job_results)
        require(isinstance(jobs, dict), "Invalid GitHub job result map")
        found = {}
        if args.evidence_dir.exists():
            require(
                args.evidence_dir.is_dir() and not args.evidence_dir.is_symlink(),
                "Linked or invalid evidence directory",
            )
            # download-artifact v5 flattens a single pattern match, but keeps
            # artifact directories for multiple matches. Support both layouts.
            bundles = (
                [args.evidence_dir]
                if (args.evidence_dir / "evidence.json").exists()
                else sorted(args.evidence_dir.iterdir())
            )
            for bundle in bundles:
                require(bundle.is_dir() and not bundle.is_symlink(), "Invalid evidence bundle directory")
                document = read_json(bundle / "evidence.json")
                keys(document, ("schema_version", "candidate", "producer", "checks"))
                require(
                    type(document["schema_version"]) is int and document["schema_version"] == 1,
                    "Unknown evidence schema",
                )
                require(
                    document["candidate"] == candidate,
                    "Evidence belongs to another candidate, run or attempt",
                )
                producer = identifier(document["producer"])
                bundle_name = identifier(
                    f"release-gate-evidence-{producer}" if bundle == args.evidence_dir else bundle.name
                )
                require(isinstance(document["checks"], list) and document["checks"], "Empty check evidence")
                for evidence in document["checks"]:
                    keys(evidence, ("id", "profile", "environment", "junit", "artifacts", "distributions"))
                    pair = (identifier(evidence["id"]), identifier(evidence["profile"]))
                    require(
                        pair in required and pair not in found, "Unknown or duplicate check/profile evidence"
                    )
                    gate_id, check, profile = required[pair]
                    require(producer == check["producer"], "Evidence producer does not match inventory")
                    require(
                        isinstance(jobs.get(producer), dict) and jobs[producer].get("result") == "success",
                        "Producer job did not succeed in this run",
                    )
                    require(
                        evidence["distributions"] == report["distributions"],
                        "Evidence used different distribution bytes",
                    )
                    check_environment(evidence["environment"], pair[1])
                    require(isinstance(evidence["artifacts"], list), "Invalid supporting artifact list")
                    for entry in evidence["artifacts"]:
                        checked_file(bundle, entry)
                    counts = check_junit(checked_file(bundle, evidence["junit"]), profile)
                    found[pair] = {
                        "profile": pair[1],
                        "producer": producer,
                        "environment": evidence["environment"],
                        "cases": counts,
                        "junit": evidence["junit"],
                        "artifacts": evidence["artifacts"],
                        "evidence_sha256": digest(bundle / "evidence.json"),
                        "bundle": bundle_name,
                        "source_bundle": bundle.relative_to(args.evidence_dir).as_posix(),
                    }
        for pair, (gate_id, check, profile) in required.items():
            gate = report["gates"].setdefault(gate_id, {"ready": True, "checks": []})
            problems = []
            if check["implementation"] != "ready":
                problems.append("check implementation is planned")
            if not profile["required_cases"]:
                problems.append("required case inventory is incomplete")
            if pair not in found:
                problems.append("candidate evidence is missing")
            if problems:
                gate["ready"] = False
                report["errors"].append(f"{gate_id}/{pair[0]}/{pair[1]}: " + "; ".join(problems))
            gate["checks"].append(
                {"id": pair[0], "profile": pair[1], "ready": not problems, "evidence": found.get(pair)}
            )
        report["ready"] = not report["errors"] and set(report["gates"]) == GATES
    except (GateError, OSError, ValueError, KeyError, TypeError) as error:
        # Raw parse/file errors can include credentials or payloads. Only our safe
        # invariant messages are included in the machine-readable public report.
        report["errors"].append(
            str(error) if isinstance(error, GateError) else "Malformed or unavailable release input"
        )
        report["invalid"] = True
    return report


def assemble_bundle(args, report):
    """Copy the exact verified inputs into a new publication bundle and hash it.

    :param args: CLI options including an unused output directory.
    :param report: Successful candidate-bound report.
    """
    require(not args.bundle_dir.exists(), "Publication bundle directory already exists")
    require(
        not args.bundle_dir.resolve().is_relative_to(args.evidence_dir.resolve())
        and not args.bundle_dir.resolve().is_relative_to(args.dist_dir.resolve()),
        "Publication output overlaps release inputs",
    )
    args.bundle_dir.mkdir(parents=True)
    try:
        directory = args.bundle_dir / "dist"
        directory.mkdir()
        for name, expected in report["distributions"].items():
            shutil.copyfile(args.dist_dir / name, directory / name)
            require(
                digest(directory / name) == expected,
                "Distribution changed while assembling publication bundle",
            )
        write_json(args.bundle_dir / "release-gates.json", report)
        write_json(
            args.bundle_dir / "compatibility.json",
            {
                "schema_version": 1,
                "candidate": report["candidate"],
                "profiles": [
                    item["evidence"] for gate in report["gates"].values() for item in gate["checks"]
                ],
            },
        )
        for gate in report["gates"].values():
            for check in gate["checks"]:
                evidence = check["evidence"]
                source = args.evidence_dir / evidence["source_bundle"]
                destination = args.bundle_dir / "evidence" / evidence["bundle"]
                entries = [
                    {"path": "evidence.json", "sha256": evidence["evidence_sha256"]},
                    evidence["junit"],
                    *evidence["artifacts"],
                ]
                for entry in entries:
                    original = checked_file(source, entry)
                    target = destination / entry["path"]
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(original, target)
                    require(digest(target) == entry["sha256"], "Evidence changed during publication assembly")
        files = sorted(path for path in args.bundle_dir.rglob("*") if path.is_file())
        (args.bundle_dir / "SHA256SUMS").write_text(
            "".join(f"{digest(path)}  {path.relative_to(args.bundle_dir).as_posix()}\n" for path in files),
            encoding="utf-8",
        )
    except BaseException:
        shutil.rmtree(args.bundle_dir)
        raise


def main(argv=None):
    """Evaluate readiness and optionally assemble a strict-success publication bundle.

    :param argv: Optional argument list for CLI testing.
    :returns: Zero for success/audit, one for blocked publication or invalid inputs.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=Path("docs/release-gates.json"))
    parser.add_argument("--project", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--dist-dir", type=Path, required=True)
    parser.add_argument("--job-results", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--bundle-dir", type=Path)
    parser.add_argument("--mode", choices=("audit", "enforce"), default="enforce")
    for name, variable in (
        ("repository", "GITHUB_REPOSITORY"),
        ("sha", "GITHUB_SHA"),
        ("run-id", "GITHUB_RUN_ID"),
        ("run-attempt", "GITHUB_RUN_ATTEMPT"),
        ("tag", "RELEASE_TAG"),
    ):
        parser.add_argument(f"--{name}", default=os.environ.get(variable, ""))
    args = parser.parse_args(argv)
    report = evaluate(args)
    if report["ready"] and args.mode == "enforce" and args.bundle_dir is not None:
        try:
            assemble_bundle(args, report)
        except (GateError, OSError) as error:
            report["ready"] = False
            report["errors"].append(
                str(error) if isinstance(error, GateError) else "Cannot assemble publication bundle"
            )
    write_json(args.report, report)
    print("Release gates: " + ("READY" if report["ready"] else "BLOCKED"))
    for error in report["errors"]:
        print(error)
    return int(not report["ready"]) if args.mode == "enforce" else int(report.get("invalid", False))


if __name__ == "__main__":
    sys.exit(main())
