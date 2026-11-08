"""Traceability coverage is checked against source clauses and real AST selectors.

The small specification and test module below are synthetic schema fixtures;
they do not constitute release evidence for the actual project.
"""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "tools/check_traceability.py"


def source(anchor, text):
    return {"anchor": anchor, "text": text, "sha256": hashlib.sha256(text.encode()).hexdigest()}


def check(selector="tests/test_observer.py::test_observes"):
    return {
        "selector": selector,
        "kind": "unit",
        "modes": ["shared"],
        "platforms": ["linux", "macos", "windows"],
        "expected": "The synthetic marker is preserved.",
    }


@pytest.fixture
def traceability(tmp_path):
    spec = tmp_path / "SPEC.md"
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_observer.py").write_text(
        "def test_observes():\n    assert True\n\n"
        "class TestObserver:\n    def test_keeps_history(self):\n        assert True\n"
    )
    meta = "The term MUST indicates a mandatory obligation."
    requirement = "The observer MUST preserve the synthetic marker."
    deferred = "The future temporal assertion MUST preserve the marker."
    rows = [
        f"| AC-{index:02} | Synthetic criterion {index} | Marker is preserved |" for index in range(1, 89)
    ]
    spec.write_text(
        "# Synthetic release specification\n\n**Revision:** 1.6\\\n\n"
        '<a id="s01"></a>\n## 1. Definitions\n\n' + meta + "\n\n"
        '<a id="s02"></a>\n## 2. Core\n\n' + requirement + "\n\n"
        '<a id="s18"></a>\n## 18. Deferred temporal assertions\n\n' + deferred + "\n\n"
        '<a id="s25"></a>\n## 25. Acceptance criteria\n\n'
        "| ID | Scenario | Expected |\n| --- | --- | --- |\n" + "\n".join(rows) + "\n"
    )
    requirements = [
        {
            "id": "MUST-s01-001",
            "kind": "must",
            "scope": "meta",
            "source": source("s01", meta),
            "summary": "Define the normative term.",
            "checks": [],
            "gaps": [],
            "rationale": "Vocabulary, not an executable product obligation.",
        },
        {
            "id": "MUST-s02-001",
            "kind": "must",
            "scope": "mvp",
            "source": source("s02", requirement),
            "summary": "Preserve the synthetic marker.",
            "checks": [check()],
            "gaps": [],
        },
        {
            "id": "MUST-s18-001",
            "kind": "must",
            "scope": "deferred",
            "source": source("s18", deferred),
            "summary": "Preserve a future temporal marker.",
            "checks": [],
            "gaps": [],
            "rationale": "Temporal features are deferred by the release specification.",
        },
        *[
            {
                "id": f"AC-{index:02}",
                "kind": "acceptance",
                "scope": "mvp",
                "source": source("s25", text),
                "summary": f"Synthetic criterion {index}.",
                "checks": [check()],
                "gaps": [],
            }
            for index, text in enumerate(rows, 1)
        ],
    ]
    document = {"schema_version": 1, "spec_revision": "1.6", "requirements": requirements}
    inventory = tmp_path / "traceability.json"
    report = tmp_path / "report.json"

    def save():
        inventory.write_text(json.dumps(document, indent=2) + "\n")

    def run():
        save()
        return subprocess.run(
            [
                sys.executable,
                "-I",
                str(CHECKER),
                "--spec",
                str(spec),
                "--inventory",
                str(inventory),
                "--root",
                str(tmp_path),
                "--report",
                str(report),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

    return {"spec": spec, "document": document, "requirements": requirements, "report": report, "run": run}


def assert_invalid(traceability):
    result = traceability["run"]()
    assert result.returncode == 1, result.stdout + result.stderr
    assert json.loads(traceability["report"].read_text())["errors"]


def test_every_clause_and_acceptance_row_maps_to_a_real_executable_check(traceability):
    result = traceability["run"]()
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(traceability["report"].read_text())["errors"] == []


def test_missing_check_is_an_explicit_work_item_not_claimed_verification(traceability):
    requirement = traceability["requirements"][1]
    requirement["checks"] = []
    requirement["gaps"] = [
        {"id": "marker-native-proof", "stage": 4, "expected": "A real producer confirms the marker."}
    ]
    result = traceability["run"]()
    assert result.returncode == 0, result.stdout + result.stderr


def test_ast_qualified_class_test_selector_is_valid(traceability):
    traceability["requirements"][1]["checks"] = [
        check("tests/test_observer.py::TestObserver::test_keeps_history")
    ]
    result = traceability["run"]()
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("requirement_id", ["MUST-s02-001", "AC-01", "AC-88"])
def test_no_required_source_clause_or_acceptance_criterion_can_disappear(traceability, requirement_id):
    traceability["requirements"][:] = [
        row for row in traceability["requirements"] if row["id"] != requirement_id
    ]
    assert_invalid(traceability)


def test_new_normative_source_clause_requires_a_new_mapping(traceability):
    text = traceability["spec"].read_text()
    traceability["spec"].write_text(
        text.replace("## 2. Core", "## 2. Core\n\nThe observer MUST isolate a new field.")
    )
    assert_invalid(traceability)


@pytest.mark.parametrize("change", ["duplicate-id", "unknown-id", "wrong-kind", "stale-revision"])
def test_inventory_identity_and_kind_must_match_the_specification(traceability, change):
    row = traceability["requirements"][1]
    if change == "duplicate-id":
        traceability["requirements"].append(deepcopy(row))
    elif change == "unknown-id":
        row["id"] = "MUST-s02-999"
    elif change == "wrong-kind":
        row["kind"] = "acceptance"
    else:
        traceability["document"]["spec_revision"] = "1.5"
    assert_invalid(traceability)


@pytest.mark.parametrize("change", ["stale-text", "stale-hash", "wrong-anchor"])
def test_source_binding_cannot_be_reused_after_requirement_drift(traceability, change):
    row = traceability["requirements"][1]
    if change == "stale-text":
        row["source"]["text"] = "A different obligation."
    elif change == "stale-hash":
        row["source"]["sha256"] = "0" * 64
    else:
        row["source"]["anchor"] = "s03"
    assert_invalid(traceability)


@pytest.mark.parametrize("change", ["empty-mapping", "empty-expected", "no-platforms", "no-modes"])
def test_obligations_need_a_meaningful_check_or_missing_work_item(traceability, change):
    row = traceability["requirements"][1]
    if change == "empty-mapping":
        row["checks"] = []
    elif change == "empty-expected":
        row["checks"][0]["expected"] = ""
    elif change == "no-platforms":
        row["checks"][0]["platforms"] = []
    else:
        row["checks"][0]["modes"] = []
    assert_invalid(traceability)


@pytest.mark.parametrize(
    "selector",
    [
        "tests/missing.py::test_observes",
        "tests/test_observer.py::test_missing",
        "tests/test_observer.py::TestMissing::test_keeps_history",
        "tests/test_observer.py::TestObserver::test_missing",
        "../outside.py::test_observes",
    ],
)
def test_unresolvable_or_outside_ast_selector_is_rejected(traceability, selector):
    traceability["requirements"][1]["checks"] = [check(selector)]
    assert_invalid(traceability)


@pytest.mark.parametrize("scope", ["deferred", "meta"])
def test_mvp_obligation_cannot_be_hidden_by_reclassification(traceability, scope):
    traceability["requirements"][1]["scope"] = scope
    traceability["requirements"][1]["rationale"] = "Attempted exemption."
    assert_invalid(traceability)


@pytest.mark.parametrize("change", ["empty-id", "invalid-stage", "empty-outcome"])
def test_missing_work_items_need_identifiable_stage_and_expected_result(traceability, change):
    row = traceability["requirements"][1]
    gap = {"id": "marker-native-proof", "stage": 4, "expected": "Real marker proof."}
    row["checks"], row["gaps"] = [], [gap]
    if change == "empty-id":
        gap["id"] = ""
    elif change == "invalid-stage":
        gap["stage"] = 99
    else:
        gap["expected"] = ""
    assert_invalid(traceability)
