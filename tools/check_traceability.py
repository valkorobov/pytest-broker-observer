"""Validate SPEC requirement coverage by checks or explicit staged work items.

References are checked statically without importing test modules or contacting a
broker. A complete inventory is not evidence that the referenced checks passed.
"""

import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path, PurePosixPath
import re


ROOT = Path(__file__).resolve().parents[1]


def extract_spec(text):
    """Assign stable anchor/ordinal IDs to physical MUST lines and acceptance rows."""
    revision = re.search(r"\*\*Revision:\*\*\s*([0-9.]+)", text)
    if not revision:
        raise ValueError("SPEC revision is missing")
    requirements = {}
    counts = Counter()
    anchor = None
    for line in text.splitlines():
        match = re.search(r'<a id="(s[0-9]+)"></a>', line)
        if match:
            anchor = match.group(1)
        acceptance = re.match(r"\|\s*(AC-[0-9]{2})\s*\|", line)
        if not re.search(r"\bMUST\b", line) and not acceptance:
            continue
        if not anchor:
            raise ValueError("requirement has no section anchor")
        kind = "acceptance" if acceptance else "must"
        if acceptance:
            identifier = acceptance.group(1)
        else:
            counts[anchor] += 1
            identifier = f"MUST-{anchor}-{counts[anchor]:03d}"
        if identifier in requirements:
            raise ValueError("SPEC contains a duplicate requirement ID")
        source = line.strip()
        scope = "mvp"
        if kind == "must" and anchor == "s01":
            scope = "meta"
        elif kind == "must" and anchor == "s18":
            scope = "deferred"
        requirements[identifier] = {
            "kind": kind,
            "scope": scope,
            "source": {
                "anchor": anchor,
                "text": source,
                "sha256": hashlib.sha256(source.encode()).hexdigest(),
            },
        }
    if {identifier for identifier in requirements if identifier.startswith("AC-")} != {
        f"AC-{index:02d}" for index in range(1, 89)
    }:
        raise ValueError("SPEC must contain exactly AC-01 through AC-88")
    return revision.group(1), requirements


def test_symbols(path):
    """Collect directly defined test functions, including class methods."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    result = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"):
            result.add(node.name)
        elif isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            for method in node.body:
                if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)) and method.name.startswith(
                    "test_"
                ):
                    result.add(f"{node.name}::{method.name}")
    return result


def selector_exists(selector, root, cache):
    """Resolve an exact static pytest test selector inside tests or examples."""
    if not isinstance(selector, str):
        return False
    parts = selector.split("::")
    relative = PurePosixPath(parts[0])
    if (
        len(parts) not in (2, 3)
        or relative.is_absolute()
        or "\\" in parts[0]
        or ".." in relative.parts
        or relative.parts[0:1] not in (("tests",), ("examples",))
        or relative.suffix != ".py"
    ):
        return False
    path = root / relative
    if not path.is_file() or not path.resolve().is_relative_to(root.resolve()):
        return False
    if path not in cache:
        cache[path] = test_symbols(path)
    return "::".join(parts[1:]) in cache[path]


def nonempty(value):
    """Identify meaningful text fields."""
    return isinstance(value, str) and bool(value.strip())


def validate(spec, inventory, root):
    """Reject stale, missing, misclassified, or untraceable requirements."""
    revision, extracted = extract_spec(spec)
    errors = []
    if not isinstance(inventory, dict) or inventory.get("schema_version") != 1:
        raise ValueError("unsupported inventory schema")
    if inventory.get("spec_revision") != revision:
        errors.append("inventory revision differs from SPEC")
    rows = inventory.get("requirements")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("requirements must be a list of objects")
    ids = [row.get("id") for row in rows]
    if any(not isinstance(identifier, str) for identifier in ids):
        raise ValueError("requirement IDs must be strings")
    if len(set(ids)) != len(ids):
        errors.append("inventory contains duplicate requirement IDs")
    for identifier in sorted(set(extracted) - set(ids)):
        errors.append(f"{identifier}: missing inventory row")
    if set(ids) - set(extracted):
        errors.append("inventory contains unknown requirement IDs")
    cache = {}
    for row in rows:
        identifier = row["id"]
        expected = extracted.get(identifier)
        if expected is None:
            continue
        for key in ("kind", "scope", "source"):
            if row.get(key) != expected[key]:
                errors.append(f"{identifier}: stale or incorrect {key}")
        if not nonempty(row.get("summary")):
            errors.append(f"{identifier}: summary is missing")
        checks, gaps = row.get("checks"), row.get("gaps")
        if not isinstance(checks, list) or not isinstance(gaps, list):
            errors.append(f"{identifier}: checks and gaps must be lists")
            continue
        if expected["scope"] != "mvp" and not nonempty(row.get("rationale")):
            errors.append(f"{identifier}: non-MVP classification needs a rationale")
        if expected["scope"] == "mvp" and not checks and not gaps:
            errors.append(f"{identifier}: no check or explicit missing-check work item")
        for check in checks:
            if not isinstance(check, dict):
                errors.append(f"{identifier}: invalid check")
                continue
            if not selector_exists(check.get("selector"), root, cache):
                errors.append(f"{identifier}: test selector does not resolve to a directly defined test")
            if check.get("kind") not in {"unit", "native-double", "integration", "packaging", "ci"}:
                errors.append(f"{identifier}: invalid check kind")
            for key, allowed in (
                ("modes", {"sync", "async", "shared"}),
                ("platforms", {"linux", "macos", "windows"}),
            ):
                values = check.get(key)
                if (
                    not isinstance(values, list)
                    or not values
                    or any(value not in allowed for value in values)
                ):
                    errors.append(f"{identifier}: invalid {key}")
            if not nonempty(check.get("expected")):
                errors.append(f"{identifier}: check needs an expected outcome")
        gap_ids = set()
        for gap in gaps:
            if (
                not isinstance(gap, dict)
                or not nonempty(gap.get("id"))
                or not nonempty(gap.get("expected"))
                or type(gap.get("stage")) is not int
                or gap["stage"] not in range(1, 9)
            ):
                errors.append(f"{identifier}: invalid staged work item")
            elif gap["id"] in gap_ids:
                errors.append(f"{identifier}: duplicate work item ID")
            else:
                gap_ids.add(gap["id"])
    return errors, {
        "spec_revision": revision,
        "requirements": len(extracted),
        "must": sum(row["kind"] == "must" for row in extracted.values()),
        "acceptance": sum(row["kind"] == "acceptance" for row in extracted.values()),
        "rows_with_checks": sum(bool(row.get("checks")) for row in rows),
        "rows_with_gaps": sum(bool(row.get("gaps")) for row in rows),
    }


def main():
    """Write a structural inventory report, never a test-pass attestation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=ROOT / "SPEC.md")
    parser.add_argument("--inventory", type=Path, default=ROOT / "docs/traceability.json")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = {"schema_version": 1, "inventory_complete": False, "errors": []}
    try:
        errors, counts = validate(
            args.spec.read_text(encoding="utf-8"),
            json.loads(args.inventory.read_text(encoding="utf-8")),
            args.root,
        )
        report.update(counts)
        report["errors"] = errors
        report["inventory_complete"] = not errors
    except (OSError, ValueError, TypeError, KeyError, SyntaxError):
        report["errors"] = ["SPEC, inventory, or referenced test source could not be validated"]
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Traceability inventory: " + ("complete" if report["inventory_complete"] else "invalid"))
    for error in report["errors"]:
        print(error)
    return 0 if report["inventory_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
