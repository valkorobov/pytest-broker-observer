"""Resolve audited compatibility profiles without silently relaxing direct pins."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def normalize(name):
    """Normalize a distribution name according to the project's requirement grammar."""
    return re.sub(r"[-_.]+", "-", name).lower()


def version_tuple(value):
    """Compare numeric versions without treating a post-release as its floor."""
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)*)(?:\.post([0-9]+))?", value)
    if match is None:
        raise ValueError("profile contains an unsupported version form")
    values = tuple(int(part) for part in match.group(1).split("."))
    release = values + (0,) * max(0, 3 - len(values))
    return release, int(match.group(2)) if match.group(2) is not None else -1


def applies(marker, python_version):
    """Evaluate the explicitly supported Python-specific lower-bound markers."""
    if not marker:
        return True
    match = re.fullmatch(r"python_version\s*(<|>=)\s*(['\"])(3\.[0-9]+)\2", marker.strip())
    if match is None:
        raise ValueError("project dependency uses an unsupported compatibility marker")
    left, right = version_tuple(python_version), version_tuple(match.group(3))
    return left < right if match.group(1) == "<" else left >= right


def declarations(project, python_version):
    """Collect public extras and test dependencies, preserving each applicable bound."""
    requirements = [
        value for values in project["project"]["optional-dependencies"].values() for value in values
    ]
    requirements += project["dependency-groups"]["test"]
    result = defaultdict(lambda: {"extras": set(), "bounds": []})
    for requirement in requirements:
        value, _, marker = requirement.partition(";")
        if not applies(marker.strip(), python_version):
            continue
        match = re.fullmatch(r"([A-Za-z0-9._-]+)(?:\[([A-Za-z0-9,._-]+)\])?(.+)", value.strip())
        if match is None:
            raise ValueError("project dependency has an unsupported declaration")
        name, extras, bounds = match.groups()
        entry = result[normalize(name)]
        if extras:
            entry["extras"].update(normalize(extra) for extra in extras.split(","))
        for bound in bounds.split(","):
            parsed = re.fullmatch(r"\s*(>=|<)\s*([0-9]+(?:\.[0-9]+)*)\s*", bound)
            if parsed is None:
                raise ValueError("project dependency has an unsupported version bound")
            entry["bounds"].append(parsed.groups())
    return result


def parse_pin_name(value):
    """Parse an exact profile distribution and its requested extras."""
    match = re.fullmatch(r"([a-z0-9][a-z0-9-]*)(?:\[([a-z0-9,-]+)\])?", value)
    if match is None:
        raise ValueError("profile contains an unsupported distribution name")
    name, extras = match.groups()
    return name, set(extras.split(",")) if extras else set()


def load_profile(
    profile,
    python_version,
    project_path=ROOT / "pyproject.toml",
    inventory_path=ROOT / "docs/compatibility-profiles.json",
    lock_path=ROOT / "uv.lock",
):
    """Return validated direct requirements and optional locked transitive constraints."""
    inventory = json.loads(Path(inventory_path).read_text(encoding="utf-8"))
    if inventory.get("schema_version") != 1 or profile not in ("minimum", "current"):
        raise ValueError("unsupported compatibility profile schema or name")
    if inventory.get("python_versions") != ["3.11", "3.12", "3.13", "3.14"]:
        raise ValueError("profile inventory must retain every supported Python version")
    if inventory.get("platforms") != ["linux", "macos", "windows"]:
        raise ValueError("profile inventory must retain every supported platform")
    if python_version not in inventory["python_versions"]:
        raise ValueError("unsupported compatibility interpreter")
    selected = inventory["profiles"][profile]
    pins = dict(selected["pins"])
    overrides = selected["python_overrides"]
    if set(overrides) - set(inventory["python_versions"]):
        raise ValueError("profile override uses an unsupported Python version")
    for key, value in overrides.get(python_version, {}).items():
        if key not in pins:
            raise ValueError("profile override must replace an existing direct pin")
        pins[key] = value
    project = tomllib.loads(Path(project_path).read_text(encoding="utf-8"))
    declared = declarations(project, python_version)
    by_name = {}
    for key, version in pins.items():
        name, extras = parse_pin_name(key)
        if name in by_name or name not in declared or extras != declared[name]["extras"]:
            raise ValueError("profile direct distributions or extras differ from project declarations")
        numeric = version_tuple(version)
        for operator, bound in declared[name]["bounds"]:
            if not (numeric >= version_tuple(bound) if operator == ">=" else numeric < version_tuple(bound)):
                raise ValueError("profile direct pin violates project dependency bounds")
        if profile == "minimum":
            floor = max(
                version_tuple(bound) for operator, bound in declared[name]["bounds"] if operator == ">="
            )
            if numeric != floor:
                raise ValueError("minimum profile must use the exact applicable direct lower bound")
        by_name[name] = version
    if set(by_name) != set(declared):
        raise ValueError("profile omits a declared direct dependency")
    lock = tomllib.loads(Path(lock_path).read_text(encoding="utf-8"))
    locked = {}
    for package in lock["package"]:
        if "registry" in package.get("source", {}):
            name = normalize(package["name"])
            if name in locked and locked[name] != package["version"]:
                raise ValueError("current profile needs explicit handling for multiple locked versions")
            locked[name] = package["version"]
    if profile == "current" and any(locked.get(name) != version for name, version in by_name.items()):
        raise ValueError("current profile direct versions differ from uv.lock")
    return {
        "schema_version": 1,
        "profile": profile,
        "python": python_version,
        "description": selected["description"],
        "requirements": [f"{name}=={version}" for name, version in sorted(pins.items())],
        "constraints": [f"{name}=={version}" for name, version in sorted(locked.items())]
        if profile == "current"
        else [],
        "direct_versions": dict(sorted(by_name.items())),
    }


def main():
    """Write exact requirement inputs and a declared-profile manifest."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("minimum", "current"), required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--project", type=Path, default=ROOT / "pyproject.toml")
    parser.add_argument("--inventory", type=Path, default=ROOT / "docs/compatibility-profiles.json")
    parser.add_argument("--lock", type=Path, default=ROOT / "uv.lock")
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--constraints", type=Path)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = load_profile(args.profile, args.python, args.project, args.inventory, args.lock)
    except (OSError, ValueError, KeyError, TypeError):
        parser.exit(1, "Compatibility profile is invalid or inconsistent with project metadata.\n")
    for path, content in (
        (args.requirements, "\n".join(result["requirements"]) + "\n"),
        (args.manifest, json.dumps(result, indent=2) + "\n"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    if args.constraints:
        args.constraints.parent.mkdir(parents=True, exist_ok=True)
        args.constraints.write_text("\n".join(result["constraints"]) + "\n", encoding="utf-8")
    print(f"Compatibility profile: {args.profile} / Python {args.python}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
