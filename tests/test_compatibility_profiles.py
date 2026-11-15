"""Compatibility inputs must retain exact direct floors and locked current pins."""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "compatibility_profiles", ROOT / "tools/compatibility_profiles.py"
)
assert SPEC and SPEC.loader
PROFILES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROFILES)


@pytest.fixture
def profile_files(tmp_path):
    inventory = json.loads((ROOT / "docs/compatibility-profiles.json").read_text())
    project = (ROOT / "pyproject.toml").read_text()
    project_path = tmp_path / "pyproject.toml"
    project_path.write_text(project)
    inventory_path = tmp_path / "profiles.json"

    def load(profile="minimum", python="3.14"):
        inventory_path.write_text(json.dumps(inventory))
        return PROFILES.load_profile(profile, python, project_path, inventory_path, ROOT / "uv.lock")

    return inventory, project_path, load


@pytest.mark.parametrize("python", ["3.11", "3.12", "3.13", "3.14"])
@pytest.mark.parametrize("profile", ["minimum", "current"])
def test_each_supported_interpreter_has_exact_complete_direct_pins(profile_files, python, profile):
    _, _, load = profile_files
    result = load(profile, python)
    assert len(result["requirements"]) == 12
    assert len(result["direct_versions"]) == 12
    assert all("==" in requirement for requirement in result["requirements"])
    assert bool(result["constraints"]) == (profile == "current")
    expected = "2.16.1" if profile == "current" else ("2.12.1" if python == "3.14" else "2.6.0")
    assert result["direct_versions"]["confluent-kafka"] == expected


@pytest.mark.parametrize(
    "change",
    ["omit", "extra", "duplicate-normalized", "extras", "below-floor", "relaxed-floor", "post-release"],
)
def test_minimum_profile_cannot_omit_or_silently_relax_declared_dependencies(profile_files, change):
    inventory, _, load = profile_files
    pins = inventory["profiles"]["minimum"]["pins"]
    if change == "omit":
        del pins["redis"]
    elif change == "extra":
        pins["unrelated"] = "1.0.0"
    elif change == "duplicate-normalized":
        pins["testcontainers"] = pins["testcontainers[kafka]"]
    elif change == "extras":
        pins["testcontainers"] = pins.pop("testcontainers[kafka]")
    elif change == "below-floor":
        pins["redis"] = "7.0.0"
    elif change == "post-release":
        pins["pytest"] = "8.3.0.post1"
    else:
        pins["redis"] = "7.2.0"
    with pytest.raises(ValueError):
        load()


def test_current_profile_must_match_the_checked_in_lock(profile_files):
    inventory, _, load = profile_files
    inventory["profiles"]["current"]["pins"]["redis"] = "8.0.0"
    with pytest.raises(ValueError, match="uv.lock"):
        load("current")


@pytest.mark.parametrize("dimension", ["python_versions", "platforms"])
def test_inventory_must_retain_the_claimed_matrix(profile_files, dimension):
    inventory, _, load = profile_files
    inventory[dimension].pop()
    with pytest.raises(ValueError):
        load()


@pytest.mark.parametrize("python", ["3.10", "3.15", "314"])
def test_unclaimed_interpreter_is_rejected(profile_files, python):
    _, _, load = profile_files
    with pytest.raises(ValueError):
        load(python=python)


def test_override_cannot_introduce_an_unreviewed_dependency(profile_files):
    inventory, _, load = profile_files
    inventory["profiles"]["minimum"]["python_overrides"]["3.14"]["unrelated"] = "1.0.0"
    with pytest.raises(ValueError):
        load()


def test_new_project_dependency_requires_updating_profiles(profile_files):
    _, project, load = profile_files
    project.write_text(project.read_text().replace('"redis>=7.1,<9"', '"redis>=7.1,<9", "new-runtime>=1,<2"'))
    with pytest.raises(ValueError):
        load()


def test_current_constraints_do_not_install_the_development_environment(profile_files):
    _, _, load = profile_files
    result = load("current")
    assert "pyright==1.1.414" in result["constraints"]
    assert not any(value.startswith("pyright==") for value in result["requirements"])
    assert not any(value.startswith("pytest-broker-observer==") for value in result["constraints"])


def test_profile_loading_does_not_mutate_inventory(profile_files):
    inventory, _, load = profile_files
    before = deepcopy(inventory)
    load(python="3.14")
    load(python="3.11")
    assert inventory == before
