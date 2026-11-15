"""Run one compatibility cell against unchanged candidate distribution files.

The coordinating interpreter needs uv and Pyright only. Target environments are
created independently, never synchronized from the editable development project.
Reports describe actual execution; they are not release-gate attestations.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

from compatibility_profiles import load_profile


ROOT = Path(__file__).resolve().parents[1]
EXTRAS = ("kafka", "rabbitmq", "redis", "protobuf", "pytest-asyncio", "allure", "contract-tests")


def verify_artifacts(directory):
    """Verify exactly the two distributions against the packaging job's manifest."""
    dist = directory / "dist"
    files = []
    for path in sorted(dist.iterdir()):
        if (
            path.name == ".gitignore"
            and not path.is_symlink()
            and path.is_file()
            and path.read_bytes() in (b"*", b"*\n")
        ):
            continue
        files.append(path)
    if (
        len(files) != 2
        or any(path.is_symlink() or not path.is_file() for path in files)
        or sum(path.name.endswith("-py3-none-any.whl") for path in files) != 1
        or sum(path.name.endswith(".tar.gz") for path in files) != 1
    ):
        raise ValueError("candidate must contain exactly the expected wheel and sdist")
    expected = {}
    for line in (directory / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([a-f0-9]{64})  dist/([^/\\]+)", line)
        if match is None or match.group(2) in expected:
            raise ValueError("invalid candidate checksum manifest")
        expected[match.group(2)] = match.group(1)
    actual = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    if expected != actual:
        raise ValueError("candidate distribution checksums differ")
    return files, actual


def require_complete_junit(path):
    """Reject empty, skipped, failed, or retried mandatory check results."""
    tree = ET.parse(path)
    cases = list(tree.iter("testcase"))
    bad = {"failure", "error", "skipped", "rerunFailure", "rerunError", "flakyFailure", "flakyError"}
    # Pytest can emit an empty classname for a copied standalone test module.
    # Its file/name identity is still valid JUnit and must remain distinguishable.
    identities = [(case.get("classname") or case.get("file") or "", case.get("name")) for case in cases]
    if (
        not cases
        or len(set(identities)) != len(cases)
        or any(not identity[1] for identity in identities)
        or any(node.tag in bad for node in tree.iter())
    ):
        raise ValueError("mandatory JUnit evidence contains an unexecuted or unsuccessful case")
    for suite in tree.iter():
        if suite.tag not in {"testsuite", "testsuites"}:
            continue
        for field in ("errors", "failures", "skipped"):
            if suite.get(field, "0") != "0":
                raise ValueError("mandatory JUnit counters contain unsuccessful cases")
        if "tests" in suite.attrib and suite.get("tests") != str(sum(1 for _ in suite.iter("testcase"))):
            raise ValueError("mandatory JUnit case count differs from its suite counter")
    return len(cases)


class Cell:
    """Own bounded command logs and actual environments for one matrix cell."""

    def __init__(self, profile, output, workspace, uv, pyright):
        self.profile, self.output, self.workspace = profile, output, workspace
        self.uv, self.pyright = uv, pyright
        self.report = {
            "schema_version": 1,
            "successful": False,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "candidate": {
                name: os.environ.get(variable)
                for name, variable in {
                    "repository": "GITHUB_REPOSITORY",
                    "source_sha": "GITHUB_SHA",
                    "run_id": "GITHUB_RUN_ID",
                    "run_attempt": "GITHUB_RUN_ATTEMPT",
                }.items()
            },
            "environment": {
                "python": platform.python_version(),
                "implementation": platform.python_implementation(),
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "runner_os": os.environ.get("RUNNER_OS"),
                "runner_arch": os.environ.get("RUNNER_ARCH"),
                "runner_image": os.environ.get("ImageOS"),
                "runner_image_version": os.environ.get("ImageVersion"),
            },
            "declared_profile": profile,
            "commands": [],
            "environments": {},
            "junit": {},
            "errors": [],
        }

    def save(self):
        """Keep partial evidence after the first failing command, without a rerun."""
        (self.output / "manifest.json").write_text(json.dumps(self.report, indent=2) + "\n", encoding="utf-8")

    def run(self, label, command, cwd=ROOT, timeout=600):
        """Run a bounded command and retain its exit code and output on failure."""
        print(f"Compatibility: {label}", flush=True)
        started = time.monotonic()
        environment = dict(os.environ)
        for key in (
            "PYTHONPATH",
            "PYTHONHOME",
            "VIRTUAL_ENV",
            "PYTEST_ADDOPTS",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
            "PYTEST_PLUGINS",
            "KAFKA_BOOTSTRAP_SERVERS",
            "RABBITMQ_URL",
            "RABBITMQ_MANAGEMENT_URL",
            "REDIS_URL",
        ):
            environment.pop(key, None)
        record = {"id": label, "command": [str(value) for value in command], "returncode": None}
        self.report["commands"].append(record)
        try:
            result = subprocess.run(
                record["command"], cwd=cwd, env=environment, capture_output=True, text=True, timeout=timeout
            )
        except subprocess.TimeoutExpired as error:
            record["timed_out"] = True

            def decode(value):
                return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value or ""

            log = self.output / f"{label}.log"
            log.write_text((decode(error.stdout) + decode(error.stderr))[-200_000:], encoding="utf-8")
            record["log"] = log.name
            raise ValueError(f"{label}: command exceeded its time limit") from None
        finally:
            record["duration_seconds"] = time.monotonic() - started
            self.save()
        record["returncode"] = result.returncode
        log = self.output / f"{label}.log"
        log.write_text((result.stdout + result.stderr)[-200_000:], encoding="utf-8")
        record["log"] = log.name
        self.save()
        if result.returncode:
            print((result.stdout + result.stderr)[-6000:], flush=True)
            raise ValueError(f"{label}: command failed; see its recorded first output")
        return result.stdout

    def create(self, name, artifact, extra=None):
        """Create a fresh target and install only the requested artifact feature."""
        directory = self.workspace / name
        self.run(name + "-create", [self.uv, "venv", "--no-config", "--python", sys.executable, directory])
        python = directory / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        reference = f"pytest-broker-observer[{extra}] @ {artifact.as_uri()}" if extra else str(artifact)
        command = [
            self.uv,
            "pip",
            "install",
            "--no-config",
            "--python",
            python,
            "--constraint",
            self.output / "requirements.txt",
            "--constraint",
            self.output / "constraints.txt",
            "--only-binary",
            "confluent-kafka",
            reference,
        ]
        self.run(name + "-install", command)
        return python

    def versions(self, name, python):
        """Record actual package versions and installation origin from the target."""
        text = self.run(
            name + "-versions",
            [
                python,
                "-I",
                "-c",
                "import importlib.metadata as m, json, sys, event_observer; "
                "print(json.dumps({'python':sys.version,'prefix':sys.prefix,'package':event_observer.__file__,"
                "'packages':{d.metadata['Name']:d.version for d in m.distributions()}}))",
            ],
        )
        self.report["environments"][name] = json.loads(text)
        self.save()

    def smoke(self, name, python, mode, extra=None):
        """Run the installed-origin checks and require every declared case to pass."""
        junit = self.output / f"{name}.xml"
        command = [
            python,
            "-I",
            ROOT / "tools/check_compatibility_install.py",
            "--mode",
            mode,
            "--report",
            self.output / f"{name}.json",
            "--junitxml",
            junit,
        ]
        if extra:
            command += ["--extra", extra]
        self.run(name, command)
        self.report["junit"][junit.name] = {
            "cases": require_complete_junit(junit),
            "sha256": hashlib.sha256(junit.read_bytes()).hexdigest(),
        }
        self.save()

    def pytest(self, name, python, arguments):
        """Use the installed package for selected unit, parity, and example nodes."""
        junit = self.output / f"{name}.xml"
        self.run(
            name,
            [
                python,
                "-I",
                "-m",
                "pytest",
                "-c",
                ROOT / "pyproject.toml",
                *arguments,
                "-W",
                "error::RuntimeWarning",
                "-W",
                "error::pytest.PytestUnraisableExceptionWarning",
                "--junitxml",
                junit,
                "-o",
                "junit_family=xunit1",
                "-q",
            ],
            timeout=900,
        )
        self.report["junit"][junit.name] = {
            "cases": require_complete_junit(junit),
            "sha256": hashlib.sha256(junit.read_bytes()).hexdigest(),
        }
        self.save()

    def execute(self, artifacts):
        """Exercise both package forms and each implemented optional feature."""
        for artifact in artifacts:
            kind = "wheel" if artifact.suffix == ".whl" else "sdist"
            minimal = self.create(kind + "-minimal", artifact)
            self.smoke(kind + "-stdlib", minimal, "stdlib")
            self.run(
                kind + "-pytest-install",
                [
                    self.uv,
                    "pip",
                    "install",
                    "--no-config",
                    "--python",
                    minimal,
                    "--constraint",
                    self.output / "constraints.txt",
                    "--constraint",
                    self.output / "requirements.txt",
                    f"pytest-broker-observer[pytest] @ {artifact.as_uri()}",
                ],
            )
            self.smoke(kind + "-pytest-only", minimal, "pytest-only")
            self.versions(kind + "-minimal", minimal)
            full = self.create(kind + "-full", artifact, "all")
            self.run(
                kind + "-test-stack",
                [
                    self.uv,
                    "pip",
                    "install",
                    "--no-config",
                    "--python",
                    full,
                    "--requirement",
                    self.output / "requirements.txt",
                    "--constraint",
                    self.output / "constraints.txt",
                    "--only-binary",
                    "confluent-kafka",
                ],
            )
            self.run(kind + "-dependency-check", [self.uv, "pip", "check", "--python", full])
            self.versions(kind + "-full", full)
            actual = {
                re.sub(r"[-_.]+", "-", key).lower(): value
                for key, value in self.report["environments"][kind + "-full"]["packages"].items()
            }
            if any(actual.get(name) != version for name, version in self.profile["direct_versions"].items()):
                raise ValueError(
                    "installed direct dependency versions differ from the selected exact profile"
                )
            self.smoke(kind + "-native", full, "native")
            self.run(
                kind + "-typing",
                [
                    sys.executable,
                    ROOT / "tools/check_installed_typing.py",
                    "--python",
                    full,
                    "--pyright",
                    self.pyright,
                    "--report",
                    self.output / f"{kind}-typing.json",
                ],
            )
            if kind == "wheel":
                self.pytest("unit", full, ["tests", "-m", "not integration"])
                self.pytest("unit-xdist", full, ["tests", "-m", "not integration", "-n", "2"])
            else:
                self.pytest(
                    "sdist-parity",
                    full,
                    [
                        "tests/test_sync_api.py",
                        "tests/test_protobuf_runtime.py",
                        "tests/test_observation_marks.py",
                        "tests/compatibility/test_protobuf_generated.py",
                    ],
                )
            self.pytest(
                kind + "-examples",
                full,
                [
                    "examples/pytest",
                    "examples/allure",
                    "-m",
                    "not integration",
                    "-k",
                    "not test_intentional_timeout_failure",
                    "--alluredir",
                    self.output / f"{kind}-allure-results",
                ],
            )
        wheel = next(path for path in artifacts if path.suffix == ".whl")
        for extra in EXTRAS:
            python = self.create("extra-" + extra, wheel, extra)
            self.smoke("extra-" + extra, python, "optional", extra)
            self.versions("extra-" + extra, python)


def main():
    """Validate the selected runner identity and preserve success or first failure."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("minimum", "current"), required=True)
    parser.add_argument("--expected-python", required=True)
    parser.add_argument("--expected-platform", choices=("linux", "macos", "windows"), required=True)
    parser.add_argument("--expected-architecture", choices=("x64", "arm64"), required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--uv", default=shutil.which("uv"))
    parser.add_argument("--pyright", default=shutil.which("pyright"))
    args = parser.parse_args()
    output = args.output.absolute()
    output.mkdir(parents=True, exist_ok=True)
    cell = None
    try:
        actual_platform = {"Linux": "linux", "Darwin": "macos", "Windows": "windows"}.get(platform.system())
        actual_architecture = {"x86_64": "x64", "amd64": "x64", "arm64": "arm64", "aarch64": "arm64"}.get(
            platform.machine().lower()
        )
        if (
            f"{sys.version_info.major}.{sys.version_info.minor}" != args.expected_python
            or actual_platform != args.expected_platform
            or actual_architecture != args.expected_architecture
            or not args.uv
            or not args.pyright
        ):
            raise ValueError("runner interpreter, platform, uv, or Pyright does not match the requested cell")
        profile = load_profile(args.profile, args.expected_python)
        (output / "requirements.txt").write_text("\n".join(profile["requirements"]) + "\n", encoding="utf-8")
        (output / "constraints.txt").write_text("\n".join(profile["constraints"]) + "\n", encoding="utf-8")
        with tempfile.TemporaryDirectory(prefix="observer-compatibility-") as workspace:
            cell = Cell(profile, output, Path(workspace), args.uv, args.pyright)
            cell.save()
            artifacts, hashes = verify_artifacts(args.artifacts.absolute())
            cell.report["candidate"]["artifacts"] = hashes
            cell.report["tooling"] = {
                "uv": cell.run("uv-version", [args.uv, "--version"]).strip(),
                "pyright": cell.run("pyright-version", [args.pyright, "--version"]).strip(),
            }
            cell.execute(artifacts)
            if hashes != verify_artifacts(args.artifacts.absolute())[1]:
                raise ValueError("candidate bytes changed during compatibility execution")
            cell.report["successful"] = True
    except (OSError, ValueError, KeyError, TypeError, ET.ParseError) as error:
        message = (
            str(error)
            if type(error) is ValueError
            else "compatibility inputs or execution could not be validated"
        )
        if cell:
            cell.report["errors"].append(message)
        else:
            (output / "manifest.json").write_text(
                json.dumps({"schema_version": 1, "successful": False, "errors": [message]}, indent=2) + "\n",
                encoding="utf-8",
            )
        print(message, file=sys.stderr)
    finally:
        if cell:
            cell.report["finished_at"] = datetime.now(timezone.utc).isoformat()
            cell.save()
    return 0 if cell and cell.report["successful"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
