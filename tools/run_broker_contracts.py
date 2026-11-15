"""Run one pinned real-broker profile and preserve its contract evidence."""

from __future__ import annotations

import argparse
import base64
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory
import tomllib
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree

from .check_release_gates import (
    GateError,
    check_environment,
    check_junit,
    digest,
    distribution_hashes,
    inventory_checks,
    read_json,
    require,
    write_json,
)
from .compatibility_profiles import load_profile


ROOT = Path(__file__).resolve().parents[1]
PROFILE_FILE = ROOT / "tools/broker_profiles.json"
VARIABLES = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "RABBITMQ_URL",
    "RABBITMQ_MANAGEMENT_URL",
    "REDIS_URL",
    "EVENT_OBSERVER_TOXIPROXY_URL",
    "EVENT_OBSERVER_TOXIPROXY_PROXY",
    "PYTEST_ADDOPTS",
    "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
    "PYTEST_PLUGINS",
)


def profile_for(profile_id):
    """Resolve a reviewed image and dependency profile without a floating tag."""
    inventory = read_json(PROFILE_FILE)
    require(inventory["schema_version"] == 1, "Unsupported broker inventory")
    for broker, profiles in inventory["profiles"].items():
        for profile in profiles:
            if profile["id"] == profile_id:
                return broker, profile, inventory["toxiproxy"]
    raise GateError("Unknown broker profile")


def open_broker(broker, profile, proxy, output):
    """Import Docker and native clients only for explicitly selected integration runs."""
    if broker == "kafka":
        from tests.integration.containers.kafka import kafka_service

        return kafka_service(profile["image"], proxy["image"], log_dir=output)
    from tests.integration.containers.services import rabbitmq_service, redis_service

    service = rabbitmq_service if broker == "rabbitmq" else redis_service
    return service(profile["image"], log_dir=output)


def credentials(environment):
    """Collect synthetic credentials in the formats test diagnostics can include."""
    tokens = set()
    for name in ("RABBITMQ_URL", "RABBITMQ_MANAGEMENT_URL", "REDIS_URL"):
        parts = urlsplit(environment.get(name, ""))
        if parts.username:
            tokens.update((parts.username, unquote(parts.username)))
        if parts.password:
            password = unquote(parts.password)
            user = unquote(parts.username or "")
            tokens.update(
                (parts.password, password, base64.b64encode(f"{user}:{password}".encode()).decode())
            )
    return sorted(tokens, key=len, reverse=True)


def scrub(data, tokens):
    """Remove credentials before retaining logs or JUnit files."""
    for token in tokens:
        data = data.replace(token.encode(), b"[redacted]")
    return data


def run_pytest(selectors, service_env, output, name):
    """Use the real broker configured before collection and preserve the first result."""
    report = output / f"{name}.xml"
    environment = {key: value for key, value in os.environ.items() if key not in VARIABLES}
    environment.update(service_env)
    tokens = credentials(service_env)
    with TemporaryDirectory(prefix="pbo-broker-junit-") as temporary:
        raw_report = Path(temporary) / "results.xml"
        command = [
            sys.executable,
            "-I",
            "-m",
            "pytest",
            "-c",
            str(ROOT / "pyproject.toml"),
            *selectors,
            "-m",
            "integration",
            "-q",
            "--junitxml",
            str(raw_report),
            "-o",
            "junit_family=xunit1",
        ]
        output_bytes = b""
        try:
            outcome = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True, timeout=360)
            output_bytes = outcome.stdout + outcome.stderr
        except subprocess.TimeoutExpired as error:
            output_bytes = (error.stdout or b"") + (error.stderr or b"")
            raise GateError(f"{name} exceeded its time limit") from None
        finally:
            (output / f"{name}.log").write_bytes(scrub(output_bytes, tokens)[-64 * 1024 :])
            if raw_report.exists():
                require(raw_report.stat().st_size <= 16 * 1024 * 1024, "Broker JUnit exceeded its size limit")
                report.write_bytes(scrub(raw_report.read_bytes(), tokens))
    require(outcome.returncode == 0, f"{name} failed. See its first log and JUnit report.")
    return report


def contract_report(report, expected):
    """Require every registered case and reject unregistered or changed applicability."""
    counts = check_junit(report, expected)
    cases = list(ElementTree.parse(report).iter("testcase"))
    actual = {f"{case.get('classname')}::{case.get('name')}" for case in cases}
    require(
        actual == set(expected["required_cases"]), "Broker cases differ from the registered contract suite"
    )
    require(
        counts["not_applicable"] == len(expected["allowed_not_applicable"]),
        "A registered capability expectation changed",
    )
    return counts


def run_contracts(broker, profile, service_env, output, manifest):
    """Run complete async and blocking classes plus Kafka routing proof."""
    inventory = inventory_checks(read_json(ROOT / "docs/release-gates.json"))
    reports = {}
    for mode in ("async", "sync"):
        key = f"linux-{profile['id']}-{mode}"
        expected = inventory["broker-contracts", key][2]
        if mode == "async":
            selectors = [f"tests/integration/test_{broker.replace('-', '_')}_contracts.py"]
        else:
            parameter = "redis" if broker == "redis-streams" else broker
            selectors = [
                "tests/integration/test_sync_contracts.py::TestNativeBlockingAdapterContracts",
                "-k",
                parameter,
            ]
        report = run_pytest(selectors, service_env, output, mode)
        manifest["contracts"][mode] = contract_report(report, expected)
        reports[key] = report
    if broker == "kafka":
        report = run_pytest(["tests/integration/test_kafka_proxy_routing.py"], service_env, output, "proxy")
        test = "tests.integration.test_kafka_proxy_routing::test_advertised_kafka_route_cannot_bypass_disabled_proxy"
        manifest["proxy_cases"] = contract_report(
            report,
            {
                "required_cases": [f"{test}[{mode}]" for mode in ("async", "blocking")],
                "allowed_not_applicable": [],
            },
        )
    return reports


def check_image(configured, observed):
    """Match the configured index and any live platform manifest to the reviewed pins."""
    require(observed["image"] == configured["image"], "Unexpected broker image reference")
    variant = f"{observed['os']}/{observed['architecture']}"
    require(variant in configured["platform_manifests"], "Broker image architecture is outside the matrix")
    expected = configured["platform_manifests"][variant]
    actual = observed.get("manifest_digest")
    if actual:
        require(actual in {expected, configured["image"].split("@", 1)[1]}, "Broker manifest digest mismatch")
    if observed.get("version"):
        require(observed["version"] == configured["version"], "Broker server version mismatch")
    return expected


def checked_candidate(directory):
    """Bind the installed package to the wheel from this packaging job."""
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    hashes = distribution_hashes(directory / "dist", project["version"])
    wheel = directory / "dist" / next(name for name in hashes if name.endswith(".whl"))
    distribution = importlib.metadata.distribution(project["name"])
    location = Path(distribution.locate_file("event_observer/__init__.py")).resolve()
    require(
        distribution.version == project["version"] and location.is_relative_to(Path(sys.prefix).resolve()),
        "Broker tests must use the installed candidate wheel",
    )
    origin = json.loads(distribution.read_text("direct_url.json") or "{}")
    require(
        origin.get("url") == wheel.as_uri() and "archive_info" in origin, "Installed wheel origin mismatch"
    )
    archive_hash = origin["archive_info"].get("hashes", {}).get("sha256")
    if archive_hash:
        require(archive_hash == hashes[wheel.name], "Installed wheel hash mismatch")
    return project["version"], hashes


def installed_versions(profile):
    """Verify that native clients and the test harness use the selected exact pins."""
    python = f"{sys.version_info.major}.{sys.version_info.minor}"
    declared = load_profile(profile["dependencies"], python)["direct_versions"]
    versions = {name: importlib.metadata.version(name) for name in declared}
    require(versions == declared, "Installed dependencies differ from the broker profile")
    return versions


def publish_evidence(output, reports, manifest, version, hashes):
    """Write the RG-03 release-gate input from actual completed contract cases."""
    candidate = {
        "repository": os.environ["GITHUB_REPOSITORY"],
        "sha": os.environ["GITHUB_SHA"],
        "run_id": os.environ["GITHUB_RUN_ID"],
        "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
        "version": version,
        "tag": os.environ.get("RELEASE_TAG") or None,
    }
    environment = {
        "python": manifest["python"],
        "platform": manifest["platform"],
        "versions": manifest["versions"],
    }
    supporting = [output / "manifest.json"]
    if "proxy_cases" in manifest:
        supporting.append(output / "proxy.xml")

    def reference(path):
        return {"path": path.name, "sha256": digest(path)}

    checks = []
    for key, report in reports.items():
        check_environment(environment, key)
        checks.append(
            {
                "id": "broker-contracts",
                "profile": key,
                "environment": environment,
                "junit": reference(report),
                "artifacts": [reference(path) for path in supporting],
                "distributions": hashes,
            }
        )
    write_json(
        output / "evidence.json",
        {
            "schema_version": 1,
            "candidate": candidate,
            "producer": "broker-contracts",
            "checks": checks,
        },
    )


def interrupt(signum, frame):
    """Unwind owned containers when the runner is stopped."""
    raise KeyboardInterrupt


def main(argv=None):
    """Select one isolated profile and fail on incomplete contract evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--print-dependencies", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--candidate-dir", type=Path)
    parser.add_argument("--emit-release-evidence", action="store_true")
    args = parser.parse_args(argv)
    broker, profile, proxy = profile_for(args.profile)
    if args.print_dependencies:
        print(profile["dependencies"])
        return 0
    if args.output is None or args.emit_release_evidence and args.candidate_dir is None:
        parser.error("Provide --output and include --candidate-dir when emitting release evidence")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": 1,
        "profile": profile,
        "python": platform.python_version(),
        "platform": platform.system(),
        "contracts": {},
        "successful": False,
    }
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    signal.signal(signal.SIGTERM, interrupt)
    try:
        manifest["versions"] = installed_versions(profile)
        if args.candidate_dir is not None:
            candidate = args.candidate_dir.resolve()
            version, hashes = checked_candidate(candidate)
            manifest["distributions"] = hashes
        with open_broker(broker, profile, proxy, output) as service:
            manifest["service"] = service.metadata
            manifest["platform_manifest"] = check_image(profile, service.metadata)
            if broker == "kafka":
                manifest["proxy_manifest"] = check_image(proxy, service.metadata["toxiproxy"])
            reports = run_contracts(broker, profile, service.env, output, manifest)
        if args.candidate_dir is not None:
            require(
                checked_candidate(candidate) == (version, hashes), "Candidate changed during broker tests"
            )
        manifest["successful"] = True
        write_json(output / "manifest.json", manifest)
        if args.emit_release_evidence:
            publish_evidence(output, reports, manifest, version, hashes)
    except (Exception, KeyboardInterrupt) as error:
        manifest["successful"] = False
        manifest["error"] = str(error) if isinstance(error, GateError) else type(error).__name__
        write_json(output / "manifest.json", manifest)
        print(f"Broker profile {profile['id']} failed: {manifest['error']}", file=sys.stderr)
        return 1
    print(f"Broker profile {profile['id']} passed: {json.dumps(manifest['contracts'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
