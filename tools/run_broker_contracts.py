"""Run one pinned real-broker contract or fault suite and retain its evidence."""

from __future__ import annotations

import argparse
import asyncio
import base64
from contextlib import suppress
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
from time import monotonic
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
    "EVENT_OBSERVER_FAULTS",
    "EVENT_OBSERVER_FAULT_CONTAINER_ID",
    "EVENT_OBSERVER_FAULT_CONTAINER_TOKEN",
    "EVENT_OBSERVER_FAULT_PASSWORD",
    "EVENT_OBSERVER_KAFKA_ADMIN_PASSWORD",
    "EVENT_OBSERVER_KAFKA_READER_PASSWORD",
    "EVENT_OBSERVER_XDIST_SOURCE",
    "EVENT_OBSERVER_XDIST_COORDINATION",
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


def open_broker(broker, profile, proxy, output, *, faults=False, secure=False):
    """Import Docker and native clients only for explicitly selected integration runs."""
    if broker == "kafka":
        from tests.integration.containers.kafka import kafka_service

        return kafka_service(profile["image"], proxy["image"], log_dir=output, faults=faults, secure=secure)
    from tests.integration.containers.services import rabbitmq_service, redis_service

    service = rabbitmq_service if broker == "rabbitmq" else redis_service
    return service(profile["image"], log_dir=output, faults=faults)


def credentials(environment):
    """Collect synthetic credentials in the formats test diagnostics can include."""
    tokens = {
        environment[name]
        for name in (
            "EVENT_OBSERVER_FAULT_PASSWORD",
            "EVENT_OBSERVER_KAFKA_ADMIN_PASSWORD",
            "EVENT_OBSERVER_KAFKA_READER_PASSWORD",
        )
        if environment.get(name)
    }
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


def _run_worker_group(command, environment, cancelled):
    """Keep xdist workers in the owned POSIX group and reap it before fixture cleanup."""
    deadline = monotonic() + 360
    with subprocess.Popen(
        command,
        cwd=ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    ) as process:
        try:
            while True:
                try:
                    stdout, stderr = process.communicate(timeout=0.25)
                except subprocess.TimeoutExpired:
                    if cancelled.is_set() or monotonic() >= deadline:
                        raise
                else:
                    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        except BaseException:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)
            raise


def run_pytest(selectors, service_env, output, name, *, cancelled=None, strict_warnings=False):
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
        if strict_warnings:
            command.extend(
                (
                    "-o",
                    "pythonpath=.",
                    "-Werror::RuntimeWarning",
                    "-Werror::pytest.PytestUnraisableExceptionWarning",
                )
            )
        output_bytes = b""
        try:
            if cancelled is None:
                outcome = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True, timeout=360)
            else:
                outcome = _run_worker_group(command, environment, cancelled)
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


def fault_report(broker, category, selectors, service_env, output, **options):
    """Validate exact fault node IDs and require every recorded scenario to pass."""
    inventory = read_json(ROOT / "tools/broker_fault_cases.json")
    report = run_pytest(selectors, service_env, output, category, strict_warnings=True, **options)
    return contract_report(
        report,
        {"required_cases": inventory[broker][category], "allowed_not_applicable": []},
    )


def run_native_isolation(broker, service_env, output):
    """Provision one owned source for two simultaneously active pytest workers."""
    from tests.integration.native_lifecycle import make_harness, wait_for_workers

    async def run(directory):
        harness = make_harness(broker, service_env)
        try:
            source = await harness.create_source(case_id="xdist-shared-source")
            environment = {
                **service_env,
                "EVENT_OBSERVER_XDIST_SOURCE": source.name,
                "EVENT_OBSERVER_XDIST_COORDINATION": str(directory),
            }
            selectors = [
                "tests/integration/test_native_multiprocess.py",
                "-n",
                "2",
                "--dist",
                "load",
            ]
            cancelled = threading.Event()
            run_workers = asyncio.create_task(
                asyncio.to_thread(
                    fault_report, broker, "xdist", selectors, environment, output, cancelled=cancelled
                )
            )
            try:
                ready = await wait_for_workers(directory, run_workers)
                if ready:
                    write_json(output / "xdist-ready.json", ready)
                    receipt = await harness.publish(source, b"shared-source-publication")
                    require(receipt.accepted, "Broker did not confirm the shared-source publication")
                outcomes = await asyncio.shield(run_workers)
                require(ready, "Both native readers did not report READY")
                return outcomes
            finally:
                if not run_workers.done():
                    cancelled.set()
                    with suppress(GateError):
                        await run_workers
        finally:
            await harness.cleanup()

    with TemporaryDirectory(prefix="pbo-broker-workers-") as temporary:
        return asyncio.run(run(Path(temporary)))


def run_faults(broker, service_env, output, manifest):
    """Run broker failures and independent-worker checks on the owned instance."""
    test_module = f"tests/integration/test_{broker.replace('-', '_')}_native_faults.py"
    selectors = [test_module]
    if broker == "kafka":
        selectors.append("tests/integration/test_kafka_proxy_faults.py")
    manifest["faults"]["native"] = fault_report(broker, "native", selectors, service_env, output)
    manifest["faults"]["lifecycle"] = fault_report(
        broker, "lifecycle", ["tests/integration/test_native_lifecycle.py"], service_env, output
    )
    manifest["faults"]["xdist"] = run_native_isolation(broker, service_env, output)


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
    parser.add_argument("--suite", choices=("contracts", "faults"), default="contracts")
    args = parser.parse_args(argv)
    broker, profile, proxy = profile_for(args.profile)
    if args.print_dependencies:
        print(profile["dependencies"])
        return 0
    if args.output is None or args.emit_release_evidence and args.candidate_dir is None:
        parser.error("Provide --output and include --candidate-dir when emitting release evidence")
    if args.emit_release_evidence and args.suite != "contracts":
        parser.error("Only the contract suite emits RG-03 evidence")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": 1,
        "suite": args.suite,
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
        faults = args.suite == "faults"
        if faults:
            manifest["candidate"] = {
                "sha": os.environ.get("GITHUB_SHA"),
                "run_id": os.environ.get("GITHUB_RUN_ID"),
                "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
            }
        with open_broker(broker, profile, proxy, output, faults=faults) as service:
            manifest["service"] = service.metadata
            manifest["platform_manifest"] = check_image(profile, service.metadata)
            if broker == "kafka":
                manifest["proxy_manifest"] = check_image(proxy, service.metadata["toxiproxy"])
            if faults:
                manifest["faults"] = {}
                run_faults(broker, {**service.env, "EVENT_OBSERVER_FAULTS": broker}, output, manifest)
            else:
                reports = run_contracts(broker, profile, service.env, output, manifest)
        if faults and broker == "kafka":
            auth_output = output / "authentication"
            auth_output.mkdir()
            with open_broker(broker, profile, proxy, auth_output, secure=True) as service:
                manifest["authentication_service"] = service.metadata
                check_image(profile, service.metadata)
                check_image(proxy, service.metadata["toxiproxy"])
                manifest["faults"]["auth"] = fault_report(
                    broker,
                    "auth",
                    ["tests/integration/test_kafka_auth_faults.py"],
                    service.env,
                    auth_output,
                )
        if args.candidate_dir is not None:
            require(
                checked_candidate(candidate) == (version, hashes), "Candidate changed during broker tests"
            )
        manifest["successful"] = True
        if args.emit_release_evidence:
            write_json(output / "manifest.json", manifest)
            publish_evidence(output, reports, manifest, version, hashes)
    except (Exception, KeyboardInterrupt) as error:
        manifest["successful"] = False
        manifest["error"] = str(error) if isinstance(error, GateError) else type(error).__name__
        print(f"Broker profile {profile['id']} failed: {manifest['error']}", file=sys.stderr)
        return 1
    finally:
        if args.suite == "faults":
            manifest["reports"] = {
                path.relative_to(output).as_posix(): digest(path)
                for path in sorted((*output.glob("*.xml"), *output.glob("authentication/*.xml")))
            }
            if (output / "xdist-ready.json").is_file():
                manifest["xdist_ready_sha256"] = digest(output / "xdist-ready.json")
        write_json(output / "manifest.json", manifest)
    print(f"Broker profile {profile['id']} {args.suite} passed: {json.dumps(manifest[args.suite])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
