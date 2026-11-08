"""Check public consumer inference against an isolated installed distribution."""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


def check(python, pyright, fixture):
    """Analyze a copied consumer and prove the checker detects an incorrect type."""
    source = fixture.read_text(encoding="utf-8")
    correct = "assert_type(raw.wait_for(matches, timeout=1), bytes | None)"
    if source.count(correct) != 1:
        raise ValueError("consumer negative-control target is missing or ambiguous")
    with tempfile.TemporaryDirectory(prefix="observer-consumer-") as directory:
        root = Path(directory)
        probe = subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                "import pathlib, sys, event_observer; "
                "assert pathlib.Path(event_observer.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix).resolve()); "
                "assert sys.prefix != sys.base_prefix",
            ],
            cwd=root,
            capture_output=True,
            timeout=30,
        )
        if probe.returncode:
            raise ValueError(
                "consumer target must import an installed package inside an isolated environment"
            )
        consumer = root / "consumer.py"
        config = root / "pyrightconfig.json"
        config.write_text(
            json.dumps(
                {
                    "include": ["consumer.py"],
                    "extraPaths": [],
                    "pythonVersion": "3.11",
                    "typeCheckingMode": "basic",
                }
            ),
            encoding="utf-8",
        )
        command = [str(pyright), "--project", str(config), "--pythonpath", str(python), "--outputjson"]
        results = []
        for text, expected in (
            (source, 0),
            (source.replace(correct, "assert_type(raw.wait_for(matches, timeout=1), int)"), 1),
        ):
            consumer.write_text(text, encoding="utf-8")
            run = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=120)
            data = json.loads(run.stdout)
            summary = data["summary"]
            if (
                summary["filesAnalyzed"] < 1
                or run.returncode != expected
                or summary["errorCount"] != expected
            ):
                raise ValueError("installed consumer typing or its negative control failed")
            if expected and not any(
                item.get("rule") == "reportAssertTypeFailure" for item in data["generalDiagnostics"]
            ):
                raise ValueError("negative control did not detect the intended type mismatch")
            results.append(summary)
        return {"consumer": results[0], "negative_control": results[1]}


def main():
    """Use a clean artifact interpreter, never the editable development interpreter."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--pyright", type=Path, default=Path(shutil.which("pyright") or "pyright"))
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "tests/typing/release_consumer.py",
    )
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = {"schema_version": 1, "valid": False, "errors": []}
    try:
        # Preserve venv interpreter symlinks: resolving one selects the base
        # interpreter and silently discards the isolated installation.
        report.update(check(args.python.absolute(), args.pyright.absolute(), args.fixture))
        report["valid"] = True
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired):
        report["errors"] = ["installed consumer analysis failed or produced an invalid result"]
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Installed consumer typing: " + ("verified" if report["valid"] else "invalid"))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
