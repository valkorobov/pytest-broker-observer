"""Regenerate one checked-in fixture with an explicitly pinned protoc toolchain.

Run this script using the isolated generator interpreter described in the fixture
README. It never installs dependencies or changes the tested package runtime.
"""

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
GENERATORS = {"v526": "1.64.1", "v528": "1.68.1", "current": "1.85.1"}


def main():
    """Compile a source file and record exact tools, command, and content hashes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("generation", choices=GENERATORS)
    args = parser.parse_args()
    version = importlib.metadata.version("grpcio-tools")
    if version != GENERATORS[args.generation]:
        raise SystemExit("The isolated generator version does not match the fixture pin")
    directory = ROOT / "tests/compatibility/protobuf"
    source = f"observer_{args.generation}.proto"
    command = ["-m", "grpc_tools.protoc", "--proto_path=.", "--python_out=.", source]
    subprocess.run([sys.executable, *command], cwd=directory, check=True)
    protoc = subprocess.check_output(
        [sys.executable, "-m", "grpc_tools.protoc", "--version"], text=True
    ).strip()
    target = source.removesuffix(".proto") + "_pb2.py"
    generated = (directory / target).read_bytes()
    gencode = re.search(rb"# Protobuf Python Version: ([0-9.]+)", generated)
    if gencode is None:
        raise SystemExit("The generated file does not declare its Protobuf generation version")
    report = {
        "schema_version": 1,
        "generation": args.generation,
        "generator": {
            "distribution": "grpcio-tools",
            "version": version,
            "protoc": protoc,
            "python": platform.python_version(),
            "platform": platform.system(),
            "machine": platform.machine(),
        },
        "generation_runtime": importlib.metadata.version("protobuf"),
        "gencode_version": gencode.group(1).decode("ascii"),
        "command": ["python", *command],
        "working_directory": "tests/compatibility/protobuf",
        "source": {"file": source, "sha256": hashlib.sha256((directory / source).read_bytes()).hexdigest()},
        "generated": {"file": target, "sha256": hashlib.sha256(generated).hexdigest()},
    }
    (directory / f"{args.generation}.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Generated {args.generation} with {protoc}")


if __name__ == "__main__":
    main()
