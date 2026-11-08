"""Check the owner's release identity against both candidate distributions.

This stdlib-only check validates a local packaging property, not release readiness
or remote ownership of a PyPI name. It never extracts archive members.
"""

import argparse
from collections import Counter
from email.parser import BytesParser
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tomllib
from zipfile import BadZipFile, ZipFile


MIT = """MIT License

Copyright (c) 2026 Valerii Korobov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
NAME = "pytest-broker-observer"


def require(condition, message):
    """Reject an invalid property using only a fixed, non-sensitive diagnostic."""
    if not condition:
        raise ValueError(message)


def project_identity(data):
    """Validate the approved identity and absence of mandatory dependencies."""
    project = tomllib.loads(data.decode("utf-8"))["project"]
    require(project.get("name") == NAME, "source distribution name differs from the approved name")
    version = project.get("version")
    require(
        isinstance(version, str) and re.fullmatch(r"[0-9]+(?:\.[0-9]+)*(?:(?:a|b|rc)[0-9]+)?", version),
        "source version must be an explicit supported release version",
    )
    require(project.get("license") == "MIT", "source SPDX license must be MIT")
    require(project.get("license-files") == ["LICENSE"], "source must declare exactly LICENSE")
    require(
        project.get("authors") == [{"name": "Valerii Korobov"}],
        "source author differs from the approved owner",
    )
    require(project.get("dependencies", []) == [], "Core must not declare mandatory dependencies")
    require(not project.get("dynamic"), "release identity must use static project metadata")
    return project


def safe_name(name):
    """Accept only normalized relative archive names."""
    parts = PurePosixPath(name).parts
    require(
        bool(parts)
        and not name.startswith("/")
        and "\\" not in name
        and all(part not in (".", "..") for part in name.rstrip("/").split("/")),
        "archive contains an unsafe path",
    )


def archive_files(path):
    """Read unique regular members, rejecting links and unsafe paths."""
    files = {}
    seen = set()

    def add(name, content):
        safe_name(name)
        require(name not in seen, "archive contains duplicate members")
        seen.add(name)
        if content is not None:
            files[name] = content

    if path.suffix == ".whl":
        with ZipFile(path) as archive:
            for member in archive.infolist():
                mode = member.external_attr >> 16
                require(not stat.S_ISLNK(mode), "wheel contains a linked member")
                require(
                    not stat.S_IFMT(mode) or stat.S_ISREG(mode) or stat.S_ISDIR(mode),
                    "wheel contains a special member",
                )
                add(member.filename, None if member.is_dir() else archive.read(member))
    else:
        with tarfile.open(path, "r:gz") as archive:
            for member in archive.getmembers():
                require(member.isfile() or member.isdir(), "sdist contains a linked or special member")
                if member.isfile():
                    stream = archive.extractfile(member)
                    require(stream is not None, "sdist member cannot be read")
                    add(member.name, stream.read())
                else:
                    add(member.name, None)
    return files


def check_metadata(data, project):
    """Require unique identity fields and strictly extra-guarded dependencies."""
    metadata = BytesParser().parsebytes(data)
    for field, value in {
        "Name": NAME,
        "Version": project["version"],
        "Author": "Valerii Korobov",
        "License-Expression": "MIT",
        "License-File": "LICENSE",
    }.items():
        require(
            metadata.get_all(field, []) == [value],
            "archive metadata identity is missing, duplicated, or inconsistent",
        )
    require(
        Counter(metadata.get_all("Project-URL", []))
        == Counter(f"{key}, {value}" for key, value in project.get("urls", {}).items()),
        "archive project URLs differ from source declarations",
    )
    extras = metadata.get_all("Provides-Extra", [])
    expected_extras = {
        re.sub(r"[-_.]+", "-", name).lower() for name in project.get("optional-dependencies", {})
    }
    require(
        len(extras) == len(expected_extras) and set(extras) == expected_extras,
        "archive extras differ from source declarations",
    )
    python = metadata.get_all("Requires-Python", [])
    require(
        len(python) == 1
        and nonempty_python(project.get("requires-python"))
        and sorted(python[0].replace(" ", "").split(","))
        == sorted(project["requires-python"].replace(" ", "").split(",")),
        "archive Python range differs from source declarations",
    )
    expected_requirements = Counter(
        (canonical_requirement(requirement), re.sub(r"[-_.]+", "-", extra).lower())
        for extra, requirements in project.get("optional-dependencies", {}).items()
        for requirement in requirements
    )
    actual_requirements = Counter()
    for dependency in metadata.get_all("Requires-Dist", []):
        # The package uses this simple marker profile. Reject other forms rather
        # than guessing whether arbitrary PEP 508 expressions imply an extra.
        marker = dependency.partition(";")[2].strip()
        match = re.fullmatch(r"extra\s*==\s*(['\"])([a-z0-9-]+)\1", marker)
        require(
            match is not None and match.group(2) in expected_extras,
            "archive declares an unguarded or unsupported dependency marker",
        )
        actual_requirements[(canonical_requirement(dependency.partition(";")[0]), match.group(2))] += 1
    require(
        actual_requirements == expected_requirements, "archive dependencies differ from source declarations"
    )


def nonempty_python(value):
    """Require the explicit supported Python range in source metadata."""
    return isinstance(value, str) and value.replace(" ", "") == ">=3.11,<3.15"


def canonical_requirement(value):
    """Normalize the project's bounded name/extras/version requirement grammar."""
    match = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[([A-Za-z0-9,._-]+)\])?(.*)", value.strip())
    require(match is not None, "dependency uses an unsupported requirement form")
    name, extras, constraint = match.groups()
    specs = constraint.replace(" ", "").split(",") if constraint.strip() else []
    require(
        all(re.fullmatch(r"(?:~=|==|!=|<=|>=|<|>)[0-9][A-Za-z0-9.*+!-]*", spec) for spec in specs),
        "dependency uses an unsupported version constraint",
    )

    def normalize(text):
        return re.sub(r"[-_.]+", "-", text).lower()

    return (
        normalize(name),
        tuple(sorted(normalize(extra) for extra in extras.split(","))) if extras else (),
        tuple(sorted(specs)),
    )


def check(project_path, license_path, dist_dir):
    """Validate source and artifacts and return the checked artifact hashes."""
    require(
        not project_path.is_symlink() and not license_path.is_symlink(),
        "source identity files must not be links",
    )
    project = project_identity(project_path.read_bytes())
    license_bytes = license_path.read_bytes()
    require(
        license_bytes.decode("utf-8").strip() == MIT.strip(), "source license differs from approved MIT terms"
    )
    version = project["version"]
    stem = f"pytest_broker_observer-{version}"
    expected = {f"{stem}-py3-none-any.whl", f"{stem}.tar.gz"}
    # uv build creates this housekeeping file before writing distributions.
    paths = []
    for path in dist_dir.iterdir():
        if path.name == ".gitignore" and not path.is_symlink() and path.read_bytes() in (b"*", b"*\n"):
            continue
        paths.append(path)
    require(
        {path.name for path in paths} == expected
        and all(path.is_file() and not path.is_symlink() for path in paths),
        "candidate directory must contain exactly the expected wheel and sdist",
    )
    hashes = {}
    for path in sorted(paths):
        files = archive_files(path)
        wheel = path.suffix == ".whl"
        prefix = f"{stem}.dist-info" if wheel else stem
        metadata_path = f"{prefix}/METADATA" if wheel else f"{prefix}/PKG-INFO"
        license_member = f"{prefix}/licenses/LICENSE" if wheel else f"{prefix}/LICENSE"
        marker = "event_observer/py.typed" if wheel else f"{prefix}/src/event_observer/py.typed"
        require(
            metadata_path in files and files.get(license_member) == license_bytes,
            "archive identity or license contents differ from source",
        )
        require(marker in files, "archive is missing the public typing marker")
        check_metadata(files[metadata_path], project)
        if not wheel:
            require(all(name.startswith(stem + "/") for name in files), "sdist contains an unexpected root")
            require(
                project_identity(files[f"{stem}/pyproject.toml"]) == project,
                "sdist project metadata differs from source",
            )
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return version, hashes


def main():
    """Run the local identity check without echoing untrusted input data."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--license", type=Path, default=Path("LICENSE"))
    parser.add_argument("--dist-dir", type=Path, default=Path("dist"))
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = {
        "schema_version": 1,
        "ready": False,
        "errors": [],
        "distribution": NAME,
        "version": None,
        "artifacts": {},
    }
    try:
        report["version"], report["artifacts"] = check(args.project, args.license, args.dist_dir)
        report["ready"] = True
    except ValueError as error:
        # Only explicitly raised messages are safe; parsing failures may contain
        # user-controlled metadata, so use a generic message for their subclasses.
        report["errors"] = [str(error) if type(error) is ValueError else "invalid source or archive metadata"]
    except (OSError, KeyError, TypeError, BadZipFile, tarfile.TarError):
        report["errors"] = ["source or candidate archives could not be validated"]
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Release identity: " + ("verified" if report["ready"] else "invalid"))
    for error in report["errors"]:
        print(error)
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
