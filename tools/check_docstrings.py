"""Check named source definitions and their explicitly declared attributes."""

from __future__ import annotations

import ast
from pathlib import Path
import re
import sys

DEFINITIONS = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
IVAR = re.compile(r":ivar\s+(?:[^:\n]*\s)?([A-Za-z_]\w*)\s*:")
PARAM = re.compile(r":param\s+(?:[^:\n]*\s)?\*{0,2}([A-Za-z_]\w*)\s*:")


def assignment_names(target: ast.expr, *, instance: str | None = None) -> set[str]:
    """Return explicitly assigned fields, unpacking tuple and list targets.

    :param target: Assignment target to inspect without executing source code.
    :param instance: Instance parameter name, or None for class-level assignments.
    :returns: Field names owned by the selected class or instance.
    """
    if isinstance(target, (ast.Tuple, ast.List)):
        return set().union(*(assignment_names(item, instance=instance) for item in target.elts))
    if instance is None and isinstance(target, ast.Name):
        return {target.id}
    if (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == instance
    ):
        return {target.attr}
    return set()


def assigned_fields(node: ast.AST, instance: str | None = None) -> set[str]:
    """Find declared fields while keeping nested class definitions separate.

    :param node: Class statement or instance-method subtree to inspect.
    :param instance: Instance parameter name, or None for class fields.
    :returns: Explicitly declared field names, excluding peer-object mutations.
    """
    fields: set[str] = set()
    if isinstance(node, ast.ClassDef):
        return fields
    if isinstance(node, ast.Assign):
        for target in node.targets:
            fields.update(assignment_names(target, instance=instance))
    elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        fields.update(assignment_names(node.target, instance=instance))
    for child in ast.iter_child_nodes(node):
        fields.update(assigned_fields(child, instance))
    return fields


def class_fields(node: ast.ClassDef) -> set[str]:
    """Collect class constants, explicit instance state, and properties.

    :param node: Class definition, including dataclasses, protocols, and enums.
    :returns: Own field names requiring Sphinx ivar documentation.
    """
    fields: set[str] = set()
    for statement in node.body:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            decorators = {ast.unparse(item) for item in statement.decorator_list}
            if "property" in decorators:
                fields.add(statement.name)
            if not {"classmethod", "staticmethod"}.intersection(decorators):
                positional = statement.args.posonlyargs + statement.args.args
                if positional:
                    fields.update(assigned_fields(statement, positional[0].arg))
        else:
            fields.update(assigned_fields(statement))
    return fields


def audit(path: Path) -> tuple[list[str], int, int]:
    """Audit every named definition without importing optional dependencies.

    :param path: Python source file to parse.
    :returns: Failures, number of definitions, and number of own class fields.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    failures: list[str] = []
    definitions = attributes = 0
    for node in ast.walk(tree):
        if not isinstance(node, DEFINITIONS):
            continue
        definitions += 1
        location = f"{path}:{node.lineno}: {node.name}"
        doc = ast.get_docstring(node)
        if not doc:
            failures.append(f"{location}: missing docstring")
            doc = ""
        if isinstance(node, ast.ClassDef):
            fields = class_fields(node)
            attributes += len(fields)
            missing = fields - set(IVAR.findall(doc))
            if missing:
                failures.append(f"{location}: missing :ivar: for {', '.join(sorted(missing))}")
        else:
            args = node.args
            params = {arg.arg for arg in args.posonlyargs + args.args + args.kwonlyargs}
            params.difference_update({"self", "cls"})
            params.update(arg.arg for arg in (args.vararg, args.kwarg) if arg is not None)
            missing = params - set(PARAM.findall(doc))
            if missing:
                failures.append(f"{location}: missing :param: for {', '.join(sorted(missing))}")
    return failures, definitions, attributes


def main() -> int:
    """Check all packaged Python source and print a concise coverage result.

    :returns: Zero for complete coverage, or one when documentation is missing.
    """
    root = Path(__file__).resolve().parents[1]
    failures: list[str] = []
    definitions = attributes = 0
    for path in sorted((root / "src").rglob("*.py")):
        issues, count, fields = audit(path)
        failures.extend(issues)
        definitions += count
        attributes += fields
    for failure in failures:
        print(failure)
    if failures:
        print(f"Source documentation: {len(failures)} failures")
        return 1
    print(f"Source documentation: {definitions} definitions and {attributes} attributes documented")
    return 0


if __name__ == "__main__":
    sys.exit(main())
