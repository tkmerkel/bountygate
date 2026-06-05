#!/usr/bin/env python3
"""Static pre-scan of Airflow DAG files into structured skeletons.

Grounds the inventory workflow's per-DAG agents in real symbols (dag_id,
schedule, imports, referenced tables) so they classify from facts, not guesses.
"""
from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

# Table references inside SQL string literals.
_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)",
    re.IGNORECASE,
)
# pandas .to_sql("table", ...)
_TOSQL_RE = re.compile(r"to_sql\(\s*[\"']([a-zA-Z_][a-zA-Z0-9_]*)[\"']")


def _literal_strings(tree: ast.AST) -> list[str]:
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]


def _find_dag_id(tree: ast.AST) -> str | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "id", getattr(func, "attr", ""))
            if name in {"DAG", "dag"}:
                for kw in node.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        return str(kw.value.value)
                if node.args and isinstance(node.args[0], ast.Constant):
                    return str(node.args[0].value)
    return None


def _find_schedule(tree: ast.AST) -> str | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg in {"schedule", "schedule_interval"} and isinstance(
                    kw.value, ast.Constant
                ):
                    return str(kw.value.value)
    return None


def _find_imports(tree: ast.AST) -> list[str]:
    mods: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                mods.add(a.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.add(node.module)
    return sorted(mods)


def _find_tables(strings: list[str]) -> list[str]:
    tables: set[str] = set()
    for s in strings:
        for m in _TABLE_RE.findall(s):
            tables.add(m.split(".")[-1])
        for m in _TOSQL_RE.findall(s):
            tables.add(m)
    return sorted(tables)


def scan_file(path: Path, repo: str) -> dict:
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    return {
        "file": str(path),
        "repo": repo,
        "dag_id": _find_dag_id(tree),
        "schedule": _find_schedule(tree),
        "imports": _find_imports(tree),
        "tables": _find_tables(_literal_strings(tree)),
    }


def scan_dir(dag_dir: Path, repo: str) -> list[dict]:
    records = []
    for path in sorted(Path(dag_dir).glob("*.py")):
        if path.name.startswith("test_") or path.name == "__init__.py":
            continue
        try:
            records.append(scan_file(path, repo))
        except SyntaxError as exc:
            records.append({"file": str(path), "repo": repo, "error": str(exc)})
    return records


def main(argv: list[str]) -> int:
    # argv entries: "<repo_label>=<dag_dir>"
    records: list[dict] = []
    for arg in argv:
        repo, _, dag_dir = arg.partition("=")
        records.extend(scan_dir(Path(dag_dir), repo))
    json.dump(records, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
