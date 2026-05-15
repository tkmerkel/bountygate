"""Drift detector: compare a fresh trace's derived config to stored YAML.

Used to catch selector drift before the next live bet. Run periodically (or
on a cron) against a fresh recording session — if the bookmaker has changed
an aria-label, accordion text, or selector pattern, the diff fires.

Exit codes:
    0  identical (after dropping volatile fields)
    1  drift detected (one or more fields differ)
    2  trace or stored config is unreadable / invalid

CLI:
    python -m toolkit.codegen.drift --trace path/to/trace.jsonl
    python -m toolkit.codegen.drift --trace ... --json   # machine-readable
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from typing import Any

from toolkit.codegen import betmgm as betmgm_codegen
from toolkit.codegen import fanduel as fanduel_codegen
from toolkit.recorder.schema import load_trace

REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
EXECUTOR_DIR = os.path.join(REPO_ROOT, "arbitrage_executor")


@contextlib.contextmanager
def _executor_cwd():
    """SelectorManager resolves YAML paths relative to CWD as
    selectors/{site}_markets.yaml. Make those reads work from any caller."""
    prev = os.getcwd()
    try:
        os.chdir(EXECUTOR_DIR)
        yield
    finally:
        os.chdir(prev)

VOLATILE_FIELDS = frozenset({"validated_at"})


class DriftError(Exception):
    pass


def _diff_dicts(fresh: dict, stored: dict) -> list[dict]:
    """Compare two configs and return a list of field-level diffs.

    Each diff entry: {"field": str, "fresh": Any, "stored": Any, "kind": str}
    where kind in {"changed", "missing_in_fresh", "missing_in_stored"}.
    """
    diffs: list[dict] = []
    keys = set(fresh) | set(stored)
    for key in sorted(keys):
        if key in VOLATILE_FIELDS:
            continue
        if key not in fresh:
            diffs.append(
                {"field": key, "fresh": None, "stored": stored[key], "kind": "missing_in_fresh"}
            )
            continue
        if key not in stored:
            diffs.append(
                {"field": key, "fresh": fresh[key], "stored": None, "kind": "missing_in_stored"}
            )
            continue
        if fresh[key] != stored[key]:
            diffs.append(
                {"field": key, "fresh": fresh[key], "stored": stored[key], "kind": "changed"}
            )
    return diffs


def detect_drift(trace_path: str) -> tuple[str, str, list[dict]]:
    """Compare codegen(trace) vs SelectorManager.get_market(book, market).

    Returns (book, market_key, diffs). diffs is empty on no drift.
    """
    from arbitrage_executor.selector_finder import SelectorManager

    header, records = load_trace(trace_path)
    if header.book == "fanduel":
        market_key, fresh = fanduel_codegen.trace_to_config(header, records)
    elif header.book == "betmgm":
        market_key, fresh = betmgm_codegen.trace_to_config(header, records)
    else:
        raise DriftError(f"unknown book in trace: {header.book!r}")

    with _executor_cwd():
        stored = SelectorManager.get_market(header.book, market_key)
    if not stored:
        return header.book, market_key, [
            {
                "field": "<entire-config>",
                "fresh": fresh,
                "stored": None,
                "kind": "missing_in_stored",
            }
        ]

    return header.book, market_key, _diff_dicts(fresh, stored)


def _format_text(book: str, market_key: str, diffs: list[dict]) -> str:
    if not diffs:
        return f"no drift: {book}/{market_key}"
    lines = [f"DRIFT in {book}/{market_key} ({len(diffs)} field(s)):"]
    for d in diffs:
        lines.append(f"  [{d['kind']}] {d['field']}")
        lines.append(f"    fresh : {d['fresh']!r}")
        lines.append(f"    stored: {d['stored']!r}")
    return "\n".join(lines)


def main(argv: Any = None) -> int:
    p = argparse.ArgumentParser(
        prog="drift",
        description="Compare a fresh recorder trace to the stored selector YAML.",
    )
    p.add_argument("--trace", required=True, help="path to a recorder JSONL trace")
    p.add_argument("--json", action="store_true",
                   help="emit JSON instead of human-readable text")
    args = p.parse_args(argv)

    try:
        book, market_key, diffs = detect_drift(args.trace)
    except (DriftError, FileNotFoundError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(
            {"book": book, "market": market_key, "diffs": diffs},
            indent=2, default=str,
        ))
    else:
        print(_format_text(book, market_key, diffs))
    return 0 if not diffs else 1


if __name__ == "__main__":
    sys.exit(main())
