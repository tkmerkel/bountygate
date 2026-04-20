"""Summarize the most recent arbitrage_executor/audit_logs entries.

Each execution attempt creates a directory named:
    audit_logs/{timestamp}_{player}_{market}/
containing opportunity_info.json + screenshots.

This script lists the most recent N, showing timestamp, player, market,
and the outcome inferred from screenshot filenames (success / error / unknown).
Read-only.

Usage:
    python claude_toolkit/tail_audit.py
    python claude_toolkit/tail_audit.py -n 20
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_AUDIT_DIR = os.path.join(_REPO_ROOT, "arbitrage_executor", "audit_logs")


def _infer_outcome(audit_path: str) -> str:
    """Classify an audit directory from its screenshot filenames."""
    if not os.path.isdir(audit_path):
        return "missing"
    try:
        names = os.listdir(audit_path)
    except OSError:
        return "unreadable"
    lowered = [n.lower() for n in names]
    joined = " ".join(lowered)
    if "orphan" in joined:
        return "ORPHANED"
    if "critical" in joined:
        return "critical"
    if any("error" in n or "fail" in n for n in lowered):
        return "error"
    if any("success" in n or "confirm" in n or "placed" in n for n in lowered):
        return "success"
    return "unknown"


def _parse_dir_name(name: str) -> tuple[str, str, str]:
    """audit dir names look like '20260124_162659_Evan_Mobley_player_rebounds'."""
    parts = name.split("_")
    if len(parts) < 3:
        return name, "", ""
    ts = f"{parts[0]}_{parts[1]}"
    # Market key is the last underscore-joined slug of the trailing tokens. We
    # don't know the exact player/market boundary without the JSON, so fall
    # back to "rest" and optionally refine from opportunity_info.json below.
    rest = "_".join(parts[2:])
    return ts, rest, ""


def _from_opportunity_info(audit_path: str) -> dict:
    info_path = os.path.join(audit_path, "opportunity_info.json")
    if not os.path.isfile(info_path):
        return {}
    try:
        with open(info_path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-n", "--limit", type=int, default=10,
                        help="Number of recent entries to show (default 10).")
    args = parser.parse_args()

    if not os.path.isdir(_AUDIT_DIR):
        print(f"No audit_logs directory at {_AUDIT_DIR}.")
        return 0

    entries = [
        name for name in os.listdir(_AUDIT_DIR)
        if os.path.isdir(os.path.join(_AUDIT_DIR, name))
    ]
    if not entries:
        print("No audit entries.")
        return 0

    entries.sort(reverse=True)
    entries = entries[: args.limit]

    print(f"{'timestamp':>15}  {'outcome':>8}  player / market")
    print(f"{'-' * 15}  {'-' * 8}  {'-' * 40}")

    for name in entries:
        path = os.path.join(_AUDIT_DIR, name)
        ts, rest, _ = _parse_dir_name(name)
        info = _from_opportunity_info(path)
        player = info.get("player_name") or ""
        market = (
            info.get("market_key")
            or info.get("over_market_key")
            or info.get("under_market_key")
            or ""
        )
        if player or market:
            label = f"{player} / {market}".strip(" /")
        else:
            label = rest
        outcome = _infer_outcome(path)
        print(f"{ts:>15}  {outcome:>8}  {label}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
