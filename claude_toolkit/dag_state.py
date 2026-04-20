"""Airflow DAG run states from the logs filesystem.

Reads airflow/logs/dag_id=<NAME>/run_id=<TYPE>__<ISO-TIMESTAMP>_<suffix>/
directories and reports the last run timestamp plus the number of runs in
the last 24h per DAG. No Airflow or Postgres required -- pure filesystem.

This is a proxy for "is the analytics pipeline alive?" If a DAG that
should run every 5 minutes has no runs in the last 24h, something is wrong.
Silence by the executor (no Discord alerts, no new PENDING tasks) is
ambiguous; this gives the other half of the picture.

Usage:
    python claude_toolkit/dag_state.py
    python claude_toolkit/dag_state.py --window-hours 6
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_AIRFLOW_LOGS = os.path.join(_REPO_ROOT, "airflow", "logs")

# run_id=<TYPE>__<ISO8601 timestamp with tz>[_<suffix>]
# Windows note: NTFS forbids ':' in filenames, so when the Airflow docker
# container's log volume is mounted on Windows the colons in the timestamp
# get swapped for the Unicode private-use character U+F03A. We normalize
# before matching and parsing.
_WINDOWS_COLON_SUBSTITUTE = "\uf03a"
_RUN_ID_RE = re.compile(r"^run_id=([a-zA-Z_]+)__([0-9T\-:\.+]+)(?:_(.+))?$")


def _parse_run_timestamp(dirname: str) -> Optional[datetime]:
    normalized = dirname.replace(_WINDOWS_COLON_SUBSTITUTE, ":")
    m = _RUN_ID_RE.match(normalized)
    if not m:
        return None
    ts_raw = m.group(2)
    try:
        return datetime.fromisoformat(ts_raw)
    except ValueError:
        return None


def _iter_dag_dirs() -> Iterable[tuple[str, str]]:
    if not os.path.isdir(_AIRFLOW_LOGS):
        return
    for name in sorted(os.listdir(_AIRFLOW_LOGS)):
        if not name.startswith("dag_id="):
            continue
        dag_id = name.removeprefix("dag_id=")
        path = os.path.join(_AIRFLOW_LOGS, name)
        if os.path.isdir(path):
            yield dag_id, path


def _collect_runs(dag_path: str) -> list[datetime]:
    try:
        entries = os.listdir(dag_path)
    except OSError:
        return []
    runs: list[datetime] = []
    for entry in entries:
        if not entry.startswith("run_id="):
            continue
        ts = _parse_run_timestamp(entry)
        if ts is not None:
            runs.append(ts)
    runs.sort()
    return runs


def _humanize_ago(ts: datetime) -> str:
    now = datetime.now(tz=timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = now - ts
    secs = int(delta.total_seconds())
    if secs < 0:
        return "in the future?"
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--window-hours", type=int, default=24,
                        help="Count runs with start timestamp within the last N hours (default 24).")
    args = parser.parse_args()

    if not os.path.isdir(_AIRFLOW_LOGS):
        print(f"No airflow logs directory at {_AIRFLOW_LOGS}.")
        print("If Airflow is running via docker-compose, either the container")
        print("hasn't been started yet or the log volume is mapped elsewhere.")
        return 0

    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=args.window_hours)

    rows: list[tuple[str, Optional[datetime], int, int]] = []
    for dag_id, path in _iter_dag_dirs():
        runs = _collect_runs(path)
        total = len(runs)
        in_window = sum(1 for r in runs if (r if r.tzinfo else r.replace(tzinfo=timezone.utc)) >= cutoff)
        last = runs[-1] if runs else None
        rows.append((dag_id, last, total, in_window))

    if not rows:
        print("No DAG run logs found.")
        return 0

    name_w = max(len(r[0]) for r in rows)
    print(f"{'dag_id'.ljust(name_w)}  {'last run':>22}  ago       {f'runs/{args.window_hours}h':>10}  total")
    print(f"{'-' * name_w}  {'-' * 22}  --------  {'-' * 10}  -----")
    for dag_id, last, total, in_window in rows:
        if last is None:
            last_str = "(none)"
            ago = ""
        else:
            last_str = last.isoformat(timespec="seconds")
            ago = _humanize_ago(last)
        print(f"{dag_id.ljust(name_w)}  {last_str:>22}  {ago:<8}  {in_window:>10}  {total}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
