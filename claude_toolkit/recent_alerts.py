"""Summarize arbitrage_executor log files.

Reads the three logs that execution_logger.py writes:
    arbitrage_executor/logs/execution_failures.log
    arbitrage_executor/logs/unmapped_markets.log
    arbitrage_executor/logs/execution_success.log

Prints counts per day for the last 7 days and the last 5 failure messages.
Read-only.

Usage:
    python claude_toolkit/recent_alerts.py
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter
from datetime import date, datetime, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_LOGS_DIR = os.path.join(_REPO_ROOT, "arbitrage_executor", "logs")

_LOG_FILES = [
    ("failures", "execution_failures.log"),
    ("unmapped", "unmapped_markets.log"),
    ("success", "execution_success.log"),
]

_SEPARATOR = "-" * 80
_HEADER_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})[\.\d:+-]*\]\s+(.*)$")


def _iter_entries(path: str):
    """Yield (header_line, body_lines) tuples from a log file.

    Entries are separated by a line of 80 dashes (see execution_logger.py).
    The first line of each entry is '[ISO_TIMESTAMP] MESSAGE'.
    """
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8", errors="replace") as f:
        buf: list[str] = []
        for raw in f:
            line = raw.rstrip("\n")
            if line.strip() == _SEPARATOR.strip():
                if buf:
                    header = buf[0]
                    body = buf[1:]
                    yield header, body
                buf = []
            else:
                buf.append(line)
        if buf:
            yield buf[0], buf[1:]


def _parse_header(header: str):
    m = _HEADER_RE.match(header)
    if not m:
        return None, header
    day_str, _time_str, message = m.group(1), m.group(2), m.group(3)
    try:
        day = datetime.strptime(day_str, "%Y-%m-%d").date()
    except ValueError:
        return None, header
    return day, message


def _summarize(path: str, window_days: int) -> tuple[Counter, list]:
    """Return (counts_by_day, all_headers_parsed) for the given log."""
    today = date.today()
    cutoff = today - timedelta(days=window_days - 1)
    counts: Counter = Counter()
    headers: list[tuple[date | None, str]] = []
    for header, _ in _iter_entries(path):
        day, message = _parse_header(header)
        headers.append((day, message))
        if day is not None and day >= cutoff:
            counts[day] += 1
    return counts, headers


def _print_day_table(title: str, counts: Counter, window_days: int) -> None:
    today = date.today()
    print(f"=== {title}: counts per day (last {window_days}) ===")
    total = 0
    for offset in range(window_days - 1, -1, -1):
        day = today - timedelta(days=offset)
        n = counts.get(day, 0)
        total += n
        print(f"  {day.isoformat()}  {n:>4}")
    print(f"  {'total':>10}  {total:>4}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--recent-failures", type=int, default=5,
                        help="Show the last N failure headers (default 5).")
    args = parser.parse_args()

    if not os.path.isdir(_LOGS_DIR):
        print(f"No logs directory at {_LOGS_DIR}.")
        return 0

    summaries: dict[str, tuple[Counter, list]] = {}
    for name, filename in _LOG_FILES:
        path = os.path.join(_LOGS_DIR, filename)
        if not os.path.isfile(path):
            print(f"(missing) {path}")
            continue
        summaries[name] = _summarize(path, args.window_days)

    if not summaries:
        return 0

    for name, _ in _LOG_FILES:
        if name not in summaries:
            continue
        counts, _ = summaries[name]
        _print_day_table(name, counts, args.window_days)
        print()

    print(f"=== Last {args.recent_failures} failure headers ===")
    if "failures" not in summaries:
        print("  (execution_failures.log not present)")
    else:
        _, headers = summaries["failures"]
        tail = headers[-args.recent_failures:] if headers else []
        if not tail:
            print("  (none)")
        for day, message in tail:
            day_str = day.isoformat() if day else "????-??-??"
            print(f"  {day_str}  {message[:110]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
