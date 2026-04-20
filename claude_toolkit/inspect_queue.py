"""Read-only snapshot of bot_execution_queue.

Shows counts by status, any tasks stuck in RUNNING (with age), the last 10
FAILED (with error excerpt), and the last 10 COMPLETED. No writes.

Usage:
    python claude_toolkit/inspect_queue.py
    python claude_toolkit/inspect_queue.py --stuck-threshold-minutes 10
"""

from __future__ import annotations

import argparse
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_EXECUTOR = os.path.join(_REPO_ROOT, "arbitrage_executor")
if _EXECUTOR not in sys.path:
    sys.path.insert(0, _EXECUTOR)

import db_connection  # noqa: E402 -- ensures .env bootstrap and DATABASE_URL
from sqlalchemy import create_engine, text  # noqa: E402


def _engine():
    return create_engine(db_connection.DATABASE_URL)


def _status_counts(conn) -> dict:
    rows = conn.execute(text(
        f"SELECT status, COUNT(*) FROM {db_connection.EXECUTION_QUEUE_TABLE} "
        "GROUP BY status ORDER BY status"
    )).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def _stuck_running(conn, threshold_minutes: int) -> list:
    rows = conn.execute(text(
        f"""
        SELECT id, status, started_at,
               EXTRACT(EPOCH FROM (NOW() - started_at)) / 60.0 AS age_minutes,
               error_log
        FROM {db_connection.EXECUTION_QUEUE_TABLE}
        WHERE status = 'RUNNING'
          AND started_at < NOW() - (:mins || ' minutes')::interval
        ORDER BY started_at
        """
    ), {"mins": str(threshold_minutes)}).fetchall()
    return rows


def _recent(conn, status: str, limit: int = 10) -> list:
    rows = conn.execute(text(
        f"""
        SELECT id, status, created_at, started_at, finished_at, error_log
        FROM {db_connection.EXECUTION_QUEUE_TABLE}
        WHERE status = :status
        ORDER BY COALESCE(finished_at, started_at, created_at) DESC
        LIMIT :limit
        """
    ), {"status": status, "limit": limit}).fetchall()
    return rows


def _truncate(value, max_len: int = 120) -> str:
    if value is None:
        return ""
    text_val = str(value).replace("\n", " / ")
    if len(text_val) > max_len:
        return text_val[: max_len - 1] + "..."
    return text_val


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stuck-threshold-minutes", type=int, default=30)
    args = parser.parse_args()

    engine = _engine()
    try:
        with engine.connect() as conn:
            counts = _status_counts(conn)
            stuck = _stuck_running(conn, args.stuck_threshold_minutes)
            failed = _recent(conn, "FAILED")
            completed = _recent(conn, "COMPLETED")
    finally:
        engine.dispose()

    print("=== Status counts ===")
    if not counts:
        print("  (queue is empty)")
    else:
        for status, n in counts.items():
            print(f"  {status:10s}  {n}")

    print()
    print(f"=== Stuck RUNNING (older than {args.stuck_threshold_minutes}m) ===")
    if not stuck:
        print("  (none)")
    else:
        print(f"  {'id':>6}  {'age_min':>9}  started_at                      error_log")
        for row in stuck:
            age = f"{float(row.age_minutes):.1f}" if row.age_minutes is not None else "?"
            started = str(row.started_at) if row.started_at else ""
            print(f"  {row.id:>6}  {age:>9}  {started:31s}  {_truncate(row.error_log, 80)}")
        print()
        print("  -> Run `python claude_toolkit/rescue_stuck_tasks.py --dry-run` to inspect.")

    print()
    print("=== Last 10 FAILED ===")
    if not failed:
        print("  (none)")
    else:
        print(f"  {'id':>6}  finished_at                    error_log")
        for row in failed:
            finished = str(row.finished_at) if row.finished_at else ""
            print(f"  {row.id:>6}  {finished:29s}  {_truncate(row.error_log, 110)}")

    print()
    print("=== Last 10 COMPLETED ===")
    if not completed:
        print("  (none)")
    else:
        print(f"  {'id':>6}  finished_at")
        for row in completed:
            finished = str(row.finished_at) if row.finished_at else ""
            print(f"  {row.id:>6}  {finished}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
