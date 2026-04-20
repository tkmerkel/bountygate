"""Interactively rescue tasks stuck in RUNNING.

Finds rows in bot_execution_queue with status='RUNNING' and started_at older
than --threshold-minutes (default 30). For each, prints the row and prompts
y/N before resetting to PENDING. There is NO non-interactive reset mode --
resetting a task that's actually still executing would cause double-execution
(same opportunity placed twice), and that risk dominates convenience.

Usage:
    python claude_toolkit/rescue_stuck_tasks.py
    python claude_toolkit/rescue_stuck_tasks.py --threshold-minutes 10
    python claude_toolkit/rescue_stuck_tasks.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_EXECUTOR = os.path.join(_REPO_ROOT, "arbitrage_executor")
if _EXECUTOR not in sys.path:
    sys.path.insert(0, _EXECUTOR)

import db_connection  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402


def _find_stuck(conn, threshold_minutes: int) -> list:
    return conn.execute(text(
        f"""
        SELECT id, status, created_at, started_at,
               EXTRACT(EPOCH FROM (NOW() - started_at)) / 60.0 AS age_minutes,
               error_log
        FROM {db_connection.EXECUTION_QUEUE_TABLE}
        WHERE status = 'RUNNING'
          AND started_at < NOW() - (:mins || ' minutes')::interval
        ORDER BY started_at
        """
    ), {"mins": str(threshold_minutes)}).fetchall()


def _reset_to_pending(conn, task_id: int) -> None:
    conn.execute(text(
        f"""
        UPDATE {db_connection.EXECUTION_QUEUE_TABLE}
        SET status = 'PENDING',
            started_at = NULL,
            finished_at = NULL,
            error_log = COALESCE(error_log || ' | ', '')
                      || 'reset from RUNNING by claude_toolkit/rescue_stuck_tasks.py'
        WHERE id = :task_id AND status = 'RUNNING'
        """
    ), {"task_id": task_id})


def _describe(row) -> str:
    age = f"{float(row.age_minutes):.1f}m" if row.age_minutes is not None else "?"
    started = str(row.started_at) if row.started_at else "?"
    return (
        f"  id={row.id}  status={row.status}  age={age}  started_at={started}\n"
        f"    error_log: {row.error_log or '(none)'}"
    )


def _prompt_yn(prompt: str) -> bool:
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        return False
    return answer in ("y", "yes")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--threshold-minutes", type=int, default=30,
                        help="Rows with started_at older than this are candidates (default 30).")
    parser.add_argument("--dry-run", action="store_true",
                        help="List candidates; never prompt, never write.")
    args = parser.parse_args()

    engine = create_engine(db_connection.DATABASE_URL)
    try:
        with engine.connect() as conn:
            stuck = _find_stuck(conn, args.threshold_minutes)
    except Exception as e:
        print(f"Query failed: {type(e).__name__}: {e}", file=sys.stderr)
        engine.dispose()
        return 2

    if not stuck:
        print(f"No RUNNING tasks older than {args.threshold_minutes} minutes.")
        engine.dispose()
        return 0

    print(f"Found {len(stuck)} RUNNING task(s) older than {args.threshold_minutes}m:")
    print()
    for row in stuck:
        print(_describe(row))
        print()

    if args.dry_run:
        print("--dry-run -- no changes made.")
        engine.dispose()
        return 0

    print("WARNING: resetting a task that is still actively executing can cause")
    print("double-placement of the same opportunity. Only confirm if you are")
    print("certain the worker process for this task is no longer running.")
    print()

    reset_count = 0
    for row in stuck:
        if _prompt_yn(f"Reset task id={row.id} to PENDING? [y/N]: "):
            try:
                with engine.begin() as conn:
                    _reset_to_pending(conn, row.id)
                print(f"  -> id={row.id} reset to PENDING.")
                reset_count += 1
            except Exception as e:
                print(f"  ! id={row.id} reset failed: {type(e).__name__}: {e}")
        else:
            print(f"  -> id={row.id} left as-is.")

    engine.dispose()
    print()
    print(f"Done. {reset_count}/{len(stuck)} reset.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
