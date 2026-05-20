"""
Executor-only DB helpers.

This module contains just the queries the arbitrage executor runs at runtime:
fetching opportunities, tracking executed ones, and driving the remote task
queue. Broader admin / ETL helpers (schema creation, bulk upserts, etc.) live
in ``app/shared/python/bountygate/utils/db_connection.py`` and are used by
Airflow DAGs and maintenance scripts — not by the executor.
"""

import os
import warnings
from typing import Any, Dict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

warnings.filterwarnings('ignore')


def _load_env_file_if_present() -> None:
    """Populate os.environ from every .env walking up from this file.

    Local .env (closer to this module) takes precedence — we populate via
    ``setdefault`` so an already-set key is never overwritten.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        env_path = os.path.join(here, ".env")
        if os.path.isfile(env_path):
            with open(env_path, encoding="utf-8") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())
        parent = os.path.dirname(here)
        if parent == here:
            return
        here = parent


_load_env_file_if_present()
DATABASE_URL = os.environ["DATABASE_URL"]

EXECUTED_OPPORTUNITIES_TABLE = "bg_executed_opportunities"
EXECUTION_QUEUE_TABLE = "bot_execution_queue"


def fetch_data(query):
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            res = conn.execute(text(query))
            rows = res.fetchall()
            cols = list(res.keys())
        df = pd.DataFrame(rows, columns=cols)
        if df.empty:
            print("No data returned from the query.")
        else:
            print(f"Fetched {len(df)} rows from the database.")
        return df
    finally:
        engine.dispose()


def ensure_executed_opportunities_table(table_name: str = EXECUTED_OPPORTUNITIES_TABLE) -> None:
    """Create a small bookkeeping table to record executed opportunities."""
    engine = create_engine(DATABASE_URL)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        opportunity_hash TEXT PRIMARY KEY,
        player_name TEXT,
        sport_title TEXT,
        home_team TEXT,
        away_team TEXT,
        market_key TEXT,
        line_value DECIMAL(12,4),
        over_bookmaker_key TEXT,
        under_bookmaker_key TEXT,
        roi DECIMAL(12,4),
        fetched_at_utc TIMESTAMPTZ,
        source_table TEXT,
        executed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
    except Exception as e:
        print(f"Error ensuring executed opportunities table: {e}")
    finally:
        engine.dispose()


def has_executed_opportunity(opportunity_hash: str, table_name: str = EXECUTED_OPPORTUNITIES_TABLE) -> bool:
    """Return True if the opportunity hash already exists in the tracking table."""
    ensure_executed_opportunities_table(table_name)
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT 1 FROM {table_name} WHERE opportunity_hash = :opportunity_hash LIMIT 1"),
                {"opportunity_hash": opportunity_hash},
            )
            return result.fetchone() is not None
    except Exception as e:
        print(f"Error checking executed opportunity: {e}")
        return False
    finally:
        engine.dispose()


def mark_opportunity_executed(
    opportunity_hash: str,
    opportunity_info: Dict[str, Any],
    table_name: str = EXECUTED_OPPORTUNITIES_TABLE,
) -> None:
    """Insert the executed opportunity into the tracking table (idempotent)."""
    ensure_executed_opportunities_table(table_name)
    engine = create_engine(DATABASE_URL)
    payload = {
        "opportunity_hash": opportunity_hash,
        "player_name": opportunity_info.get("player_name"),
        "sport_title": opportunity_info.get("sport_title"),
        "home_team": opportunity_info.get("home_team"),
        "away_team": opportunity_info.get("away_team"),
        "market_key": opportunity_info.get("market_key"),
        "line_value": opportunity_info.get("under_line") or opportunity_info.get("over_line"),
        "over_bookmaker_key": opportunity_info.get("over_bookmaker_key"),
        "under_bookmaker_key": opportunity_info.get("under_bookmaker_key"),
        "roi": opportunity_info.get("roi"),
        "fetched_at_utc": opportunity_info.get("fetched_at_utc"),
        "source_table": opportunity_info.get("source_table"),
    }
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {table_name} (
                        opportunity_hash,
                        player_name,
                        sport_title,
                        home_team,
                        away_team,
                        market_key,
                        line_value,
                        over_bookmaker_key,
                        under_bookmaker_key,
                        roi,
                        fetched_at_utc,
                        source_table
                    ) VALUES (
                        :opportunity_hash,
                        :player_name,
                        :sport_title,
                        :home_team,
                        :away_team,
                        :market_key,
                        :line_value,
                        :over_bookmaker_key,
                        :under_bookmaker_key,
                        :roi,
                        :fetched_at_utc,
                        :source_table
                    )
                    ON CONFLICT (opportunity_hash) DO NOTHING
                    """
                ),
                payload,
            )
    except Exception as e:
        print(f"Error marking executed opportunity: {e}")
    finally:
        engine.dispose()


def ensure_execution_queue_table(table_name: str = EXECUTION_QUEUE_TABLE) -> None:
    """Create the execution queue table for remote task dispatch."""
    engine = create_engine(DATABASE_URL)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'PENDING',
        error_log TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ
    );
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
    except Exception as e:
        print(f"Error ensuring execution queue table: {e}")
    finally:
        engine.dispose()


def pending_task_count(table_name: str = EXECUTION_QUEUE_TABLE) -> int:
    """Return the count of PENDING tasks in the execution queue."""
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name} WHERE status = 'PENDING'")
            ).fetchone()
            return int(row[0]) if row else 0
    except Exception as e:
        print(f"Error counting pending tasks: {e}")
        return 0
    finally:
        engine.dispose()


def cleanup_stale_pending(ttl_minutes: int = 15,
                          table_name: str = EXECUTION_QUEUE_TABLE) -> int:
    """Delete PENDING tasks older than ttl_minutes.

    Stale tasks accumulate when the DAG keeps inserting while the worker is
    offline. By the time the worker comes back, those rows correspond to
    long-gone opportunities (table snapshot has been replaced many times).
    Draining them costs Chrome time for nothing.
    """
    ensure_execution_queue_table(table_name)
    engine = create_engine(DATABASE_URL)
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(f"""
                    DELETE FROM {table_name}
                    WHERE status = 'PENDING'
                      AND created_at < NOW() - INTERVAL ':ttl minutes'
                """.replace(":ttl", str(int(ttl_minutes))))
            )
            return int(result.rowcount or 0)
    except Exception as e:
        print(f"Error cleaning stale PENDING tasks: {e}")
        return 0
    finally:
        engine.dispose()


def reap_orphaned_running(ttl_minutes: int = 5,
                          table_name: str = EXECUTION_QUEUE_TABLE) -> int:
    """Mark RUNNING tasks older than ttl_minutes as FAILED.

    A RUNNING row that's older than any plausible execution (~3-5 min in the
    worst case) is from a crashed worker — Chrome died, OS rebooted, etc.
    Without reaping, claim_pending_task will never pick them up again (they're
    not PENDING), and they accumulate forever.
    """
    ensure_execution_queue_table(table_name)
    engine = create_engine(DATABASE_URL)
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(f"""
                    UPDATE {table_name}
                    SET status = 'FAILED',
                        finished_at = NOW(),
                        error_log = COALESCE(error_log, '')
                                    || ' [reaped: RUNNING > '
                                    || ':ttl' || ' min on startup]'
                    WHERE status = 'RUNNING'
                      AND started_at < NOW() - INTERVAL ':ttl minutes'
                """.replace(":ttl", str(int(ttl_minutes))))
            )
            return int(result.rowcount or 0)
    except Exception as e:
        print(f"Error reaping orphaned RUNNING tasks: {e}")
        return 0
    finally:
        engine.dispose()


def claim_pending_task(table_name: str = EXECUTION_QUEUE_TABLE) -> int | None:
    """Atomically claim the oldest PENDING task. Returns task id or None."""
    ensure_execution_queue_table(table_name)
    engine = create_engine(DATABASE_URL)
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(f"""
                    UPDATE {table_name}
                    SET status = 'RUNNING', started_at = NOW()
                    WHERE id = (
                        SELECT id FROM {table_name}
                        WHERE status = 'PENDING'
                        ORDER BY created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING id
                """)
            ).fetchone()
            return row[0] if row else None
    except Exception as e:
        print(f"Error claiming pending task: {e}")
        return None
    finally:
        engine.dispose()


_VALID_TERMINAL_STATUSES = frozenset({"COMPLETED", "FAILED", "SKIPPED"})


def complete_task(task_id: int, status: str, error_msg: str | None = None,
                  table_name: str = EXECUTION_QUEUE_TABLE) -> None:
    """Mark a task with a terminal status.

    ``status`` must be one of:
      - ``COMPLETED`` — bet placed (success)
      - ``FAILED``    — real execution failure (bet attempted, error raised)
      - ``SKIPPED``   — no viable candidate; nothing to bet, not an error

    The earlier signature accepted ``success: bool``; that conflated SKIPPED
    with FAILED in the queue table and made queue stats misleading (today's
    25 ``FAILED`` rows were really 3 real failures + 22 no-viables).
    """
    if status not in _VALID_TERMINAL_STATUSES:
        raise ValueError(
            f"complete_task: status must be one of {sorted(_VALID_TERMINAL_STATUSES)}, "
            f"got {status!r}"
        )
    engine = create_engine(DATABASE_URL)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    UPDATE {table_name}
                    SET status = :status, finished_at = NOW(), error_log = :error_log
                    WHERE id = :task_id
                """),
                {"status": status, "error_log": error_msg, "task_id": task_id},
            )
    except Exception as e:
        print(f"Error completing task {task_id}: {e}")
    finally:
        engine.dispose()
