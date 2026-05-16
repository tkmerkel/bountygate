"""Single entry point all watchers use to report status to Postgres.

Reads DATABASE_URL from the environment (rewriting Heroku's postgres:// to
postgresql+psycopg2:// to satisfy SQLAlchemy 2.x). Upserts a single row per
watcher name. Callers pass current state; the dashboard renders it.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_engine: Optional[Engine] = None


def _get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = os.environ["DATABASE_URL"]
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg2://", 1)
        _engine = create_engine(url, pool_pre_ping=True)  # type: ignore[assignment]
    assert _engine is not None
    return _engine


def heartbeat(
    name: str,
    *,
    is_running: bool,
    expected_interval_s: int,
    pending_count: int = 0,
    oldest_pending_age_s: Optional[int] = None,
    completed_24h: int = 0,
    errors_24h: int = 0,
    last_error: Optional[str] = None,
) -> None:
    """Upsert one watcher_heartbeats row. Call on every loop tick + start/stop."""
    now = datetime.now(timezone.utc)
    with _get_engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO watcher_heartbeats (
                    name, is_running, last_tick_at, pending_count, oldest_pending_age_s,
                    completed_24h, errors_24h, last_error, expected_interval_s
                ) VALUES (
                    :name, :is_running, :now, :pending, :oldest, :done, :err, :last_err, :interval
                )
                ON CONFLICT (name) DO UPDATE SET
                    is_running = EXCLUDED.is_running,
                    last_tick_at = EXCLUDED.last_tick_at,
                    pending_count = EXCLUDED.pending_count,
                    oldest_pending_age_s = EXCLUDED.oldest_pending_age_s,
                    completed_24h = EXCLUDED.completed_24h,
                    errors_24h = EXCLUDED.errors_24h,
                    last_error = EXCLUDED.last_error,
                    expected_interval_s = EXCLUDED.expected_interval_s
                """
            ),
            {
                "name": name,
                "is_running": is_running,
                "now": now,
                "pending": pending_count,
                "oldest": oldest_pending_age_s,
                "done": completed_24h,
                "err": errors_24h,
                "last_err": last_error,
                "interval": expected_interval_s,
            },
        )
