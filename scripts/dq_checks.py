#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from sqlalchemy import create_engine, text
import json


def _add_shared_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shared = repo_root / "app" / "shared" / "python"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))


_add_shared_to_path()
from bountygate.utils import db_connection as dbc  # type: ignore  # noqa: E402


def url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


def emit(conn, metric_name: str, value, dimensions: Dict | None = None, notes: str | None = None) -> None:
    conn.execute(
        text(
            "INSERT INTO dq_metrics(metric_name, metric_value, dimensions, notes)\n"
            "VALUES (:name, :val, CAST(:dims AS jsonb), :notes)"
        ),
        {
            "name": metric_name,
            "val": value,
            "dims": json.dumps(dimensions or {}),
            "notes": notes,
        },
    )


def main() -> int:
    engine = create_engine(url())
    try:
        with engine.begin() as conn:
            # Totals and null checks
            r = conn.execute(
                text(
                    """
                    SELECT COUNT(*) AS total,
                           SUM((commence_at_utc IS NULL)::int) AS miss_time,
                           SUM((home_team_id IS NULL)::int) AS miss_home,
                           SUM((away_team_id IS NULL)::int) AS miss_away
                    FROM bg_unified_lines
                    """
                )
            ).first()
            total, miss_time, miss_home, miss_away = r
            emit(conn, "bg_unified_lines.total", total)
            emit(conn, "bg_unified_lines.missing.commence_at_utc", miss_time)
            emit(conn, "bg_unified_lines.missing.home_team_id", miss_home)
            emit(conn, "bg_unified_lines.missing.away_team_id", miss_away)

            # Time format consistency by bookmaker/sport (diagnostic)
            rows = conn.execute(
                text(
                    """
                    SELECT bookmaker_key, sport_key,
                           COUNT(*) AS cnt,
                           SUM(CASE WHEN commence_time ~ 'T' THEN 1 ELSE 0 END) AS has_T,
                           SUM(CASE WHEN commence_time ~ 'Z$' THEN 1 ELSE 0 END) AS ends_Z,
                           SUM(CASE WHEN commence_time ~ '\\+\\d{2}:?\\d{2}$' THEN 1 ELSE 0 END) AS has_tz_offset
                    FROM bg_unified_lines
                    GROUP BY 1,2
                    """
                )
            )
            for bk, sk, cnt, has_t, ends_z, has_off in rows:
                dims = {"bookmaker": bk, "sport": sk}
                emit(conn, "bg_unified_lines.timefmt.count", cnt, dims)
                emit(conn, "bg_unified_lines.timefmt.has_T", has_t, dims)
                emit(conn, "bg_unified_lines.timefmt.ends_Z", ends_z, dims)
                emit(conn, "bg_unified_lines.timefmt.has_tz_offset", has_off, dims)

            # Unmapped alias examples (top N per sport)
            unmapped = conn.execute(
                text(
                    """
                    SELECT sport_key, name, SUM(c) AS cnt FROM (
                      SELECT sport_key, home_team AS name, COUNT(*) c
                        FROM bg_unified_lines WHERE home_team_id IS NULL AND COALESCE(home_team,'')<>'' GROUP BY 1,2
                      UNION ALL
                      SELECT sport_key, away_team AS name, COUNT(*) c
                        FROM bg_unified_lines WHERE away_team_id IS NULL AND COALESCE(away_team,'')<>'' GROUP BY 1,2
                    ) a GROUP BY sport_key, name ORDER BY cnt DESC LIMIT 20
                    """
                )
            )
            for sk, name, cnt in unmapped:
                emit(conn, "bg_unified_lines.unmapped.alias", cnt, {"sport": sk, "name": name})
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
