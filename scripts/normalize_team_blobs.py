#!/usr/bin/env python3
from __future__ import annotations

"""
Normalize team fields in bg_unified_lines where values are dict/JSON-like blobs.

Strategy:
- For NBA/MLB rows where home_team/away_team starts with '{' or '[', extract a
  clean alias using regex from common keys: prefer 'team' code (e.g., ORL, LAD),
  then fallback to 'name' text if present.
- Update the text fields, then backfill team IDs via team_aliases.

Usage:
  python scripts/normalize_team_blobs.py
"""

import os
from sqlalchemy import create_engine, text
from pathlib import Path
import sys


def _add_shared_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shared = repo_root / "app" / "shared" / "python"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))


_add_shared_to_path()
from bountygate.utils import db_connection as dbc  # type: ignore  # noqa: E402


def db_url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


SQL_MATCH_COUNT = text(
    """
    SELECT
      SUM((home_team ~ '^[{\[]')::int) AS home_blob,
      SUM((away_team ~ '^[{\[]')::int) AS away_blob
    FROM bg_unified_lines
    WHERE sport_key IN ('basketball_nba','baseball_mlb','icehockey_nhl')
    """
)

# Prefer 'team' code (e.g., ORL/LAD) then fallback to 'name'
SQL_UPDATE_HOME = text(
    """
    UPDATE bg_unified_lines SET home_team = COALESCE(
      (NULLIF(
         regexp_replace(
           regexp_replace(
             regexp_replace(
               regexp_replace(home_team, '''', '"', 'g'),
               'True', 'true', 'g'
             ),
             'False', 'false', 'g'
           ),
           'None', 'null', 'g'
         ), ''
       )::jsonb ->> 'team'),
      (NULLIF(
         regexp_replace(
           regexp_replace(
             regexp_replace(regexp_replace(home_team, '''', '"', 'g'), 'True', 'true', 'g'),
             'False', 'false', 'g'
           ),
           'None', 'null', 'g'
         ), ''
       )::jsonb ->> 'name')
    )
    WHERE sport_key IN ('basketball_nba','baseball_mlb','icehockey_nhl')
      AND home_team IS NOT NULL
      AND home_team ~ '^[{\[]';
    """
)

SQL_UPDATE_AWAY = text(
    """
    UPDATE bg_unified_lines SET away_team = COALESCE(
      (NULLIF(
         regexp_replace(
           regexp_replace(
             regexp_replace(
               regexp_replace(away_team, '''', '"', 'g'),
               'True', 'true', 'g'
             ),
             'False', 'false', 'g'
           ),
           'None', 'null', 'g'
         ), ''
       )::jsonb ->> 'team'),
      (NULLIF(
         regexp_replace(
           regexp_replace(
             regexp_replace(regexp_replace(away_team, '''', '"', 'g'), 'True', 'true', 'g'),
             'False', 'false', 'g'
           ),
           'None', 'null', 'g'
         ), ''
       )::jsonb ->> 'name')
    )
    WHERE sport_key IN ('basketball_nba','baseball_mlb','icehockey_nhl')
      AND away_team IS NOT NULL
      AND away_team ~ '^[{\[]';
    """
)

SQL_BACKFILL_IDS = [
    text(
        """
        UPDATE bg_unified_lines bul
        SET home_team_id = ta.team_id
        FROM team_aliases ta
        WHERE bul.home_team_id IS NULL
          AND bul.sport_key = ta.sport_key
          AND bul.home_team = ta.alias
        """
    ),
    text(
        """
        UPDATE bg_unified_lines bul
        SET away_team_id = ta.team_id
        FROM team_aliases ta
        WHERE bul.away_team_id IS NULL
          AND bul.sport_key = ta.sport_key
          AND bul.away_team = ta.alias
        """
    ),
]


def main() -> int:
    engine = create_engine(db_url())
    try:
        with engine.begin() as conn:
            before = conn.execute(SQL_MATCH_COUNT).first()
            print("Before: blobs home/away:", dict(before._mapping))

            conn.execute(SQL_UPDATE_HOME)
            conn.execute(SQL_UPDATE_AWAY)

            after = conn.execute(SQL_MATCH_COUNT).first()
            print("After updates: blobs home/away:", dict(after._mapping))

            # Backfill IDs where now possible
            for stmt in SQL_BACKFILL_IDS:
                conn.execute(stmt)

        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
