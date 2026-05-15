#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path
from typing import Dict

from sqlalchemy import create_engine, text


def _add_shared_to_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shared = repo_root / "app" / "shared" / "python"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))


_add_shared_to_path()
from bountygate.utils import db_connection as dbc  # type: ignore  # noqa: E402


NFL: Dict[str, str] = {
    "ARI": "Arizona Cardinals",
    "ATL": "Atlanta Falcons",
    "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",
    "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",
    "DET": "Detroit Lions",
    "GB": "Green Bay Packers",
    "HOU": "Houston Texans",
    "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs",
    "LV": "Las Vegas Raiders",
    "LAC": "Los Angeles Chargers",
    "LAR": "Los Angeles Rams",
    "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings",
    "NE": "New England Patriots",
    "NO": "New Orleans Saints",
    "NYG": "New York Giants",
    "NYJ": "New York Jets",
    "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers",
    "SEA": "Seattle Seahawks",
    "SF": "San Francisco 49ers",
    "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",
    "WAS": "Washington Commanders",
}


def url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


def main() -> int:
    engine = create_engine(url())
    sport_key = "americanfootball_nfl"
    try:
        with engine.begin() as conn:
            # Ensure team_reference rows exist
            for abbr, name in NFL.items():
                row = conn.execute(
                    text(
                        "SELECT team_id FROM team_reference WHERE sport_key=:s AND display_name=:n"
                    ),
                    {"s": sport_key, "n": name},
                ).first()
                if row is None:
                    tid = str(uuid.uuid4())
                    conn.execute(
                        text(
                            "INSERT INTO team_reference(team_id, sport_key, display_name, abbreviation) "
                            "VALUES (:id, :s, :n, :abbr)"
                        ),
                        {"id": tid, "s": sport_key, "n": name, "abbr": abbr},
                    )
                else:
                    tid = row[0]

                # Insert alias mapping for this abbreviation
                conn.execute(
                    text(
                        "INSERT INTO team_aliases(sport_key, alias, team_id, source_bookmaker) "
                        "VALUES (:s, :alias, :tid, :src) ON CONFLICT (sport_key, alias) DO NOTHING"
                    ),
                    {"s": sport_key, "alias": abbr, "tid": tid, "src": "sleeper"},
                )

            # Backfill IDs using new aliases
            conn.execute(
                text(
                    """
                    UPDATE bg_unified_lines bul
                    SET home_team_id = ta.team_id
                    FROM team_aliases ta
                    WHERE bul.home_team_id IS NULL
                      AND bul.sport_key = ta.sport_key
                      AND bul.home_team = ta.alias
                    """
                )
            )
            conn.execute(
                text(
                    """
                    UPDATE bg_unified_lines bul
                    SET away_team_id = ta.team_id
                    FROM team_aliases ta
                    WHERE bul.away_team_id IS NULL
                      AND bul.sport_key = ta.sport_key
                      AND bul.away_team = ta.alias
                    """
                )
            )
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

