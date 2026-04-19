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


MLB: Dict[str, str] = {
    "ARI": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CHW": "Chicago White Sox",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC": "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "OAK": "Oakland Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD": "San Diego Padres",
    "SEA": "Seattle Mariners",
    "SF": "San Francisco Giants",
    "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
}


def url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


def main() -> int:
    engine = create_engine(url())
    sport_key = "baseball_mlb"
    try:
        with engine.begin() as conn:
            for abbr, name in MLB.items():
                row = conn.execute(
                    text("SELECT team_id FROM team_reference WHERE sport_key=:s AND display_name=:n"),
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
                conn.execute(
                    text(
                        "INSERT INTO team_aliases(sport_key, alias, team_id, source_bookmaker) "
                        "VALUES (:s, :alias, :tid, :src) ON CONFLICT (sport_key, alias) DO NOTHING"
                    ),
                    {"s": sport_key, "alias": abbr, "tid": tid, "src": "sleeper"},
                )

            # Backfill
            conn.execute(
                text(
                    "UPDATE bg_unified_lines bul SET home_team_id = ta.team_id "
                    "FROM team_aliases ta WHERE bul.home_team_id IS NULL AND bul.sport_key = ta.sport_key AND bul.home_team = ta.alias"
                )
            )
            conn.execute(
                text(
                    "UPDATE bg_unified_lines bul SET away_team_id = ta.team_id "
                    "FROM team_aliases ta WHERE bul.away_team_id IS NULL AND bul.sport_key = ta.sport_key AND bul.away_team = ta.alias"
                )
            )
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

