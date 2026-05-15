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


NHL: Dict[str, str] = {
    "ANA": "Anaheim Ducks",
    "ARI": "Arizona Coyotes",
    "BOS": "Boston Bruins",
    "BUF": "Buffalo Sabres",
    "CGY": "Calgary Flames",
    "CAR": "Carolina Hurricanes",
    "CHI": "Chicago Blackhawks",
    "COL": "Colorado Avalanche",
    "CBJ": "Columbus Blue Jackets",
    "DAL": "Dallas Stars",
    "DET": "Detroit Red Wings",
    "EDM": "Edmonton Oilers",
    "FLA": "Florida Panthers",
    "LAK": "Los Angeles Kings",
    "MIN": "Minnesota Wild",
    "MTL": "Montréal Canadiens",
    "NSH": "Nashville Predators",
    "NJD": "New Jersey Devils",
    "NYI": "New York Islanders",
    "NYR": "New York Rangers",
    "OTT": "Ottawa Senators",
    "PHI": "Philadelphia Flyers",
    "PIT": "Pittsburgh Penguins",
    "SEA": "Seattle Kraken",
    "SJS": "San Jose Sharks",
    "STL": "St. Louis Blues",
    "TBL": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",
    "VAN": "Vancouver Canucks",
    "VGK": "Vegas Golden Knights",
    "WSH": "Washington Capitals",
    "WPG": "Winnipeg Jets",
}


def url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


def main() -> int:
    engine = create_engine(url())
    sport_key = "icehockey_nhl"
    try:
        with engine.begin() as conn:
            for abbr, name in NHL.items():
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

