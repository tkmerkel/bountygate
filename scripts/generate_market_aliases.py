#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text


def _add_shared_to_path() -> None:
    import sys
    repo_root = Path(__file__).resolve().parents[1]
    shared = repo_root / "app" / "shared" / "python"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))


_add_shared_to_path()
from bountygate.utils import db_connection as dbc  # type: ignore  # noqa: E402
from bountygate.utils.etl_assets import (  # type: ignore  # noqa: E402
    splash_market_map_dict,
    pp_market_map_dict,
    sport_league_map,
)


def db_url() -> str:
    return os.environ.get("DATABASE_URL") or getattr(dbc, "DATABASE_URL", "")


def canonical_or_self(bm_key: str) -> Optional[str]:
    # Already canonical if looks like snake_case with known prefixes
    if not bm_key:
        return None
    if any(bm_key.startswith(p) for p in ("player_", "batter_", "pitcher_")):
        return bm_key
    # Also accept common snake_case aliases
    if "_" in bm_key and bm_key.islower():
        return bm_key
    return None


def normalize_display_key(s: str) -> str:
    # Normalize whitespace and plus variants
    t = (s or "").strip()
    # Common harmonizations
    t = t.replace(" + ", "+").replace(" & ", "+").replace(" / ", "/")
    # Title-case choices we expect, preserve abbreviations
    return t


def map_display_to_canonical(bookmaker: str, sport_key: str, display: str) -> Optional[str]:
    # 1) Splash
    if bookmaker == "splash":
        key = display.strip().lower().replace(" ", "_")
        return splash_market_map_dict.get(key)

    # 2) PrizePicks/Underdog/Sleeper – try PP map by league
    league = sport_league_map.get(sport_key)
    pp_map = pp_market_map_dict.get(league or "", {})
    if display in pp_map:
        return pp_map[display]

    # Variants for NBA names
    disp = normalize_display_key(display)
    nba_variants = {
        "Points": "player_points",
        "Rebounds": "player_rebounds",
        "Assists": "player_assists",
        "3-PT Made": "player_threes",
        "Three Pointers Made": "player_threes",
        "Pts+Rebs+Asts": "player_points_rebounds_assists",
        "Points+Rebounds+Assists": "player_points_rebounds_assists",
        "Pts+Rebs": "player_points_rebounds",
        "Points+Rebounds": "player_points_rebounds",
        "Pts+Asts": "player_points_assists",
        "Points+Assists": "player_points_assists",
        "Rebs+Asts": "player_rebounds_assists",
        "Rebounds+Assists": "player_rebounds_assists",
        "Blocks+Steals": "player_blocks_steals",
    }
    if sport_key == "basketball_nba" and disp in nba_variants:
        return nba_variants[disp]

    # NHL
    nhl_variants = {
        "Goals": "player_goals",
        "Shots On Goal": "player_shots_on_goal",
        "SOG": "player_shots_on_goal",
        "Assists": "player_assists",
        "Points": "player_points",
        "Blocked Shots": "player_blocked_shots",
        "Saves": "player_total_saves",
    }
    if sport_key == "icehockey_nhl" and disp in nhl_variants:
        return nhl_variants[disp]

    # NFL
    nfl_variants = {
        "Pass Yards": "player_pass_yds",
        "Pass TDs": "player_pass_tds",
        "Rush+Rec TDs": "player_rush_reception_tds",
        "Rush+Rec Yds": "player_rush_reception_yds",
        "Pass+Rush Yds": "player_pass_rush_reception_yds",
        "Pass Attempts": "player_pass_attempts",
        "Pass Completions": "player_pass_completions",
        "Rush Yards": "player_rush_yds",
        "Rush Attempts": "player_rush_attempts",
        "INT": "player_pass_interceptions",
        "Longest Rush": "player_rush_longest",
        "Receiving Yards": "player_reception_yds",
        "Receptions": "player_receptions",
        "Longest Reception": "player_reception_longest",
        "FG Made": "player_field_goals",
        "Kicking Points": "player_kicking_points",
        "Tackles": "player_solo_tackles",
        "Tackles+Assists": "player_tackles_assists",
    }
    if sport_key == "americanfootball_nfl" and disp in nfl_variants:
        return nfl_variants[disp]

    # MLB
    mlb_variants = {
        "Total Bases": "batter_total_bases",
        "Runs": "batter_runs_scored",
        "RBIs": "batter_rbis",
        "Hits+Runs+RBIs": "batter_hits_runs_rbis",
        "Hits": "batter_hits",
        "Singles": "batter_singles",
        "Home Runs": "batter_home_runs",
        "Stolen Bases": "batter_stolen_bases",
        "Strikeouts Thrown": "pitcher_strikeouts",
        "Outs": "pitcher_outs",
        "Walks Allowed": "pitcher_walks",
        "Runs Allowed": "pitcher_earned_runs",
    }
    if sport_key == "baseball_mlb" and disp in mlb_variants:
        return mlb_variants[disp]

    return None


def main() -> int:
    out_dir = Path("db/market_aliases")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "market_aliases.csv"

    engine = create_engine(db_url())
    try:
        sql = text(
            """
            SELECT DISTINCT bookmaker_key, sport_key, bm_market_key
            FROM bg_unified_lines
            WHERE COALESCE(bm_market_key,'')<>''
            ORDER BY 1,2,3
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()

        mapped = []
        for bookmaker_key, sport_key, bm_market_key in rows:
            bm_market_key = bm_market_key or ""
            canonical = canonical_or_self(bm_market_key)
            note = "canonical" if canonical else "auto"
            if not canonical:
                canonical = map_display_to_canonical(str(bookmaker_key), str(sport_key), str(bm_market_key))
                if canonical is None:
                    note = "unmapped"
            mapped.append({
                "bookmaker_key": bookmaker_key,
                "sport_key": sport_key,
                "bm_market_key": bm_market_key,
                "canonical_market_key": canonical or "",
                "notes": note,
            })

        # Write CSV
        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "bookmaker_key",
                    "sport_key",
                    "bm_market_key",
                    "canonical_market_key",
                    "notes",
                ],
            )
            w.writeheader()
            for row in mapped:
                w.writerow(row)

        print(f"Wrote {len(mapped)} rows to {out_path}")
        # Print a small summary of unmapped
        df = pd.DataFrame(mapped)
        summary = df[df["canonical_market_key"] == ""]["bm_market_key"].value_counts().head(20)
        if not summary.empty:
            print("Top unmapped market keys:")
            for k, v in summary.items():
                print(f"  {k}: {v}")
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

