"""Sports and market_keys the arb pipeline ingests from the-odds-api.

Ported verbatim from airflow/dags/_archive_pre_pivot/bg_arb_pipeline_lib/markets.py
into the shared package at bountygate.arb.markets. Values are identical to the
archive source; the module docstring has been updated to reference the new home.

ARB_MARKETS_BY_SPORT is keyed by sport_key — each request only sends the
markets relevant to that sport. The-odds-api charges credits per
market-per-request, so sending NFL markets in an NBA call wastes credits
and slows the round-trip.

Reference: https://the-odds-api.com/sports-odds-data/betting-markets.html

Scope: only numeric over/under markets. The ingest normalizer drops outcomes
whose `name` is not "over"/"under", so yes/no markets (anytime_td,
first_basket, double_double, goal_scorer_anytime, etc.) would burn credits
without producing rows.

Note: ARB_BOOKMAKERS was intentionally dropped from this port. The new
pipeline takes whatever books the API returns; book filtering happens at the
executor layer, not the config layer.
"""
from __future__ import annotations

import os

ARB_SPORTS: tuple[str, ...] = (
    "basketball_nba",
    "icehockey_nhl",
    "americanfootball_nfl",
    "baseball_mlb",
    "basketball_ncaab",
    "basketball_wnba",
)

SPORT_TITLE: dict[str, str] = {
    "basketball_nba": "NBA",
    "icehockey_nhl": "NHL",
    "americanfootball_nfl": "NFL",
    "baseball_mlb": "MLB",
    "basketball_ncaab": "NCAAB",
    "basketball_wnba": "WNBA",
}

# Sport-specific market lists. Maintained from MAP_MISSING_MARKETS-2026-05-14.md,
# current selectors, and the-odds-api docs. Add as books expose new ones.
_NBA_PROP_MARKETS: tuple[str, ...] = (
    "player_points", "player_points_alternate",
    "player_assists", "player_assists_alternate",
    "player_rebounds", "player_rebounds_alternate",
    "player_threes", "player_threes_alternate",
    "player_blocks", "player_blocks_alternate",
    "player_steals", "player_steals_alternate",
    "player_turnovers", "player_turnovers_alternate",
    "player_points_rebounds", "player_points_rebounds_alternate",
    "player_points_assists", "player_points_assists_alternate",
    "player_rebounds_assists", "player_rebounds_assists_alternate",
    "player_points_rebounds_assists", "player_points_rebounds_assists_alternate",
    "player_blocks_steals",
    "player_fantasy_points", "player_fantasy_points_alternate",
    "player_points_q1",
)

ARB_MARKETS_BY_SPORT: dict[str, tuple[str, ...]] = {
    "basketball_nba": _NBA_PROP_MARKETS,
    "basketball_ncaab": _NBA_PROP_MARKETS,
    "basketball_wnba": _NBA_PROP_MARKETS,
    "icehockey_nhl": (
        "player_points", "player_points_alternate",
        "player_goals", "player_goals_alternate",
        "player_assists", "player_assists_alternate",
        "player_power_play_points", "player_power_play_points_alternate",
        "player_shots_on_goal", "player_shots_on_goal_alternate",
        "player_blocked_shots", "player_blocked_shots_alternate",
        "player_total_saves", "player_total_saves_alternate",
    ),
    "americanfootball_nfl": (
        # Passing
        "player_pass_yds", "player_pass_yds_alternate",
        "player_pass_tds", "player_pass_tds_alternate",
        "player_pass_completions", "player_pass_completions_alternate",
        "player_pass_attempts", "player_pass_attempts_alternate",
        "player_pass_interceptions", "player_pass_interceptions_alternate",
        "player_pass_longest_completion", "player_pass_longest_completion_alternate",
        # Rushing
        "player_rush_yds", "player_rush_yds_alternate",
        "player_rush_tds", "player_rush_tds_alternate",
        "player_rush_attempts", "player_rush_attempts_alternate",
        "player_rush_longest", "player_rush_longest_alternate",
        # Receiving
        "player_reception_yds", "player_reception_yds_alternate",
        "player_receptions", "player_receptions_alternate",
        "player_reception_tds", "player_reception_tds_alternate",
        "player_reception_longest", "player_reception_longest_alternate",
        # Combined skill-position
        "player_pass_rush_yds", "player_pass_rush_yds_alternate",
        "player_rush_reception_yds", "player_rush_reception_yds_alternate",
        "player_pass_rush_reception_yds", "player_pass_rush_reception_yds_alternate",
        "player_rush_reception_tds", "player_rush_reception_tds_alternate",
        "player_pass_rush_reception_tds", "player_pass_rush_reception_tds_alternate",
        # Kicking
        "player_kicking_points", "player_kicking_points_alternate",
        "player_field_goals", "player_field_goals_alternate",
        "player_pats", "player_pats_alternate",
        # Defense
        "player_sacks", "player_sacks_alternate",
        "player_solo_tackles", "player_solo_tackles_alternate",
        "player_tackles_assists", "player_tackles_assists_alternate",
        "player_defensive_interceptions",
    ),
    "baseball_mlb": (
        "batter_singles", "batter_singles_alternate",
        "batter_doubles", "batter_doubles_alternate",
        "batter_stolen_bases", "batter_stolen_bases_alternate",
    ),
}

# Flat list for tests and any consumer that wants the union.
ARB_MARKETS: tuple[str, ...] = tuple(
    sorted({m for markets in ARB_MARKETS_BY_SPORT.values() for m in markets})
)

# Markets we never bet (debug-only).
ARB_MARKET_BLACKLIST: frozenset[str] = frozenset({
    "pitcher_strikeouts",
    "pitcher_strikeouts_alternate",
})

_DEFAULT_PROPS_SPORTS = "basketball_nba,baseball_mlb,icehockey_nhl,americanfootball_nfl,basketball_wnba"


def props_sports() -> dict[str, tuple[str, ...]]:
    """Per-sport prop market lists gated by BG_PROPS_SPORTS (comma list).
    Unknown sport keys are ignored with a warning print. Default excludes NCAAB."""
    raw = os.getenv("BG_PROPS_SPORTS", _DEFAULT_PROPS_SPORTS)
    out = {}
    for key in (k.strip() for k in raw.split(",") if k.strip()):
        if key in ARB_MARKETS_BY_SPORT:
            out[key] = ARB_MARKETS_BY_SPORT[key]
        else:
            print(f"[arb.markets] BG_PROPS_SPORTS contains unknown sport key: {key}")
    return out
