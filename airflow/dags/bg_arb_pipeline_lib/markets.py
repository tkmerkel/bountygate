"""Sports and market_keys the arb pipeline ingests from the-odds-api.

ARB_MARKETS_BY_SPORT keyed by sport_key — each request only sends the
markets relevant to that sport. The-odds-api charges credits per
market-per-request, so sending NFL markets in an NBA call wastes credits
and slows the round-trip.

Reference: https://the-odds-api.com/sports-odds-data/betting-markets.html
"""
from __future__ import annotations

ARB_SPORTS: tuple[str, ...] = (
    "basketball_nba",
    "icehockey_nhl",
    "americanfootball_nfl",
    "baseball_mlb",
)

SPORT_TITLE: dict[str, str] = {
    "basketball_nba": "NBA",
    "icehockey_nhl": "NHL",
    "americanfootball_nfl": "NFL",
    "baseball_mlb": "MLB",
}

ARB_BOOKMAKERS: tuple[str, ...] = ("fanduel", "betmgm")

# Sport-specific market lists. Maintained from MAP_MISSING_MARKETS-2026-05-14.md,
# current selectors, and the-odds-api docs. Add as books expose new ones.
ARB_MARKETS_BY_SPORT: dict[str, tuple[str, ...]] = {
    "basketball_nba": (
        "player_points", "player_points_alternate",
        "player_assists", "player_assists_alternate",
        "player_rebounds", "player_rebounds_alternate",
        "player_threes", "player_threes_alternate",
        "player_blocks", "player_blocks_alternate",
        "player_steals", "player_steals_alternate",
        "player_points_rebounds", "player_points_rebounds_alternate",
        "player_points_assists", "player_points_assists_alternate",
        "player_points_rebounds_assists", "player_points_rebounds_assists_alternate",
        "player_points_q1",
    ),
    "icehockey_nhl": (
        "player_shots_on_goal", "player_shots_on_goal_alternate",
        "player_total_saves",
    ),
    "americanfootball_nfl": (
        # Add NFL player-prop markets as needed.
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
