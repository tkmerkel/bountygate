"""Sports and market_keys the arb pipeline ingests from the-odds-api.

Both standard and `_alternate` variants are listed explicitly. Future iteration:
move to YAML or a DB-driven config so updates don't require a code deploy.

Reference: https://the-odds-api.com/sports-odds-data/betting-markets.html
"""
from __future__ import annotations

ARB_SPORTS: tuple[str, ...] = (
    "basketball_nba",
    "icehockey_nhl",
    "americanfootball_nfl",
    "baseball_mlb",
)

# Sport-key -> sport_title used in DB rows. The-odds-api uses sport keys like
# "basketball_nba"; downstream tables use the friendlier title.
SPORT_TITLE: dict[str, str] = {
    "basketball_nba": "NBA",
    "icehockey_nhl": "NHL",
    "americanfootball_nfl": "NFL",
    "baseball_mlb": "MLB",
}

ARB_BOOKMAKERS: tuple[str, ...] = ("fanduel", "betmgm")

# Player-prop markets we want for arb. Add to this list as books expose new ones.
# Maintained from MAP_MISSING_MARKETS-2026-05-14.md and current selectors.
ARB_MARKETS: tuple[str, ...] = (
    # NBA / basketball
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
    # NHL
    "player_shots_on_goal", "player_shots_on_goal_alternate",
    "player_total_saves",
    # MLB
    "batter_singles", "batter_singles_alternate",
    "batter_doubles", "batter_doubles_alternate",
    "batter_stolen_bases", "batter_stolen_bases_alternate",
    # NFL (placeholders — add as needed)
)

# Markets we never bet (debug-only). Mirror bg_arbitrage_player_props.MARKET_BLACKLIST.
ARB_MARKET_BLACKLIST: frozenset[str] = frozenset({
    "pitcher_strikeouts",
    "pitcher_strikeouts_alternate",
})
