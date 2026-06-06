"""Sports, markets, and bookmakers for the game-line arb pipeline.

The-odds-api `/v4/sports/{sport}/odds` returns every in-window event for a
sport in a single call, with one request billed per market per region. Game
markets (h2h, spreads, totals) are the same shape across all 5 sports, so a
flat list — not the per-sport split used by bg_arb_pipeline — is correct here.
"""
from __future__ import annotations

GAME_ARB_SPORTS: tuple[str, ...] = (
    "americanfootball_nfl",
    "basketball_nba",
    "icehockey_nhl",
    "baseball_mlb",
    "basketball_ncaab",
)

SPORT_TITLE: dict[str, str] = {
    "americanfootball_nfl": "NFL",
    "basketball_nba": "NBA",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
    "basketball_ncaab": "NCAAB",
}

GAME_ARB_MARKETS: tuple[str, ...] = ("h2h", "spreads", "totals")

# `williamhill_us` is the-odds-api's key for Caesars in the US. Confirmed via
# the-odds-api bookmaker reference. Flip to `caesars` only if the historical
# bountygate Caesars rows land under a different key.
GAME_ARB_BOOKMAKERS: tuple[str, ...] = (
    "fanduel",
    "betmgm",
    "williamhill_us",
    "draftkings",
)

GAME_ARB_REGION: str = "us"
