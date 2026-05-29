"""Thin HTTP client layer for the Phase 2 enrichment feeds.

URL builders are pure (unit-tested). ``fetch_json`` is a small requests wrapper
with a timeout and a couple of retries; it is exercised live by the DAGs, not in
unit tests (the parsers are tested against captured fixtures instead).

All feeds are keyless. ESPN sport/league path map covers the active sports;
add an entry to ESPN_PATHS to extend.
"""
from __future__ import annotations

import time
from datetime import date
from typing import Optional

# (espn_sport, espn_league)
ESPN_PATHS = {
    "baseball_mlb": ("baseball", "mlb"),
    "basketball_nba": ("basketball", "nba"),
    "icehockey_nhl": ("hockey", "nhl"),
    "americanfootball_nfl": ("football", "nfl"),
    "americanfootball_ncaaf": ("football", "college-football"),
    "basketball_ncaab": ("basketball", "mens-college-basketball"),
}

_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_MLB_BASE = "https://statsapi.mlb.com/api/v1"
_NHL_BASE = "https://api-web.nhle.com/v1"


# --- ESPN -------------------------------------------------------------------
def build_espn_scoreboard_url(sport_key: str, on_date: date) -> str:
    sport, league = ESPN_PATHS[sport_key]
    return f"{_ESPN_BASE}/{sport}/{league}/scoreboard?dates={on_date.strftime('%Y%m%d')}"


def build_espn_injuries_url(sport_key: str) -> str:
    sport, league = ESPN_PATHS[sport_key]
    return f"{_ESPN_BASE}/{sport}/{league}/injuries"


# --- MLB StatsAPI -----------------------------------------------------------
def build_mlb_schedule_url(on_date: date) -> str:
    return f"{_MLB_BASE}/schedule?sportId=1&date={on_date.strftime('%Y-%m-%d')}"


def build_mlb_boxscore_url(game_pk) -> str:
    return f"{_MLB_BASE}/game/{game_pk}/boxscore"


# --- NHL --------------------------------------------------------------------
def build_nhl_schedule_url(on_date) -> str:
    # on_date may be a date or a 'YYYY-MM-DD' string; NHL accepts the date path.
    d = on_date.strftime("%Y-%m-%d") if isinstance(on_date, date) else str(on_date)
    return f"{_NHL_BASE}/schedule/{d}"


def build_nhl_boxscore_url(game_id) -> str:
    return f"{_NHL_BASE}/gamecenter/{game_id}/boxscore"


# --- HTTP -------------------------------------------------------------------
def fetch_json(url: str, *, timeout: int = 20, retries: int = 2,
               backoff: float = 1.5) -> Optional[dict]:
    """GET ``url`` and return parsed JSON, or None on persistent failure.

    Imported lazily so the pure URL builders/parsers import without requests.
    """
    import requests  # lazy: keeps the module importable without requests

    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "bountygate/phase2"})
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 - feed flakiness is expected; caller skips on None
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
    print(f"[enrichment.clients] fetch_json failed for {url}: {last_exc}")
    return None
