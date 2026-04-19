"""PrizePicks loader → unified lines DataFrame.

This module intentionally does not write CSVs. It provides a function that
returns a pandas DataFrame shaped to the unified schema used by the pipeline.

Implement fetch logic against the PrizePicks API and map to COMMON_COLUMNS.
Until configured, the stub returns an empty DataFrame so DAGs continue to run.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Iterable

import os
import pandas as pd


COMMON_COLUMNS = [
    "player_name",
    "outcome",
    "line",
    "market_key",
    "bm_market_key",
    "price",
    "multiplier",
    "bookmaker_key",
    "sport_key",
    "sport_title",
    "commence_time",
    "home_team",
    "away_team",
    "event_id",
    "fetched_at_utc",
]


def _ensure_columns(df: pd.DataFrame, defaults: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    defaults = defaults or {}
    for column in COMMON_COLUMNS:
        if column not in df.columns:
            df[column] = defaults.get(column)
    return df[COMMON_COLUMNS]


def to_unified_dataframe(records: List[Dict[str, Any]], now_utc: Optional[datetime] = None) -> pd.DataFrame:
    """Map raw PrizePicks projection records to the unified schema.

    Expected keys in each record (examples, adjust to your API response):
    - player_name, outcome, line, market_key, bm_market_key, price, multiplier,
      sport_key, sport_title, commence_time, home_team, away_team, event_id
    This function sets bookmaker_key='prizepicks' and fetched_at_utc to now.
    """
    now = now_utc or datetime.now(timezone.utc)
    if not records:
        return pd.DataFrame(columns=COMMON_COLUMNS)
    df = pd.DataFrame(records)
    df["bookmaker_key"] = "prizepicks"
    df["fetched_at_utc"] = now
    return _ensure_columns(df)


def fetch_prizepicks_df(session, timeout: int = 10) -> pd.DataFrame:
    """Fetch PrizePicks data and return as unified DataFrame.

    Configure API access via environment variables if needed (e.g., PRIZEPICKS_API_URL,
    PRIZEPICKS_API_KEY). By default, this returns an empty DataFrame so the DAG
    can run without configuration.
    """
    api_url = os.environ.get("PRIZEPICKS_API_URL") or "https://partner-api.prizepicks.com/projections"

    try:
        import requests  # local import to avoid hard dep in environments not using this
        headers = {
            "accept": "application/json",
        }
        api_key = os.environ.get("PRIZEPICKS_API_KEY")
        if api_key:
            headers["authorization"] = f"Bearer {api_key}"
        resp = session.get(api_url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return pd.DataFrame(columns=COMMON_COLUMNS)

    # Build player map from included
    included = payload.get("included", []) if isinstance(payload, dict) else []
    player_name_by_id: Dict[str, str] = {}
    league_by_player: Dict[str, str] = {}
    for inc in included:
        if not isinstance(inc, dict) or inc.get("type") != "new_player":
            continue
        pid = str(inc.get("id"))
        attrs = inc.get("attributes", {}) or {}
        name = attrs.get("name") or attrs.get("display_name")
        league = attrs.get("league")
        if pid and name:
            player_name_by_id[pid] = name
        if pid and league:
            league_by_player[pid] = league

    # Parse projections
    data_items = payload.get("data", []) if isinstance(payload, dict) else []
    now = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    for proj in data_items:
        if not isinstance(proj, dict):
            continue
        attrs = proj.get("attributes", {}) or {}
        rel = proj.get("relationships", {}) or {}
        player_rel = (rel.get("new_player", {}) or {}).get("data", {}) or {}
        player_id = str(player_rel.get("id") or "")

        league = league_by_player.get(player_id) or attrs.get("league")
        league_map = {
            "NBA": "basketball_nba",
            "MLB": "baseball_mlb",
            "NHL": "icehockey_nhl",
            "NFL": "americanfootball_nfl",
        }
        sport_key = league_map.get(str(league).upper(), None)

        row = {
            "player_name": player_name_by_id.get(player_id) or attrs.get("player_name"),
            "outcome": None,  # will duplicate to over/under below
            "line": attrs.get("line_score"),
            "market_key": attrs.get("stat_type"),
            "bm_market_key": attrs.get("stat_display_name") or attrs.get("stat_type"),
            "price": None,
            "multiplier": None,
            "bookmaker_key": "prizepicks",
            "sport_key": sport_key,
            "sport_title": league,
            "commence_time": attrs.get("start_time"),
            "home_team": None,
            "away_team": None,
            "event_id": proj.get("id") or (attrs.get("group_key") or attrs.get("game_id")),
            "fetched_at_utc": now,
        }
        rows.append(row)

    base_df = pd.DataFrame(rows)
    if base_df.empty:
        return pd.DataFrame(columns=COMMON_COLUMNS)
    # Duplicate each row into over/under outcomes (PrizePicks higher/lower)
    over_df = base_df.copy()
    over_df["outcome"] = "over"
    under_df = base_df.copy()
    under_df["outcome"] = "under"
    out_df = pd.concat([over_df, under_df], ignore_index=True)
    return _ensure_columns(out_df)


# ------------ Minimal mapping template (fill in with actual keys) ------------ #

def map_prizepicks_record(raw: Dict[str, Any], *, now_utc: Optional[datetime] = None) -> Dict[str, Any]:
    """Map a single PrizePicks raw record into unified row dict.

    Adjust the dotted lookups to your payload. The examples reflect a common
    shape: top-level projection info, nested player and game objects.

    Example raw (illustrative):
    {
      "id": "proj_123",
      "stat_type": "PTS",
      "line_score": 24.5,
      "league": "nba",
      "player": {"id": "pl_1", "name": "Nikola Jokic"},
      "game": {
        "id": "g_987",
        "start_time": "2025-10-25T19:00:00Z",
        "home_team": "Denver Nuggets",
        "away_team": "Utah Jazz"
      }
    }
    """
    now = now_utc or datetime.now(timezone.utc)

    # Optional market mapping (edit to your conventions)
    market_map = {
        # "PTS": "player_points",
        # "REB": "player_rebounds",
        # "AST": "player_assists",
    }

    # Helper to dot-get
    def dget(obj: Dict[str, Any], path: Iterable[str], default: Any = None) -> Any:
        cur: Any = obj
        for key in path:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(key)
            if cur is None:
                return default
        return cur

    sport_key = (raw.get("sport_key")
                 or raw.get("league_key")
                 or (str(raw.get("league")).lower().strip() if raw.get("league") else None))
    # Normalize common leagues if desired
    league_map = {
        "nba": "basketball_nba",
        "mlb": "baseball_mlb",
        "nhl": "icehockey_nhl",
        "nfl": "americanfootball_nfl",
    }
    sport_key = league_map.get(str(sport_key).lower(), sport_key)

    bm_market_key = raw.get("market_display") or raw.get("stat_type")
    market_key = market_map.get(str(raw.get("stat_type")).upper(), raw.get("market_key") or raw.get("stat_type"))

    row: Dict[str, Any] = {
        "player_name": dget(raw, ["player", "name"]) or raw.get("player_name"),
        "outcome": None,  # or duplicate rows for "over"/"under" like Splash
        "line": raw.get("line") or raw.get("line_score"),
        "market_key": market_key,
        "bm_market_key": bm_market_key,
        "price": raw.get("price"),
        "multiplier": raw.get("multiplier"),
        "bookmaker_key": "prizepicks",
        "sport_key": sport_key,
        "sport_title": raw.get("sport_title") or (str(raw.get("league")).upper() if raw.get("league") else None),
        "commence_time": dget(raw, ["game", "start_time"]) or raw.get("commence_time") or raw.get("game_time"),
        "home_team": dget(raw, ["game", "home_team"]) or raw.get("home_team"),
        "away_team": dget(raw, ["game", "away_team"]) or raw.get("away_team"),
        "event_id": dget(raw, ["game", "id"]) or raw.get("event_id") or raw.get("game_id") or raw.get("id"),
        "fetched_at_utc": now,
    }
    return row


def parse_prizepicks_payload(payload: Any, *, now_utc: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Parse PrizePicks API payload into unified row dicts.

    If your endpoint returns {"data": [...]}, we iterate that; if it returns a
    plain list, we iterate the list directly. Customize as needed.
    """
    items: List[Dict[str, Any]]
    if isinstance(payload, list):
        items = [x for x in payload if isinstance(x, dict)]
    else:
        items = [x for x in (payload or {}).get("data", []) if isinstance(x, dict)]
    return [map_prizepicks_record(it, now_utc=now_utc) for it in items]
