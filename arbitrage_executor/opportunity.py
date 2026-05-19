import pandas as pd
import json
import os
from datetime import datetime
from db_connection import (
    fetch_data,
)

def _json_safe(value):
    if value is None:
        return None

    # Pandas missing values
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # Timestamps / datetimes
    if isinstance(value, (pd.Timestamp, datetime)):
        # Ensure timezone-aware timestamps remain so in ISO-8601
        return value.isoformat()

    # Numpy scalar types (common from DB drivers)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return value.item()
    except Exception:
        pass

    return value


def _df_to_json_records(df: pd.DataFrame):
    records = df.to_dict(orient="records")
    return [
        {k: _json_safe(v) for k, v in record.items()}
        for record in records
    ]


def get_market_keys(opportunity: dict) -> tuple[str, str]:
    """Return (over_market_key, under_market_key), falling back to market_key."""
    market_key = opportunity.get("market_key")
    return (
        opportunity.get("over_market_key") or market_key,
        opportunity.get("under_market_key") or market_key,
    )


POLL_INTERVAL_SECONDS = int(os.getenv("ARBITRAGE_POLL_SECONDS", "30"))
MIN_ROI_THRESHOLD = float(os.getenv("MIN_ROI_THRESHOLD", "0.005"))
WAGER_SCALE_FACTOR = float(os.getenv("WAGER_SCALE_FACTOR", "0.01"))
TESTING_MODE = os.getenv("TESTING_MODE", "false").lower() == "true"

# Source table — produced by the bg_arb_pipeline DAG. Carries per-leg
# market_key columns, so std/alt pairings appear in a single table instead
# of the legacy two-table split (bg_arbitrage_player_props + _alt).
OPPORTUNITIES_TABLE = "bg_arbitrage_opportunities"


def _build_query(testing_mode: bool = False) -> str:
    """Build query for fetching opportunities from the unified arb table.

    SELECT aliases the new schema's ``under_book`` / ``over_book`` /
    ``canonical_market`` to the legacy column names downstream callers
    expect (``under_bookmaker_key`` / ``over_bookmaker_key`` /
    ``market_key``), keeping the executor's opportunity-dict shape stable.
    """
    # NUMERIC columns are cast to float8 in the SELECT — psycopg2 returns
    # NUMERIC as Decimal, which doesn't multiply against Python floats
    # (WAGER_SCALE_FACTOR) in the downstream pandas arithmetic. The legacy
    # bg_arbitrage_player_props schema used DOUBLE PRECISION so the issue
    # only surfaced once we pointed at this table.
    base_query = f"""
    SELECT player_name,
           sport_title,
           home_team,
           away_team,
           canonical_market           AS market_key,
           under_market_key,
           over_market_key,
           under_line::float8         AS under_line,
           over_line::float8          AS over_line,
           under_book                 AS under_bookmaker_key,
           over_book                  AS over_bookmaker_key,
           under_price::float8        AS under_price,
           over_price::float8         AS over_price,
           wager_under::float8        AS wager_under,
           wager_over::float8         AS wager_over,
           payout::float8             AS payout,
           arb_ev::float8             AS arb_ev,
           roi::float8                AS roi,
           hours_until_commence::float8 AS hours_until_commence,
           fetched_at_utc
    FROM {OPPORTUNITIES_TABLE} bao
    WHERE under_line = over_line
      AND under_book IN ('fanduel', 'betmgm')
      AND over_book IN ('fanduel', 'betmgm')
      AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
      AND NOT EXISTS (
          SELECT 1 FROM bg_executed_opportunities eo
          WHERE eo.player_name = bao.player_name
            AND (eo.under_market_key = bao.under_market_key
                 OR eo.over_market_key = bao.over_market_key)
            AND eo.executed_at_utc >= CURRENT_DATE
      )
    """
    # under_book != over_book is enforced by the builder; no need to re-check.

    if testing_mode:
        return base_query + """
      AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '4 hours'
      AND hours_until_commence > 0
      AND hours_until_commence < 72
ORDER BY fetched_at_utc DESC, roi DESC
LIMIT 20;
"""
    return base_query + f"""
      AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '10 minutes'
      AND hours_until_commence > 0.03
      AND hours_until_commence < 24
      AND roi >= {MIN_ROI_THRESHOLD}
ORDER BY roi DESC
LIMIT 10;
"""


def _fetch_best_opportunity(testing_mode: bool = None) -> dict:
    """
    Fetch the best arbitrage opportunity from database.

    Args:
        testing_mode: Override testing mode. If None, uses TESTING_MODE env var.

    Returns:
        Dictionary with opportunity details, or empty dict if none found.
    """
    if testing_mode is None:
        testing_mode = TESTING_MODE

    print(f"Fetching opportunities (testing_mode={testing_mode})...")

    df_all = fetch_data(_build_query(testing_mode))

    if df_all is None or df_all.empty:
        print(f"No data returned from {OPPORTUNITIES_TABLE}.")
        return {}

    df_all = df_all.copy()
    df_all["source_table"] = OPPORTUNITIES_TABLE
    df_all["roi"] = pd.to_numeric(df_all.get("roi"), errors="coerce")
    df_all = df_all.dropna(subset=["roi"])

    if not testing_mode:
        # Apply ROI filter only in production mode
        df_all = df_all[df_all["roi"] >= MIN_ROI_THRESHOLD]

    if df_all.empty:
        print("No opportunities found after filtering.")
        return {}

    # Sort by ROI descending and take the best
    df_all = df_all.sort_values("roi", ascending=False)
    df_best = df_all.iloc[[0]].copy()

    df_best['wager_under'] = (df_best['wager_under'] * WAGER_SCALE_FACTOR).round(2) + 0.01
    df_best['wager_over'] = (df_best['wager_over'] * WAGER_SCALE_FACTOR).round(2) + 0.01
    df_best['payout'] = (df_best['payout'] * WAGER_SCALE_FACTOR).round(2)
    df_best['arb_ev'] = (df_best['arb_ev'] * WAGER_SCALE_FACTOR).round(2)

    opportunity = json.dumps(_df_to_json_records(df_best))
    opportunity_info = json.loads(opportunity)[0]

    # Display market key (use over_market_key, falling back to market_key for backward compatibility)
    display_market = opportunity_info.get('over_market_key') or opportunity_info.get('market_key')
    print(f"✓ Found opportunity: {opportunity_info.get('player_name')} - {display_market}")
    print(f"  ROI: {opportunity_info.get('roi', 0):.4f} ({opportunity_info.get('roi', 0) * 100:.2f}%)")
    print(f"  Books: {opportunity_info.get('over_bookmaker_key')} vs {opportunity_info.get('under_bookmaker_key')}")

    return opportunity_info


def _persist_opportunity_info(opportunity_info: dict) -> None:
    if not opportunity_info:
        return
    with open("opportunity_info.json", "w") as f:
        json.dump(opportunity_info, f, indent=4)


def fetch_all_opportunities(testing_mode: bool = None) -> list:
    """
    Fetch all viable arbitrage opportunities, sorted by ROI descending.

    Returns:
        List of opportunity dicts (may be empty).
    """
    if testing_mode is None:
        testing_mode = TESTING_MODE

    print(f"Fetching opportunities (testing_mode={testing_mode})...")

    df_all = fetch_data(_build_query(testing_mode))

    if df_all is None or df_all.empty:
        print(f"No data returned from {OPPORTUNITIES_TABLE}.")
        return []

    df_all = df_all.copy()
    df_all["source_table"] = OPPORTUNITIES_TABLE
    df_all["roi"] = pd.to_numeric(df_all.get("roi"), errors="coerce")
    df_all = df_all.dropna(subset=["roi"])

    if not testing_mode:
        df_all = df_all[df_all["roi"] >= MIN_ROI_THRESHOLD]

    if df_all.empty:
        print("No opportunities found after filtering.")
        return []

    df_all = df_all.sort_values("roi", ascending=False).reset_index(drop=True)

    # Scale wagers
    df_all['wager_under'] = (df_all['wager_under'] * WAGER_SCALE_FACTOR).round(2) + 0.01
    df_all['wager_over'] = (df_all['wager_over'] * WAGER_SCALE_FACTOR).round(2) + 0.01
    df_all['payout'] = (df_all['payout'] * WAGER_SCALE_FACTOR).round(2)
    df_all['arb_ev'] = (df_all['arb_ev'] * WAGER_SCALE_FACTOR).round(2)

    results = _df_to_json_records(df_all)
    print(f"Found {len(results)} candidate opportunities.")
    return results


def fetch_and_prepare_opportunity(testing_mode: bool = None) -> dict:
    """
    Fetch the best arbitrage opportunity, persist it locally, and return it.

    Args:
        testing_mode: Override testing mode. If None, uses TESTING_MODE env var.

    Returns:
        Dictionary with opportunity details, or empty dict if none found.
    """
    opportunity_info = _fetch_best_opportunity(testing_mode)
    if opportunity_info:
        _persist_opportunity_info(opportunity_info)
    return opportunity_info


def _normalize_direction(direction: str) -> str:
    d = (direction or "").strip().lower()
    if d not in {"over", "under"}:
        raise ValueError(f"direction must be 'over' or 'under', got: {direction!r}")
    return d


def build_side_info(info: dict, direction: str) -> dict:
    """Return a reduced dict containing only the requested side (over/under).

    This is used to keep LLM prompts unambiguous by removing the opposite side.
    """
    d = _normalize_direction(direction)
    if d == "over":
        return {
            "direction": "over",
            "player_name": info.get("player_name"),
            "sport_title": info.get("sport_title"),
            "home_team": info.get("home_team"),
            "away_team": info.get("away_team"),
            "market_key": info.get("over_market_key") or info.get("market_key"),
            "line": info.get("over_line"),
            "bookmaker_key": info.get("over_bookmaker_key"),
            "price": info.get("over_price"),
            "wager": info.get("wager_over"),
            "payout": info.get("payout"),
            "fetched_at_utc": info.get("fetched_at_utc"),
        }
    return {
        "direction": "under",
        "player_name": info.get("player_name"),
        "sport_title": info.get("sport_title"),
        "home_team": info.get("home_team"),
        "away_team": info.get("away_team"),
        "market_key": info.get("under_market_key") or info.get("market_key"),
        "line": info.get("under_line"),
        "bookmaker_key": info.get("under_bookmaker_key"),
        "price": info.get("under_price"),
        "wager": info.get("wager_under"),
        "payout": info.get("payout"),
        "fetched_at_utc": info.get("fetched_at_utc"),
    }


def infer_direction_for_book(info: dict, book_key: str) -> str:
    """Infer which side belongs to a given sportsbook key.

    Some providers may use alternate keys (e.g. BetMGM/WilliamsHill), so we allow
    a small set of aliases.
    """
    book = (book_key or "").strip().lower()
    aliases = {
        "betmgm": {"betmgm", "mgm"},
        "fanduel": {"fanduel"},
    }
    book_aliases = aliases.get(book, {book})

    over_book = (info.get("over_bookmaker_key") or "").strip().lower()
    under_book = (info.get("under_bookmaker_key") or "").strip().lower()

    if over_book in book_aliases:
        return "over"
    if under_book in book_aliases:
        return "under"
    # Fallback: if it doesn't match either, default to over (caller can override).
    return "over"