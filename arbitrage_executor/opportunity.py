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
MIN_ROI_THRESHOLD = float(os.getenv("MIN_ROI_THRESHOLD", "-1.0"))
WAGER_SCALE_FACTOR = float(os.getenv("WAGER_SCALE_FACTOR", "0.01"))
TESTING_MODE = os.getenv("TESTING_MODE", "false").lower() == "true"

table_name = "bg_arbitrage_player_props"
alt_table_name = "bg_arbitrage_player_props_alt"


def _build_query(table: str, testing_mode: bool = False) -> str:
    """
    Build query for fetching arbitrage opportunities.

    Args:
        table: Table name to query
        testing_mode: If True, removes strict filters for testing
    """
    # Base query
    # Exclude player+market pairs already executed today.
    executed_today_filter = """
        AND NOT EXISTS (
            SELECT 1 FROM bg_executed_opportunities eo
            WHERE eo.player_name = {tbl}.player_name
            AND eo.market_key = {tbl}.market_key
            AND eo.executed_at_utc >= CURRENT_DATE
        )
    """

    if table == "bg_arbitrage_player_props":
        base_query = f"""
    SELECT player_name,
            sport_title,
            home_team,
            away_team,
            market_key AS under_market_key,
            market_key AS over_market_key,
            under_line,
            over_line,
            under_bookmaker_key,
            over_bookmaker_key,
            under_price,
            over_price,
            wager_under,
            wager_over,
            payout,
            arb_ev,
            roi,
            hours_until_commence,
            fetched_at_utc
    FROM {table}
    WHERE under_line = over_line
        AND under_bookmaker_key  IN ('fanduel', 'betmgm')
        AND over_bookmaker_key  IN ('fanduel', 'betmgm')
        AND under_bookmaker_key != over_bookmaker_key
        AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
    """ + executed_today_filter.format(tbl=table)
    elif table == "bg_arbitrage_player_props_alt":
        base_query = f"""
    SELECT player_name,
            sport_title,
            home_team,
            away_team,
            under_market_key,
            over_market_key,
            under_line,
            over_line,
            under_bookmaker_key,
            over_bookmaker_key,
            under_price,
            over_price,
            wager_under,
            wager_over,
            payout,
            arb_ev,
            roi,
            hours_until_commence,
            fetched_at_utc
    FROM {table}
    WHERE under_line = over_line
        AND under_bookmaker_key  IN ('fanduel', 'betmgm')
        AND over_bookmaker_key  IN ('fanduel', 'betmgm')
        AND under_bookmaker_key != over_bookmaker_key
        AND sport_title IN ('NBA', 'NHL', 'NFL', 'MLB')
    """ + executed_today_filter.format(tbl=table)
       

    if testing_mode:
        # Testing mode: relaxed filters
        query = base_query + f"""
    AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '4 hours'
    AND hours_until_commence > 0
    AND hours_until_commence < 72
ORDER BY fetched_at_utc DESC, roi DESC
LIMIT 20;
"""
    else:
        # Production mode: strict filters
        query = base_query + f"""
    AND fetched_at_utc >= (now() AT TIME ZONE 'utc') - INTERVAL '10 minutes'
    AND hours_until_commence > 0.03
    AND hours_until_commence < 24
    AND roi >= {MIN_ROI_THRESHOLD}
ORDER BY roi DESC
LIMIT 10;
"""

    return query


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

    df_primary = fetch_data(_build_query(table_name, testing_mode))
    df_alt = fetch_data(_build_query(alt_table_name, testing_mode))

    dfs = []
    if df_primary is not None and not df_primary.empty:
        df_primary = df_primary.copy()
        df_primary["source_table"] = table_name
        dfs.append(df_primary)
    if df_alt is not None and not df_alt.empty:
        df_alt = df_alt.copy()
        df_alt["source_table"] = alt_table_name
        dfs.append(df_alt)

    if not dfs:
        print("No data returned from either table.")
        return {}

    df_all = pd.concat(dfs, ignore_index=True)
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

    df_primary = fetch_data(_build_query(table_name, testing_mode))
    df_alt = fetch_data(_build_query(alt_table_name, testing_mode))

    dfs = []
    if df_primary is not None and not df_primary.empty:
        df_primary = df_primary.copy()
        df_primary["source_table"] = table_name
        dfs.append(df_primary)
    if df_alt is not None and not df_alt.empty:
        df_alt = df_alt.copy()
        df_alt["source_table"] = alt_table_name
        dfs.append(df_alt)

    if not dfs:
        print("No data returned from either table.")
        return []

    df_all = pd.concat(dfs, ignore_index=True)
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