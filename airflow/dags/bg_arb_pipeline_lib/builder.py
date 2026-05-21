"""Symmetric arb builder.

Pure function: stage-lines DataFrame in, opportunities DataFrame out.
Generates all four pairing directions (std_std, std_alt, alt_std, alt_alt)
via a cartesian self-join of unders × overs on the join key
(event_id, player_name, canonical_market, line) with the constraint that
under_book != over_book.
"""
from __future__ import annotations

import pandas as pd

from bg_arb_pipeline_lib.hashing import derive_pairing_type, opportunity_hash
from bg_arb_pipeline_lib.markets import ARB_MARKET_BLACKLIST

_ALT_SUFFIX = "_alternate"


def _strip_alt_suffix(market_key: str) -> str:
    if market_key.endswith(_ALT_SUFFIX):
        return market_key[: -len(_ALT_SUFFIX)]
    return market_key


def build_opportunities(
    lines: pd.DataFrame,
    *,
    base_wager: float = 100.0,
    min_roi: float = 0.0,
) -> pd.DataFrame:
    """Build arb-able pairs from a per-(book, market, line, side) DataFrame.

    ``min_roi`` is a **storage-protection floor**, NOT a policy gate. The
    cartesian self-join produces ~12-20 candidate pairs per
    (event, player, market, line) tuple, and most have negative ROI from
    book overround. Default ``0.0`` drops those before write to keep
    `bg_arbitrage_opportunities` and history-table cardinality sane.

    The **policy gate** is the executor's ``MIN_ROI_THRESHOLD`` env var
    (see arbitrage_executor/opportunity.py). Lower ``min_roi`` here to
    surface near-miss arbs for analysis — but be aware of the storage
    and query-cost impact described in arbitrage_executor/LOGIC.md.

    Returns a DataFrame whose columns match bg_arbitrage_opportunities.
    """
    if lines is None or lines.empty:
        return pd.DataFrame()

    work = lines.copy()
    # Postgres NUMERIC columns come back as Decimal; coerce to float so pandas
    # arithmetic works downstream. Unit tests use float, but the live pipeline
    # reads from bg_arb_stage_lines (NUMERIC columns).
    for numeric_col in ("price", "line"):
        if numeric_col in work.columns:
            work[numeric_col] = pd.to_numeric(work[numeric_col], errors="coerce")
    work = work.dropna(subset=["price", "line"])
    work["canonical_market"] = work["market_key"].astype(str).map(_strip_alt_suffix)
    work = work[~work["market_key"].isin(ARB_MARKET_BLACKLIST)]
    if work.empty:
        return pd.DataFrame()

    unders = work[work["side"] == "under"].copy()
    overs = work[work["side"] == "over"].copy()
    if unders.empty or overs.empty:
        return pd.DataFrame()

    join_cols = ["event_id", "player_name", "canonical_market", "line"]
    merged = unders.merge(
        overs,
        on=join_cols,
        suffixes=("_u", "_o"),
        how="inner",
    )

    merged = merged[merged["bookmaker_key_u"] != merged["bookmaker_key_o"]]
    if merged.empty:
        return pd.DataFrame()

    implied_under = 1.0 / merged["price_u"]
    implied_over = 1.0 / merged["price_o"]
    overround = implied_under + implied_over

    merged = merged.assign(
        wager_under=base_wager / overround * implied_under,
        wager_over=base_wager / overround * implied_over,
        payout=base_wager / overround,
    )
    merged["arb_ev"] = merged["payout"] - base_wager
    merged["roi"] = merged["arb_ev"] / base_wager

    pre_gate_n = len(merged)
    merged = merged[merged["roi"] > min_roi]
    post_gate_n = len(merged)
    print(
        f"[builder] roi-gate at {min_roi:+.4f}: "
        f"{pre_gate_n} -> {post_gate_n} ({pre_gate_n - post_gate_n} dropped)"
    )
    if merged.empty:
        return pd.DataFrame()

    commence_u = pd.to_datetime(merged["commence_time_utc_u"], utc=True)
    fetched_u = pd.to_datetime(merged["fetched_at_utc_u"], utc=True)
    merged["hours_until_commence"] = (commence_u - fetched_u).dt.total_seconds() / 3600.0

    merged["pairing_type"] = merged.apply(
        lambda r: derive_pairing_type(r["market_key_u"], r["market_key_o"]), axis=1
    )

    out = pd.DataFrame({
        "event_id": merged["event_id"],
        "sport_title": merged["sport_title_u"],
        "home_team": merged["home_team_u"],
        "away_team": merged["away_team_u"],
        "player_name": merged["player_name"],
        "canonical_market": merged["canonical_market"],
        "pairing_type": merged["pairing_type"],
        "under_book": merged["bookmaker_key_u"],
        "under_market_key": merged["market_key_u"],
        "under_line": merged["line"],
        "under_price": merged["price_u"],
        "over_book": merged["bookmaker_key_o"],
        "over_market_key": merged["market_key_o"],
        "over_line": merged["line"],
        "over_price": merged["price_o"],
        "wager_under": merged["wager_under"],
        "wager_over": merged["wager_over"],
        "payout": merged["payout"],
        "arb_ev": merged["arb_ev"],
        "roi": merged["roi"],
        "hours_until_commence": merged["hours_until_commence"],
        "fetched_at_utc": merged["fetched_at_utc_u"],
    })

    out["opportunity_hash"] = out.apply(
        lambda r: opportunity_hash({
            "event_id": r["event_id"],
            "player_name": r["player_name"],
            "under_book": r["under_book"],
            "under_market_key": r["under_market_key"],
            "over_book": r["over_book"],
            "over_market_key": r["over_market_key"],
            "under_line": r["under_line"],
            "under_price": r["under_price"],
            "over_price": r["over_price"],
        }),
        axis=1,
    )

    cols = ["opportunity_hash"] + [c for c in out.columns if c != "opportunity_hash"]
    return out[cols].reset_index(drop=True)
