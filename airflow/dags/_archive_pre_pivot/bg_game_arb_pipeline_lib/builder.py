"""Symmetric arb builder for game-line markets (h2h, spreads, totals).

Pure function: stage-lines DataFrame in, opportunities DataFrame out.
A 2-way arb pairs two legs from different books where:
  - h2h:     outcomes are the two opposing teams (no point on either side)
  - spreads: outcomes are the two opposing teams, leg_a.point == -leg_b.point
  - totals:  outcomes are 'Over' and 'Under', leg_a.point == leg_b.point

Standard arb math: overround = 1/price_a + 1/price_b. If < 1, arb exists.
Wagers split base_wager proportionally so payouts match across legs.

All three pair functions emit the same column shape — event_id shared,
everything else suffixed _a / _b — so downstream code is uniform.
"""
from __future__ import annotations

import pandas as pd

from bg_game_arb_pipeline_lib.hashing import opportunity_hash


def _split_ab(market_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rename every non-join column with `_a` and `_b` so the cartesian self-merge
    produces predictable column names regardless of pandas' suffix heuristics."""
    a = market_df.rename(
        columns={c: f"{c}_a" for c in market_df.columns if c != "event_id"}
    )
    b = market_df.rename(
        columns={c: f"{c}_b" for c in market_df.columns if c != "event_id"}
    )
    return a, b


def _pair_h2h(market_df: pd.DataFrame) -> pd.DataFrame:
    """Pair the two teams' moneylines across different books."""
    if market_df.empty:
        return pd.DataFrame()
    a, b = _split_ab(market_df)
    merged = a.merge(b, on="event_id")
    merged = merged[merged["bookmaker_key_a"] != merged["bookmaker_key_b"]]
    merged = merged[merged["outcome_a"] != merged["outcome_b"]]
    return merged


def _pair_spreads(market_df: pd.DataFrame) -> pd.DataFrame:
    """Pair team-A spread with team-B spread where points mirror, different books."""
    if market_df.empty:
        return pd.DataFrame()
    a, b = _split_ab(market_df)
    merged = a.merge(b, on="event_id")
    merged = merged[merged["bookmaker_key_a"] != merged["bookmaker_key_b"]]
    merged = merged[merged["outcome_a"] != merged["outcome_b"]]
    merged = merged[merged["point_a"] == -merged["point_b"]]
    return merged


def _pair_totals(market_df: pd.DataFrame) -> pd.DataFrame:
    """Pair Over leg with Under leg at the same point, different books."""
    if market_df.empty:
        return pd.DataFrame()
    a, b = _split_ab(market_df)
    merged = a.merge(b, on="event_id")
    merged = merged[merged["bookmaker_key_a"] != merged["bookmaker_key_b"]]
    outcome_a_lower = merged["outcome_a"].str.lower()
    outcome_b_lower = merged["outcome_b"].str.lower()
    merged = merged[outcome_a_lower != outcome_b_lower]
    merged = merged[merged["point_a"] == merged["point_b"]]
    # Defense: real totals outcomes are 'Over' / 'Under'.
    outcome_a_lower = merged["outcome_a"].str.lower()
    outcome_b_lower = merged["outcome_b"].str.lower()
    merged = merged[outcome_a_lower.isin({"over", "under"})]
    merged = merged[outcome_b_lower.isin({"over", "under"})]
    return merged


def _drop_symmetric_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """A cartesian self-merge produces each arb twice (A×B and B×A).

    Keep one copy by requiring leg_a_book < leg_b_book alphabetically.
    """
    if df.empty:
        return df
    return df[df["bookmaker_key_a"] < df["bookmaker_key_b"]].copy()


def _compute_economics(merged: pd.DataFrame, base_wager: float) -> pd.DataFrame:
    if merged.empty:
        return merged
    implied_a = 1.0 / merged["price_a"]
    implied_b = 1.0 / merged["price_b"]
    overround = implied_a + implied_b
    merged = merged.assign(
        wager_leg_a=base_wager / overround * implied_a,
        wager_leg_b=base_wager / overround * implied_b,
        payout=base_wager / overround,
    )
    merged["arb_ev"] = merged["payout"] - base_wager
    merged["roi"] = merged["arb_ev"] / base_wager
    return merged[merged["roi"] > 0].copy()


def build_game_opportunities(lines: pd.DataFrame, *, base_wager: float = 100.0) -> pd.DataFrame:
    """Build h2h/spreads/totals arb pairs from a stage-lines DataFrame.

    Returns a DataFrame whose columns match bg_arbitrage_game_opportunities.
    """
    if lines is None or lines.empty:
        return pd.DataFrame()

    work = lines.copy()
    for numeric_col in ("price", "point"):
        if numeric_col in work.columns:
            work[numeric_col] = pd.to_numeric(work[numeric_col], errors="coerce")
    work = work.dropna(subset=["price"])
    work = work[work["price"] > 0]
    if work.empty:
        return pd.DataFrame()

    pieces: list[pd.DataFrame] = []
    for market_key, pair_fn, requires_point in (
        ("h2h", _pair_h2h, False),
        ("spreads", _pair_spreads, True),
        ("totals", _pair_totals, True),
    ):
        market_df = work[work["market_key"] == market_key].copy()
        if market_df.empty:
            continue
        if requires_point:
            market_df = market_df.dropna(subset=["point"])
            if market_df.empty:
                continue
        merged = pair_fn(market_df)
        if merged.empty:
            continue
        merged = _drop_symmetric_duplicates(merged)
        merged = _compute_economics(merged, base_wager=base_wager)
        if merged.empty:
            continue
        pieces.append(merged.assign(_market_key=market_key))

    if not pieces:
        return pd.DataFrame()

    all_merged = pd.concat(pieces, ignore_index=True)

    commence = pd.to_datetime(all_merged["commence_time_utc_a"], utc=True)
    fetched = pd.to_datetime(all_merged["fetched_at_utc_a"], utc=True)
    all_merged["hours_until_commence"] = (commence - fetched).dt.total_seconds() / 3600.0
    all_merged = all_merged[all_merged["hours_until_commence"] > 0]
    if all_merged.empty:
        return pd.DataFrame()

    out = pd.DataFrame({
        "event_id": all_merged["event_id"],
        "sport_key": all_merged["sport_key_a"],
        "sport_title": all_merged["sport_title_a"],
        "home_team": all_merged["home_team_a"],
        "away_team": all_merged["away_team_a"],
        "commence_time_utc": all_merged["commence_time_utc_a"],
        "market_key": all_merged["_market_key"],
        "leg_a_book": all_merged["bookmaker_key_a"],
        "leg_a_outcome": all_merged["outcome_a"],
        "leg_a_point": all_merged["point_a"],
        "leg_a_price": all_merged["price_a"],
        "leg_b_book": all_merged["bookmaker_key_b"],
        "leg_b_outcome": all_merged["outcome_b"],
        "leg_b_point": all_merged["point_b"],
        "leg_b_price": all_merged["price_b"],
        "wager_leg_a": all_merged["wager_leg_a"],
        "wager_leg_b": all_merged["wager_leg_b"],
        "payout": all_merged["payout"],
        "arb_ev": all_merged["arb_ev"],
        "roi": all_merged["roi"],
        "hours_until_commence": all_merged["hours_until_commence"],
        "fetched_at_utc": all_merged["fetched_at_utc_a"],
    })

    out["opportunity_hash"] = out.apply(
        lambda r: opportunity_hash({
            "event_id": r["event_id"],
            "market_key": r["market_key"],
            "leg_a_book": r["leg_a_book"],
            "leg_a_outcome": r["leg_a_outcome"],
            "leg_a_point": r["leg_a_point"],
            "leg_a_price": r["leg_a_price"],
            "leg_b_book": r["leg_b_book"],
            "leg_b_outcome": r["leg_b_outcome"],
            "leg_b_point": r["leg_b_point"],
            "leg_b_price": r["leg_b_price"],
        }),
        axis=1,
    )

    cols = ["opportunity_hash"] + [c for c in out.columns if c != "opportunity_hash"]
    return out[cols].reset_index(drop=True)
