"""
Expected-value helpers for soft-book price evaluation.

All functions operate on decimal odds and fair (no-vig) probabilities.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def edge(fair_prob: float, soft_decimal_odds: float) -> float:
    """Return raw EV per unit staked: fair_prob * decimal_odds - 1.

    Positive edge means the bet is priced better than fair value.

    Formula: E = p * d - 1
    """
    return fair_prob * soft_decimal_odds - 1.0


def edge_pct(fair_prob: float, soft_decimal_odds: float) -> float:
    """Return edge as a percentage (edge * 100).

    Formula: E% = (p * d - 1) * 100
    """
    return edge(fair_prob, soft_decimal_odds) * 100.0


def no_vig_price(fair_prob: float) -> float:
    """Return the fair decimal price implied by fair_prob (Pinnacle no-vig price).

    Formula: d_fair = 1 / p
    """
    return 1.0 / fair_prob


def is_actionable(edge_value: float, threshold: float = 0.025) -> bool:
    """Return True when edge meets or exceeds the minimum threshold.

    Default threshold = 0.025 (2.5% edge).
    """
    return edge_value >= threshold


def build_ev_opportunities(
    soft_quotes: "pd.DataFrame",
    fair_probs: "pd.DataFrame",
    *,
    soft_books: list[str],
    threshold: float = 0.025,
) -> "pd.DataFrame":
    """Join soft-book quotes against fair probs, filter by edge, and return ranked EV rows.

    Parameters
    ----------
    soft_quotes:
        Columns required: bg_event_id, sport_key, market_key, player_name, side,
        line, bookmaker_key, price_decimal, commence_at_utc, fetched_at_utc.
    fair_probs:
        Columns required: bg_event_id, market_key, player_name, line,
        fair_prob_over_shin, fair_prob_over_mult (and under variants),
        disagreement_flag.  Side ('over'/'under') is encoded in column name suffixes.
    soft_books:
        List of bookmaker_key values to consider (the 4 legal soft books).
    threshold:
        Minimum edge to include in output (default 0.025 = 2.5%).

    Returns
    -------
    pd.DataFrame with columns:
        ev_hash, bg_event_id, sport_key, market_key, player_name, side, line,
        soft_book, soft_price_decimal, fair_prob, fair_method,
        edge, edge_pct, pinnacle_no_vig_price,
        disagreement_flag, commence_at_utc, fetched_at_utc, hours_until_commence.
    """
    import pandas as pd

    # Filter to the configured soft books only.
    quotes = soft_quotes[soft_quotes["bookmaker_key"].isin(soft_books)].copy()

    if quotes.empty or fair_probs.empty:
        return pd.DataFrame()

    # Normalise side to lowercase for consistent joining.
    quotes["side"] = quotes["side"].str.lower().str.strip()

    # Build a "tidy" fair_prob frame: one row per (bg_event_id, market_key,
    # player_name, line, side) with a single fair_prob value chosen by
    # preference order Shin > mult.
    fp = fair_probs.copy()

    tidy_rows: list[dict] = []
    for _, row in fp.iterrows():
        base = {
            "bg_event_id": row["bg_event_id"],
            "market_key": row["market_key"],
            "player_name": row["player_name"],
            "line": row["line"],
            "disagreement_flag": row.get("disagreement_flag", False),
        }
        for side in ("over", "under"):
            shin_col = f"fair_prob_{side}_shin"
            mult_col = f"fair_prob_{side}_mult"
            shin_val = row.get(shin_col)
            mult_val = row.get(mult_col)
            # Prefer Shin when available and valid.
            if shin_val is not None and not pd.isna(shin_val) and float(shin_val) > 0:
                prob = float(shin_val)
                method = "shin"
            elif mult_val is not None and not pd.isna(mult_val) and float(mult_val) > 0:
                prob = float(mult_val)
                method = "mult"
            else:
                continue
            tidy_rows.append({**base, "side": side, "fair_prob": prob, "fair_method": method})

    if not tidy_rows:
        return pd.DataFrame()

    tidy_fp = pd.DataFrame(tidy_rows)

    # Join on the four-column natural key.
    merged = quotes.merge(
        tidy_fp,
        on=["bg_event_id", "market_key", "player_name", "line", "side"],
        how="inner",
    )

    if merged.empty:
        return pd.DataFrame()

    # Compute EV metrics.
    merged["edge"] = merged.apply(
        lambda r: edge(r["fair_prob"], r["price_decimal"]), axis=1
    )
    merged["edge_pct"] = merged["edge"] * 100.0
    merged["pinnacle_no_vig_price"] = merged["fair_prob"].apply(no_vig_price)

    # Filter by threshold.
    result = merged[merged["edge"] >= threshold].copy()

    if result.empty:
        return pd.DataFrame()

    # Compute hours until commence.
    result["hours_until_commence"] = (
        (result["commence_at_utc"] - result["fetched_at_utc"])
        .dt.total_seconds()
        .div(3600)
    )

    # Build stable ev_hash: md5 of pipe-delimited key fields.
    def _ev_hash(row: "pd.Series") -> str:
        key = "|".join(
            str(v)
            for v in [
                row["bg_event_id"],
                row["market_key"],
                row["player_name"],
                row["side"],
                row["line"],
                row["bookmaker_key"],
            ]
        )
        return hashlib.md5(key.encode()).hexdigest()

    result["ev_hash"] = result.apply(_ev_hash, axis=1)

    output_cols = [
        "ev_hash",
        "bg_event_id",
        "sport_key",
        "market_key",
        "player_name",
        "side",
        "line",
        "bookmaker_key",
        "price_decimal",
        "fair_prob",
        "fair_method",
        "edge",
        "edge_pct",
        "pinnacle_no_vig_price",
        "disagreement_flag",
        "commence_at_utc",
        "fetched_at_utc",
        "hours_until_commence",
    ]
    # Rename bookmaker_key -> soft_book and price_decimal -> soft_price_decimal
    # to match the mart schema described in the plan.
    result = result.rename(
        columns={"bookmaker_key": "soft_book", "price_decimal": "soft_price_decimal"}
    )
    output_cols = [
        c.replace("bookmaker_key", "soft_book").replace(
            "price_decimal", "soft_price_decimal"
        )
        for c in output_cols
    ]

    available = [c for c in output_cols if c in result.columns]
    return result[available].reset_index(drop=True)
