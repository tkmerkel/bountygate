"""Closing Line Value (CLV) calculations.

CLV measures how a bet price compares to the sharp closing line,
indicating whether the bettor had an informational edge at bet time.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def clv_from_fair(bet_fair_prob: float, closing_fair_prob: float) -> float:
    """Return CLV expressed in fair-probability terms: closing/bet - 1.

    Positive value means the closing market moved against the bettor
    (closing fair prob is higher than at bet time), which is favourable —
    the bettor beat the close.

    Args:
        bet_fair_prob: No-vig fair probability at bet placement time.
        closing_fair_prob: No-vig fair probability at market close.

    Returns:
        CLV fraction (e.g. 0.03 = +3 % beat-close).
    """
    return closing_fair_prob / bet_fair_prob - 1.0


def clv_from_price(bet_decimal: float, closing_decimal: float) -> float:
    """Return CLV expressed in decimal-odds terms: bet/closing - 1.

    Positive value means the bettor obtained higher odds than the closing
    line (good), i.e. they beat the close.

    Args:
        bet_decimal: Decimal odds obtained at bet placement.
        closing_decimal: Sharp closing decimal odds.

    Returns:
        CLV fraction (e.g. 0.04 = +4 % beat-close on price terms).
    """
    return bet_decimal / closing_decimal - 1.0


def beat_close(bet_fair_prob: float, closing_fair_prob: float) -> bool:
    """Return True when the closing fair probability exceeds the bet-time fair probability.

    This means the market's consensus moved to assign the outcome a higher
    probability *after* the bet was placed, confirming the bettor had the
    correct directional read at a better price.

    Args:
        bet_fair_prob: No-vig fair probability at bet placement.
        closing_fair_prob: No-vig fair probability at market close.

    Returns:
        True if the bettor beat the closing line.
    """
    return closing_fair_prob > bet_fair_prob


def _clv_hash(bg_event_id: str, market_key: str, player_name: str, side: str, line) -> str:
    """MD5 of the natural key fields joined by '|'."""
    raw = f"{bg_event_id}|{market_key}|{player_name}|{side}|{line}"
    return hashlib.md5(raw.encode()).hexdigest()


def compute_clv(tracked_bets: "pd.DataFrame", closing: "pd.DataFrame") -> "pd.DataFrame":
    """Join tracked bets to closing lines and compute CLV metrics.

    Args:
        tracked_bets: DataFrame with columns
            bg_event_id, market_key, player_name, side, line,
            bet_price_decimal, bet_fair_prob.
        closing: DataFrame with columns
            bg_event_id, market_key, player_name, side, line,
            closing_price_decimal, closing_fair_prob.

    Returns:
        DataFrame with all input columns plus:
            clv_pct        — CLV from fair-prob terms (closing/bet - 1)
            beat_close     — bool
            clv_hash       — MD5 of bg_event_id|market_key|player_name|side|line
    """
    import pandas as pd  # noqa: PLC0415

    join_keys = ["bg_event_id", "market_key", "player_name", "side", "line"]

    merged = tracked_bets.merge(closing, on=join_keys, how="inner")

    merged["clv_pct"] = merged.apply(
        lambda r: clv_from_fair(float(r["bet_fair_prob"]), float(r["closing_fair_prob"])),
        axis=1,
    )
    merged["beat_close"] = merged.apply(
        lambda r: beat_close(float(r["bet_fair_prob"]), float(r["closing_fair_prob"])),
        axis=1,
    )
    merged["clv_hash"] = merged.apply(
        lambda r: _clv_hash(
            str(r["bg_event_id"]),
            str(r["market_key"]),
            str(r["player_name"]),
            str(r["side"]),
            r["line"],
        ),
        axis=1,
    )

    return merged
