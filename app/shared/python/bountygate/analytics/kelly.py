"""
Fractional Kelly stake-sizing helpers.

All functions operate on probability + decimal odds and are pure math —
no IO, no database access.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def kelly_fraction(p: float, decimal_odds: float) -> float:
    """Return the full Kelly fraction of bankroll to stake.

    Formula: f* = (p * d - 1) / (d - 1)

    Negative fractions (negative-EV bets) are clamped to 0.

    Parameters
    ----------
    p:
        Fair (no-vig) probability of winning, in (0, 1].
    decimal_odds:
        Soft-book decimal odds (e.g. 1.91 for -110 American).
    """
    numerator = p * decimal_odds - 1.0
    denominator = decimal_odds - 1.0
    if denominator <= 0.0:
        return 0.0
    return max(0.0, numerator / denominator)


def quarter_kelly(p: float, decimal_odds: float) -> float:
    """Return one-quarter of the full Kelly fraction (standard conservative sizing).

    Formula: f_quarter = 0.25 * kelly_fraction(p, d)
    """
    return 0.25 * kelly_fraction(p, decimal_odds)


def capped_stake(kelly_frac: float, bankroll: float, *, cap_frac: float = 0.02) -> float:
    """Return the dollar stake after applying a hard cap on bankroll percentage.

    stake = min(kelly_frac, cap_frac) * bankroll

    The cap prevents runaway sizing on high-edge bets.  Result is floored at 0.

    Parameters
    ----------
    kelly_frac:
        Kelly fraction (e.g. quarter_kelly output).  Already dimensionless.
    bankroll:
        Total bankroll in dollars.
    cap_frac:
        Hard ceiling as a fraction of bankroll (default 0.02 = 2%).
    """
    return max(0.0, min(kelly_frac, cap_frac) * bankroll)


def size_opportunities(
    df: "pd.DataFrame",
    *,
    bankroll: float,
    kelly_multiplier: float = 0.25,
    cap_frac: float = 0.02,
) -> "pd.DataFrame":
    """Add Kelly sizing columns to an EV-opportunity DataFrame.

    Expects columns: fair_prob, soft_price_decimal (decimal odds at the soft book).

    Adds columns:
        kelly_full    -- full Kelly fraction
        kelly_quarter -- kelly_multiplier * kelly_full (default quarter Kelly)
        stake_capped  -- dollar stake after applying cap_frac ceiling

    Parameters
    ----------
    df:
        DataFrame from build_ev_opportunities (or any frame with fair_prob +
        soft_price_decimal columns).
    bankroll:
        Total bankroll in dollars (typically sum of legal-book account balances).
    kelly_multiplier:
        Fractional multiplier applied to full Kelly before capping (default 0.25).
    cap_frac:
        Maximum stake as a fraction of bankroll (default 0.02 = 2%).
    """
    import pandas as pd

    out = df.copy()

    out["kelly_full"] = out.apply(
        lambda r: kelly_fraction(float(r["fair_prob"]), float(r["soft_price_decimal"])),
        axis=1,
    )
    out["kelly_quarter"] = out["kelly_full"] * kelly_multiplier
    out["stake_capped"] = out["kelly_quarter"].apply(
        lambda f: capped_stake(f, bankroll, cap_frac=cap_frac)
    )

    return out
