"""Venue fee models for book-x-venue arbitrage detection.

Kalshi taker fee: fee = ceil_to_cent(rate * contracts * P * (1-P)).
Source: Kalshi Fee Schedule (https://kalshi.com/docs/kalshi-fee-schedule.pdf,
"Fee Schedule for Feb 2026") + Kalshi Help Center "Fees"
(https://help.kalshi.com/en/articles/13823805-fees) and the third-party
breakdown at https://marketmath.io/blog/kalshi-fees-guide-2026.
Retrieved 2026-06-12.

Verification result: the general taker fee is `roundup(0.07 * C * P * (1-P))`,
ceiling-rounded to the cent, and SPORTS / event-contract markets fall in this
general 0.07 bucket. Only specific financial-index markets (e.g. S&P 500 /
Nasdaq-100) use a reduced 0.035 multiplier, and some premium categories (crypto)
use a higher one; sports are the plain 0.07 case. Kalshi began charging maker
fees after April 2025 (~0.0175 multiplier, ~1/4 of the taker fee), but this
model intentionally charges the TAKER fee on both venue legs: it is the
conservative assumption for detection (we cannot guarantee a resting limit
order fills, so we never under-count cost).

Rate overridable via BG_KALSHI_FEE_RATE (default 0.07).
Polymarket charges no trading fee on CLOB trades.
"""
from __future__ import annotations

import math
import os


def kalshi_taker_fee(price: float, contracts: int = 1, rate: float | None = None) -> float:
    """Taker fee in dollars, ceiling-rounded to the cent (conservative for detection)."""
    r = rate if rate is not None else float(os.getenv("BG_KALSHI_FEE_RATE", "0.07"))
    raw = r * contracts * price * (1.0 - price)
    # round() guards binary-float epsilon above exact cents (e.g. C=4, p=0.5 -> 7.000000000000001)
    return math.ceil(round(raw * 100.0, 9)) / 100.0


def venue_effective_cost(price: float, venue_key: str) -> float:
    """Cost to acquire $1 of payout: ask + taker fee. Polymarket: no trading fee."""
    if venue_key == "kalshi":
        return price + kalshi_taker_fee(price)
    return price


def book_venue_roi(book_decimal: float, venue_ask: float, venue_key: str) -> tuple[float, float]:
    """(roi_pre_fee, fee_adjusted_roi) with both legs normalized to $1 payout.

    Book leg cost = 1/decimal. Venue leg cost = ask + fee(ask). Arb iff fee_adjusted_roi > 0.

    Fee is computed per-contract (C=1) and ceil-rounded, so near-margin arbs are detected
    pessimistically — the true multi-contract fee per contract is lower.
    """
    t_pre = 1.0 / book_decimal + venue_ask
    t_fee = 1.0 / book_decimal + venue_effective_cost(venue_ask, venue_key)
    return (1.0 / t_pre - 1.0, 1.0 / t_fee - 1.0)
