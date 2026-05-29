"""Line-movement signals for detecting sharp-money action.

These functions identify divergence between soft-book prices and sharp-book
prices, which is a leading indicator of informed betting activity.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

_EPSILON = 1e-6  # minimum meaningful decimal-odds move


def soft_vs_sharp_gap(soft_fair_prob: float, sharp_fair_prob: float) -> float:
    """Return the difference between soft and sharp no-vig fair probabilities.

    A positive gap (soft > sharp) means the soft book is pricing the outcome
    as more likely than the sharp book — the soft book may be a value target.

    Args:
        soft_fair_prob: No-vig fair probability from the soft (recreational) book.
        sharp_fair_prob: No-vig fair probability from the sharp book (e.g. Pinnacle).

    Returns:
        Probability-unit gap: soft_fair_prob - sharp_fair_prob.
    """
    return soft_fair_prob - sharp_fair_prob


def reverse_line_movement(
    soft_open: float | None,
    soft_close: float | None,
    sharp_open: float | None,
    sharp_close: float | None,
) -> bool:
    """Detect reverse line movement (RLM): sharp line moves opposite to soft line.

    RLM is a signal that sharp money is fading the public; the sharp book's
    line moves against the direction the soft book moved.

    Args:
        soft_open: Opening decimal odds at the soft book.
        soft_close: Closing decimal odds at the soft book.
        sharp_open: Opening decimal odds at the sharp book.
        sharp_close: Closing decimal odds at the sharp book.

    Returns:
        True when all inputs are valid, the sharp move is meaningful
        (abs change > epsilon), and the sharp direction is opposite to
        the soft direction.
    """
    # Guard against None / zero inputs
    if (
        soft_open is None
        or soft_close is None
        or sharp_open is None
        or sharp_close is None
    ):
        return False
    try:
        soft_open = float(soft_open)
        soft_close = float(soft_close)
        sharp_open = float(sharp_open)
        sharp_close = float(sharp_close)
    except (TypeError, ValueError):
        return False

    if soft_open == 0 or soft_close == 0 or sharp_open == 0 or sharp_close == 0:
        return False

    sharp_move = sharp_close - sharp_open
    soft_move = soft_close - soft_open

    # Require a meaningful sharp move
    if abs(sharp_move) <= _EPSILON:
        return False

    # RLM: sharp and soft moved in opposite directions
    def _sign(x: float) -> int:
        if x > _EPSILON:
            return 1
        if x < -_EPSILON:
            return -1
        return 0

    return _sign(sharp_move) != _sign(soft_move)


def _movement_hash(
    bg_event_id: str, market_key: str, player_name: str, side: str, line
) -> str:
    """MD5 of the natural key fields joined by '|'."""
    raw = f"{bg_event_id}|{market_key}|{player_name}|{side}|{line}"
    return hashlib.md5(raw.encode()).hexdigest()


def compute_line_movement(snapshot_history: "pd.DataFrame") -> "pd.DataFrame":
    """Summarise line-movement signals across a snapshot history.

    For each unique (bg_event_id, market_key, player_name, side, line):
      - Split rows into soft and sharp subsets based on ``is_sharp``.
      - Take the earliest and latest price for each subset.
      - Compute soft_vs_sharp_gap (using the *latest* sharp fair prob is not
        available here, so the gap is computed on price terms: latest soft
        price vs latest sharp price as decimal odds).
      - Compute reverse_line_movement between the opening and closing
        decimal odds of each subset.

    Args:
        snapshot_history: DataFrame with columns
            bg_event_id, market_key, player_name, side, line,
            bookmaker_key, is_sharp, price_decimal, fetched_at_utc.

    Returns:
        DataFrame with one row per (bg_event_id, market_key, player_name,
        side, line) and columns:
            soft_open, soft_close, sharp_open, sharp_close,
            soft_vs_sharp_gap, reverse_line_movement,
            window_start, window_end, movement_hash.
    """
    import pandas as pd  # noqa: PLC0415

    key_cols = ["bg_event_id", "market_key", "player_name", "side", "line"]

    df = snapshot_history.copy()
    df["fetched_at_utc"] = pd.to_datetime(df["fetched_at_utc"], utc=False)
    df["price_decimal"] = df["price_decimal"].astype(float)

    rows: list[dict] = []

    for group_key, group in df.groupby(key_cols, sort=False):
        bg_event_id, market_key, player_name, side, line = group_key

        soft = group[~group["is_sharp"]].sort_values("fetched_at_utc")
        sharp = group[group["is_sharp"]].sort_values("fetched_at_utc")

        soft_open = float(soft["price_decimal"].iloc[0]) if len(soft) > 0 else None
        soft_close = float(soft["price_decimal"].iloc[-1]) if len(soft) > 0 else None
        sharp_open = float(sharp["price_decimal"].iloc[0]) if len(sharp) > 0 else None
        sharp_close = float(sharp["price_decimal"].iloc[-1]) if len(sharp) > 0 else None

        # soft_vs_sharp_gap in price terms (latest): higher decimal = lower implied prob
        # gap expressed as soft_price - sharp_price (positive = soft offering more value)
        if soft_close is not None and sharp_close is not None:
            gap = soft_vs_sharp_gap(
                1.0 / soft_close if soft_close != 0 else 0.0,
                1.0 / sharp_close if sharp_close != 0 else 0.0,
            )
        else:
            gap = None

        rlm = reverse_line_movement(soft_open, soft_close, sharp_open, sharp_close)

        all_times = group["fetched_at_utc"]
        window_start = all_times.min()
        window_end = all_times.max()

        rows.append(
            {
                "bg_event_id": bg_event_id,
                "market_key": market_key,
                "player_name": player_name,
                "side": side,
                "line": line,
                "soft_open": soft_open,
                "soft_close": soft_close,
                "sharp_open": sharp_open,
                "sharp_close": sharp_close,
                "soft_vs_sharp_gap": gap,
                "reverse_line_movement": rlm,
                "window_start": window_start,
                "window_end": window_end,
                "movement_hash": _movement_hash(
                    str(bg_event_id),
                    str(market_key),
                    str(player_name),
                    str(side),
                    line,
                ),
            }
        )

    return pd.DataFrame(rows)
