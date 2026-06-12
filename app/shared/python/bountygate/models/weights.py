"""Sharpness weights for the consensus blend, from accumulated venue Brier scores.

Pinnacle-anchored prior until a venue has enough scored games:
prior weight 1.0 for pinnacle, 0.5 for everyone else. Once >= min_games,
weight = min_qualified_brier / venue_brier (sharpest qualified venue = 1.0).
"""
from __future__ import annotations

import os

PRIOR_SHARP = 1.0
PRIOR_OTHER = 0.5
SHARP_VENUE = "pinnacle"
_BRIER_FLOOR = 1e-6


def min_games_default() -> int:
    return int(os.environ.get("BG_SHARPNESS_MIN_GAMES", "200"))


def sharpness_weights(stats: dict, *, min_games: int | None = None) -> dict:
    """stats: {venue: {'brier': float, 'n_games': int}} -> {venue: weight}.

    Venues absent from stats simply aren't in the result; consumers fall back
    to fair.DEFAULT_WEIGHT for unknown books.
    """
    if min_games is None:
        min_games = min_games_default()
    qualified = {
        v: max(float(s["brier"]), _BRIER_FLOOR)
        for v, s in stats.items()
        if s.get("brier") is not None and (s.get("n_games") or 0) >= min_games
    }
    out: dict = {}
    min_b = min(qualified.values()) if qualified else None
    for venue in stats:
        if venue in qualified:
            out[venue] = min_b / qualified[venue]
        else:
            out[venue] = PRIOR_SHARP if venue == SHARP_VENUE else PRIOR_OTHER
    return out
