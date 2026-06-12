"""Prediction scoring: Brier, log loss, calibration buckets.

pairs = [(predicted_prob, realized), ...] with realized in {0, 1} (bools ok).
Venue closing lines and model_predictions rows score through the same functions.
"""
from __future__ import annotations

import math

_EPS = 1e-12
N_BUCKETS = 10


def brier_score(pairs: list):
    if not pairs:
        return None
    return sum((float(p) - float(bool(y))) ** 2 for p, y in pairs) / len(pairs)


def log_loss_score(pairs: list):
    if not pairs:
        return None
    total = 0.0
    for p, y in pairs:
        p = min(max(float(p), _EPS), 1.0 - _EPS)
        total += -(math.log(p) if y else math.log(1.0 - p))
    return total / len(pairs)


def calibration_buckets(pairs: list, n_buckets: int = N_BUCKETS) -> list:
    """Equal-width prob buckets; returns [{prob_bucket (lower bound), n,
    predicted_mean, realized_rate}, ...] sorted by bucket."""
    acc: dict = {}
    for p, y in pairs:
        idx = min(int(float(p) * n_buckets), n_buckets - 1)
        lb = round(idx / n_buckets, 10)
        n, sp, sy = acc.get(lb, (0, 0.0, 0.0))
        acc[lb] = (n + 1, sp + float(p), sy + float(bool(y)))
    return [
        {"prob_bucket": lb, "n": n, "predicted_mean": sp / n, "realized_rate": sy / n}
        for lb, (n, sp, sy) in sorted(acc.items())
    ]
