"""Per-book fair probabilities and the sharpness-weighted consensus blend.

Pure functions over two-way market prices; devig math comes from analytics.devig.
"""
from __future__ import annotations

from bountygate.analytics.devig import devig_all

DEFAULT_WEIGHT = 0.5  # weight for books absent from the weights dict


def book_fair_probs(prices_by_book: dict, names: list) -> dict:
    """Fair probs per book per devig method for one two-way group.

    prices_by_book: {bookmaker: {outcome_name: decimal_price}}
    names: the ordered two-way pair [name0, name1].
    Returns {bookmaker: {'mult'|'power'|'shin': {name: fair_prob}}}; books
    missing either side (devig_all -> None) are skipped.
    """
    n0, n1 = names
    out: dict = {}
    for book, prices in prices_by_book.items():
        d = devig_all(prices.get(n0), prices.get(n1))
        if d is None:
            continue
        out[book] = {
            "mult": {n0: d["fair_prob_over_mult"], n1: d["fair_prob_under_mult"]},
            "power": {n0: d["fair_prob_over_power"], n1: d["fair_prob_under_power"]},
            "shin": {n0: d["fair_prob_over_shin"], n1: d["fair_prob_under_shin"]},
        }
    return out


def weighted_consensus(probs_by_book: dict, weights: dict | None = None):
    """Weighted mean of per-book fair probs, renormalized to sum 1.

    probs_by_book: {bookmaker: {outcome_name: fair_prob}} (one method's probs).
    weights: {bookmaker: weight}; missing books get DEFAULT_WEIGHT; None means
    equal weights (reproduces the unweighted consensus). Books not quoting every
    outcome are skipped. Returns {outcome_name: prob} or None if nothing usable.
    """
    if not probs_by_book:
        return None
    names = set()
    for probs in probs_by_book.values():
        names |= set(probs)
    totals = {n: 0.0 for n in names}
    wsum = 0.0
    for book, probs in probs_by_book.items():
        if any(probs.get(n) is None for n in names):
            continue
        w = 1.0 if weights is None else float(weights.get(book, DEFAULT_WEIGHT))
        if w <= 0:
            continue
        wsum += w
        for n in names:
            totals[n] += w * float(probs[n])
    if wsum <= 0:
        return None
    cons = {n: totals[n] / wsum for n in names}
    s = sum(cons.values())
    return {n: v / s for n, v in cons.items()}
