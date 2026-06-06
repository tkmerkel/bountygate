"""Pure computation of sportsbook EV + arb signals from latest odds rows.

Input rows: dicts with keys event_id, market_type, bookmaker, outcome_name,
decimal_price (one row per book/outcome, already reduced to the latest snapshot).
Output: list of signal dicts ready to insert into mart_edge_signals.
Reuses the venue-agnostic analytics primitives.
"""
from __future__ import annotations

from collections import defaultdict

from bountygate.analytics.consensus import no_vig_consensus
from bountygate.analytics.devig import implied_prob, multiplicative_devig
from bountygate.analytics.ev import edge as ev_edge
from bountygate.analytics.ev import is_actionable
from bountygate.analytics.kelly import quarter_kelly

PINNACLE = "pinnacle"


def _fair_probs(group_by_book: dict, names: list[str]):
    """Return {outcome_name: fair_prob} via Pinnacle no-vig, else multi-book consensus.

    names is the ordered two-way pair [name0, name1]. group_by_book maps
    bookmaker -> {outcome_name: decimal_price}.
    """
    n0, n1 = names
    pin = group_by_book.get(PINNACLE)
    if pin and pin.get(n0) and pin.get(n1):
        p0 = implied_prob(pin[n0])
        p1 = implied_prob(pin[n1])
        f0, f1 = multiplicative_devig(p0, p1)
        return {n0: f0, n1: f1}
    over_odds, under_odds = [], []
    for book, prices in group_by_book.items():
        if book == PINNACLE:
            continue
        over_odds.append(prices.get(n0))
        under_odds.append(prices.get(n1))
    consensus = no_vig_consensus(over_odds, under_odds)
    if consensus is None:
        return None
    return {n0: consensus[0], n1: consensus[1]}


def compute_edge_signals(rows: list[dict], *, threshold: float = 0.025) -> list[dict]:
    # group rows by (event_id, market_type) -> book -> {outcome_name: price}
    groups: dict[tuple, dict] = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        price = r.get("decimal_price")
        if not price or price <= 1.0:
            continue
        groups[(r["event_id"], r["market_type"])][r["bookmaker"]][r["outcome_name"]] = price

    signals: list[dict] = []
    for (event_id, market_type), by_book in groups.items():
        names = sorted({n for prices in by_book.values() for n in prices})
        if len(names) != 2:          # two-way markets only (v1)
            continue
        fair = _fair_probs(by_book, names)
        if fair is None:
            continue

        # EV: each non-Pinnacle book/outcome vs the fair prob
        for book, prices in by_book.items():
            if book == PINNACLE:
                continue
            for name in names:
                price = prices.get(name)
                if not price:
                    continue
                e = ev_edge(fair[name], price)
                if is_actionable(e, threshold):
                    signals.append({
                        "event_id": event_id, "market_type": market_type,
                        "bookmaker": book, "outcome_name": name, "signal_type": "ev",
                        "fair_prob": fair[name], "venue_price": price,
                        "edge": e, "kelly_fraction": quarter_kelly(fair[name], price),
                    })

        # Arb: best price per outcome across ALL books; profit if inverse-sum < 1
        best = {}
        for name in names:
            candidates = [(prices[name], book) for book, prices in by_book.items() if prices.get(name)]
            if candidates:
                best[name] = max(candidates)        # (price, book)
        if len(best) == 2:
            inv_sum = sum(1.0 / best[n][0] for n in names)
            if inv_sum < 1.0:
                margin = 1.0 - inv_sum
                for name in names:
                    price, book = best[name]
                    signals.append({
                        "event_id": event_id, "market_type": market_type,
                        "bookmaker": book, "outcome_name": name, "signal_type": "arb",
                        "fair_prob": fair[name], "venue_price": price,
                        "edge": margin, "kelly_fraction": None,
                    })
    return signals
