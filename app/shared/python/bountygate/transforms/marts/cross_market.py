"""Pure assembly of mart_cross_market_prices rows from per-game venue probabilities.
Reuses the venue-agnostic analytics primitives for the sportsbook consensus."""
from __future__ import annotations

from bountygate.analytics.consensus import no_vig_consensus
from bountygate.analytics.devig import implied_prob, multiplicative_devig

PINNACLE = "pinnacle"


def sportsbook_side_probs(by_book, home, away):
    """by_book: {bookmaker: {canonical_side: decimal_price}} (sides already canonicalized
    to home/away). Fair P(side) via Pinnacle no-vig, else multi-book consensus.
    Returns {home: prob, away: prob} or None."""
    pin = by_book.get(PINNACLE)
    if pin and pin.get(home) and pin.get(away):
        fh, fa = multiplicative_devig(implied_prob(pin[home]), implied_prob(pin[away]))
        return {home: fh, away: fa}
    over, under = [], []
    for book, prices in by_book.items():
        if book == PINNACLE:
            continue
        over.append(prices.get(home))
        under.append(prices.get(away))
    c = no_vig_consensus(over, under)
    if c is None:
        return None
    return {home: c[0], away: c[1]}


def assemble_rows(games):
    """games: dicts with sport, date(datetime), home, away (canonical ids), and optional
    sportsbook/kalshi/polymarket dicts {canonical_side: prob}. Emits up to two rows per
    game (one per winning side); a side is kept only if >=2 venues have a prob."""
    rows = []
    for g in games:
        home, away = g["home"], g["away"]
        sb = g.get("sportsbook") or {}
        ka = g.get("kalshi") or {}
        pm = g.get("polymarket") or {}
        date = g["date"].strftime("%Y-%m-%d")
        sport = g["sport"].lower()
        for side in (home, away):
            k, p, s = ka.get(side), pm.get(side), sb.get(side)
            present = [v for v in (k, p, s) if v is not None]
            if len(present) < 2:
                continue
            rows.append({
                "question_key": f"{sport}:{date}:{away}@{home}:{side}",
                "kalshi_prob": k,
                "polymarket_prob": p,
                "sportsbook_consensus_prob": s,
                "max_spread": max(present) - min(present),
            })
    return rows
