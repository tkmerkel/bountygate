"""Closing-line derivation: the last pre-commence snapshot per (book, outcome).

Pure: rows in, closing dicts out. The builder owns the SQL and the insert.
"""
from __future__ import annotations

from bountygate.analytics.devig import implied_prob, multiplicative_devig


def derive_closing(rows: list, commence_time) -> list:
    """rows: dicts with bookmaker, outcome_name, decimal_price, captured_at
    (tz-aware datetime), all for ONE (event, market_type).

    Returns one dict per (bookmaker, outcome): bookmaker, outcome_name,
    decimal_price, fair_prob (multiplicative devig when the book quotes both
    sides of a two-way pair, else None), captured_at, staleness_minutes.
    """
    pre = [
        r for r in rows
        if r.get("decimal_price") and float(r["decimal_price"]) > 1.0
        and r["captured_at"] <= commence_time
    ]
    latest: dict = {}
    for r in sorted(pre, key=lambda r: r["captured_at"]):
        latest[(r["bookmaker"], r["outcome_name"])] = r

    by_book: dict = {}
    for (book, name), r in latest.items():
        by_book.setdefault(book, {})[name] = r

    out = []
    for book, by_name in sorted(by_book.items()):
        fair = None
        if len(by_name) == 2:
            (n0, r0), (n1, r1) = sorted(by_name.items())
            f0, f1 = multiplicative_devig(
                implied_prob(float(r0["decimal_price"])),
                implied_prob(float(r1["decimal_price"])),
            )
            fair = {n0: f0, n1: f1}
        for name, r in sorted(by_name.items()):
            out.append({
                "bookmaker": book,
                "outcome_name": name,
                "decimal_price": float(r["decimal_price"]),
                "fair_prob": fair[name] if fair else None,
                "captured_at": r["captured_at"],
                "staleness_minutes":
                    (commence_time - r["captured_at"]).total_seconds() / 60.0,
            })
    return out
