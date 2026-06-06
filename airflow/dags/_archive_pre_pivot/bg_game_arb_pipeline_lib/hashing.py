"""Hash for game-line arb opportunities.

Leg ordering is canonicalized (alphabetical by book) before hashing so that
(leg_a=fanduel, leg_b=betmgm) and (leg_a=betmgm, leg_b=fanduel) produce the
same hash — dedup safety against the symmetric builder.

Precision matches bg_arb_pipeline_lib.hashing:
  - point: .3f  (NULL → empty string)
  - price: .6f
"""
from __future__ import annotations

import hashlib
import math
from typing import Any, Mapping


def _format_point(value: Any) -> str:
    if value is None:
        return ""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return ""
    if math.isnan(f):
        return ""
    return f"{f:.3f}"


def opportunity_hash(row: Mapping[str, Any]) -> str:
    """Stable SHA-256 hash identifying a game-line opportunity at a point in time.

    Canonicalizes leg ordering by book name (alphabetical) so swapped legs
    yield the same hash.
    """
    book_a = str(row["leg_a_book"])
    book_b = str(row["leg_b_book"])
    if book_a > book_b:
        a_book, a_outcome, a_point, a_price = book_b, str(row["leg_b_outcome"]), row["leg_b_point"], row["leg_b_price"]
        b_book, b_outcome, b_point, b_price = book_a, str(row["leg_a_outcome"]), row["leg_a_point"], row["leg_a_price"]
    else:
        a_book, a_outcome, a_point, a_price = book_a, str(row["leg_a_outcome"]), row["leg_a_point"], row["leg_a_price"]
        b_book, b_outcome, b_point, b_price = book_b, str(row["leg_b_outcome"]), row["leg_b_point"], row["leg_b_price"]

    parts = (
        str(row["event_id"]),
        str(row["market_key"]),
        a_book, a_outcome, _format_point(a_point), f"{float(a_price):.6f}",
        b_book, b_outcome, _format_point(b_point), f"{float(b_price):.6f}",
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
