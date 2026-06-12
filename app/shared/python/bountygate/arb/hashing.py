"""Pairing-type derivation + generic opportunity hashing for the arb pipeline.

`derive_pairing_type` is ported VERBATIM from
airflow/dags/_archive_pre_pivot/bg_arb_pipeline_lib/hashing.py.

`opportunity_hash` is the generic (book-x-venue and venue-x-venue) successor to
the archive's player-prop-only hash. It keeps the archive's precision
convention so identities stay stable across detection runs:

  - line:  .3f
  - point: .3f
  - price: .6f

This precision is load-bearing: the hash is the primary key of the
arb_opportunities table, so two detection runs that observe the *same* prices
(to 6 decimals) and lines (to 3 decimals) must collapse to one row rather than
churning fresh PKs. Tightening or loosening the format would invalidate dedup
against existing history.
"""
from __future__ import annotations

import hashlib
from typing import Iterable, Optional, Tuple


def derive_pairing_type(under_market_key: str, over_market_key: str) -> str:
    """Classify a pair by which leg(s) are alternate-line variants.

    Only the literal `_alternate` suffix counts — substring matches don't.
    """
    under_is_alt = under_market_key.endswith("_alternate")
    over_is_alt = over_market_key.endswith("_alternate")
    if not under_is_alt and not over_is_alt:
        return "std_std"
    if not under_is_alt and over_is_alt:
        return "std_alt"
    if under_is_alt and not over_is_alt:
        return "alt_std"
    return "alt_alt"


def _fmt_point(point: Optional[float]) -> str:
    return "none" if point is None else f"{float(point):.3f}"


def opportunity_hash(
    *,
    kind: str,
    pairing: str,
    event_id,
    market_segment: str,
    legs: Iterable[Tuple[str, str, Optional[float], float]],
    player_name: Optional[str] = None,
    line: Optional[float] = None,
) -> str:
    """Stable SHA-256 hash identifying an arbitrage opportunity.

    `legs` is an iterable of two tuples ``(source, outcome, point, price)``.
    Legs are sorted by ``(source, outcome)`` so symmetric pairs (the same two
    legs in either order) collapse to one identity. Floats are formatted with
    the pipeline's fixed precision (line/point ``.3f``, price ``.6f``); a None
    point or line serializes to the literal ``"none"`` (distinct from ``0.0``,
    which renders as ``"0.000"``). All parts are joined with ``"|"``.
    """
    sorted_legs = sorted(legs, key=lambda leg: (leg[0], leg[1]))

    parts = [
        kind,
        pairing,
        str(event_id),
        market_segment,
        str(player_name),
        "none" if line is None else f"{float(line):.3f}",
    ]
    for source, outcome, point, price in sorted_legs:
        parts.append(source)
        parts.append(outcome)
        parts.append(_fmt_point(point))
        parts.append(f"{float(price):.6f}")

    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
