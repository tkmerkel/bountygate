"""Hash + pairing-type derivation for the arb pipeline.

Precision matches the existing bg_arbitrage_player_props._build_opportunity_key:
  - line:  .3f
  - price: .6f
Changing this would invalidate dedup against bg_executed_opportunities history.
"""
from __future__ import annotations

import hashlib
from typing import Any, Mapping


def opportunity_hash(row: Mapping[str, Any]) -> str:
    """Stable SHA-256 hash identifying an opportunity at a point in time."""
    parts = (
        str(row["event_id"]),
        str(row["player_name"]),
        str(row["under_book"]),
        str(row["under_market_key"]),
        str(row["over_book"]),
        str(row["over_market_key"]),
        f"{float(row['under_line']):.3f}",
        f"{float(row['under_price']):.6f}",
        f"{float(row['over_price']):.6f}",
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


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
