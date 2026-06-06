"""Pure parser: a Polymarket `market` raw payload -> normalized rows (no I/O)."""
from __future__ import annotations


def _status(payload: dict) -> str | None:
    if payload.get("closed"):
        return "closed"
    if payload.get("active"):
        return "active"
    return None


def parse_polymarket(payload: dict) -> dict:
    """Return {'market': {...}, 'outcomes': [...], 'prices': [...]} for one Polymarket market."""
    market = {
        "venue_key": "polymarket",
        "external_id": payload.get("condition_id"),
        "title": payload.get("question"),
        "category": None,
        "status": _status(payload),
        "open_time": None,
        "close_time": payload.get("end_date"),
        "resolved_outcome": None,
        "resolution_time": None,
    }
    volume = payload.get("volume")
    liquidity = payload.get("liquidity")
    names = payload.get("outcomes") or []
    prices_in = payload.get("outcome_prices") or []
    outcomes, prices = [], []
    for idx, name in enumerate(names):
        price = prices_in[idx] if idx < len(prices_in) else None
        outcomes.append({"outcome_name": name, "outcome_index": idx, "last_price": price})
        prices.append({"outcome_name": name, "price": price, "bid": None, "ask": None,
                       "volume": volume, "liquidity": liquidity})
    return {"market": market, "outcomes": outcomes, "prices": prices}
