"""Pure parser: a Kalshi `market` raw payload -> normalized rows (no I/O)."""
from __future__ import annotations


def _mid(bid, ask):
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2


def parse_kalshi(payload: dict) -> dict:
    """Return {'market': {...}, 'outcomes': [...], 'prices': [...]} for one Kalshi market."""
    ext = payload.get("ticker")
    market = {
        "venue_key": "kalshi",
        "external_id": ext,
        "title": payload.get("title"),
        "category": payload.get("series_ticker"),
        "status": payload.get("status"),
        "open_time": None,
        "close_time": None,
        "resolved_outcome": None,
        "resolution_time": None,
    }
    yes_mid = _mid(payload.get("yes_bid"), payload.get("yes_ask"))
    no_mid = _mid(payload.get("no_bid"), payload.get("no_ask"))
    volume = payload.get("open_interest")
    liquidity = payload.get("liquidity_dollars")
    outcomes = [
        {"outcome_name": "Yes", "outcome_index": 0, "last_price": yes_mid},
        {"outcome_name": "No", "outcome_index": 1, "last_price": no_mid},
    ]
    prices = [
        {"outcome_name": "Yes", "price": yes_mid, "bid": payload.get("yes_bid"),
         "ask": payload.get("yes_ask"), "volume": volume, "liquidity": liquidity},
        {"outcome_name": "No", "price": no_mid, "bid": payload.get("no_bid"),
         "ask": payload.get("no_ask"), "volume": volume, "liquidity": liquidity},
    ]
    return {"market": market, "outcomes": outcomes, "prices": prices}
