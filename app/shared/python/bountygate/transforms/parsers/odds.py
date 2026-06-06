"""Pure parser: a The-Odds-API `odds_line` raw payload -> event + odds rows (no I/O)."""
from __future__ import annotations


def parse_odds_line(payload: dict) -> dict:
    """Return {'event': {...}, 'odds': [...]} for one bookmaker's line on one market."""
    event = {
        "source_event_id": payload.get("event_id"),
        "sport_key": payload.get("sport_key"),
        "commence_time": payload.get("commence_time"),
        "home_team": payload.get("home_team"),
        "away_team": payload.get("away_team"),
    }
    market_type = payload.get("market")
    bookmaker = payload.get("bookmaker")
    odds = []
    for o in payload.get("outcomes") or []:
        odds.append({
            "market_type": market_type,
            "bookmaker": bookmaker,
            "outcome_name": o.get("name"),
            "decimal_price": o.get("price"),
        })
    return {"event": event, "odds": odds}
