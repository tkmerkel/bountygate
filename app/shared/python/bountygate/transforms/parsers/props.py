"""Pure parser: a The-Odds-API `player_prop` raw payload -> event + prop rows (no I/O)."""
from __future__ import annotations


def parse_player_prop(payload: dict) -> dict:
    """Return {'event': {...}, 'props': [...]} for one bookmaker's prop market.

    Keeps only outcomes with name Over/Under (case-insensitive), a numeric point,
    and a non-empty description (player name) — yes-only tiles and lineless
    outcomes are dropped (archive convention).
    """
    event = {
        "source_event_id": payload.get("event_id"),
        "sport_key": payload.get("sport_key"),
        "commence_time": payload.get("commence_time"),
        "home_team": payload.get("home_team"),
        "away_team": payload.get("away_team"),
    }
    market_key = payload.get("market")
    bookmaker = payload.get("bookmaker")
    props = []
    for o in payload.get("outcomes") or []:
        name = o.get("name") or ""
        description = o.get("description") or ""
        point = o.get("point")
        # Drop yes-only tiles, lineless outcomes, outcomes missing player name,
        # and outcomes whose name is not Over/Under.
        if name.lower() not in ("over", "under"):
            continue
        if point is None:
            continue
        if not description:
            continue
        props.append({
            "market_key": market_key,
            "player_name": description,
            "line": point,
            "side": name.lower(),
            "bookmaker": bookmaker,
            "decimal_price": o.get("price"),
        })
    return {"event": event, "props": props}
