from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_BASE_SQL = """
SELECT p.event_id, e.sport_key, e.commence_time, e.home_team, e.away_team,
       p.market_key, p.player_name, p.line, p.side, p.bookmaker,
       p.decimal_price, p.captured_at
FROM player_props_odds_history p
JOIN sports_events e ON e.event_id = p.event_id
WHERE p.captured_at >= :cutoff
  AND p.captured_at = (
    SELECT MAX(p2.captured_at) FROM player_props_odds_history p2
    WHERE p2.event_id = p.event_id AND p2.market_key = p.market_key
      AND p2.player_name = p.player_name AND p2.line = p.line
      AND p2.side = p.side AND p2.bookmaker = p.bookmaker)
"""


@router.get("/props")
def list_props(
    sport: str | None = Query(None),
    player: str | None = Query(None),
    market_key: str | None = Query(None),
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    where, params = [], {"cutoff": cutoff.isoformat(), "lim": limit, "off": offset}
    if sport is not None:
        where.append("e.sport_key = :sport")
        params["sport"] = sport
    if player is not None:
        where.append("LOWER(p.player_name) LIKE :player")
        params["player"] = f"%{player.lower()}%"
    if market_key is not None:
        where.append("p.market_key = :market_key")
        params["market_key"] = market_key
    sql = _BASE_SQL
    if where:
        sql += " AND " + " AND ".join(where)
    sql += (" ORDER BY e.commence_time, p.player_name, p.market_key, p.line, p.side"
            " LIMIT :lim OFFSET :off")
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
