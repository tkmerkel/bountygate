from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = ("event_id, sport_key, commence_time, home_team, away_team, market_type, "
         "outcome_name, consensus_prob, best_price, best_bookmaker, edge, computed_at")


@router.get("/fair-odds")
def list_fair_odds(
    sport: str | None = Query(None),
    market_type: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    where, params = [], {"lim": limit, "off": offset}
    if sport:
        where.append("sport_key = :sport")
        params["sport"] = sport
    if market_type:
        where.append("market_type = :mt")
        params["mt"] = market_type
    sql = f"SELECT {_COLS} FROM mart_fair_odds"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY edge DESC NULLS LAST LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
