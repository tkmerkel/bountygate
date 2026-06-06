from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = ("market_id, venue_key, external_id, title, category, status, "
         "open_time, close_time, resolved_outcome, resolution_time, updated_at")


@router.get("/markets")
def list_markets(
    venue: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    where, params = [], {"lim": limit, "off": offset}
    if venue:
        where.append("venue_key = :venue")
        params["venue"] = venue
    if status:
        where.append("status = :status")
        params["status"] = status
    sql = f"SELECT {_COLS} FROM markets"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
