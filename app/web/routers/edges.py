from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = ("signal_id, detected_at, venue_key, market_id, outcome_id, signal_type, "
         "fair_prob, venue_price, edge, kelly_fraction")


@router.get("/edges")
def list_edges(
    signal_type: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    sql = f"SELECT {_COLS} FROM mart_edge_signals"
    params = {"lim": limit, "off": offset}
    if signal_type:
        sql += " WHERE signal_type = :st"
        params["st"] = signal_type
    sql += " ORDER BY detected_at DESC LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
