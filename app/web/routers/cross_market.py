from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = ("question_key, captured_at, kalshi_prob, polymarket_prob, "
         "sportsbook_consensus_prob, max_spread")


@router.get("/cross-market")
def list_cross_market(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    sql = f"SELECT {_COLS} FROM mart_cross_market_prices LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"lim": limit, "off": offset}).mappings().all()
    return [dict(r) for r in rows]
