from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = "market_id, resolved_outcome, resolution_time, predicted_prob, realized, clv"


@router.get("/history")
def list_history(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    sql = f"SELECT {_COLS} FROM mart_market_history LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"lim": limit, "off": offset}).mappings().all()
    return [dict(r) for r in rows]
