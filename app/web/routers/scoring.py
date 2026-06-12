from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_SHARP_COLS = "venue_key, sport_key, score_window, n_games, brier, logloss, avg_clv, computed_at"
_CAL_COLS = "source, sport_key, prob_bucket, n, predicted_mean, realized_rate, computed_at"


@router.get("/sharpness")
def list_sharpness(
    sport: str | None = Query(None),
    engine: Engine = Depends(get_engine),
):
    sql = f"SELECT {_SHARP_COLS} FROM venue_sharpness"
    params = {}
    if sport:
        sql += " WHERE sport_key = :sport"
        params["sport"] = sport
    sql += " ORDER BY brier ASC NULLS LAST"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


@router.get("/calibration")
def list_calibration(
    source: str | None = Query(None),
    sport: str | None = Query(None),
    engine: Engine = Depends(get_engine),
):
    where, params = [], {}
    if source:
        where.append("source = :source")
        params["source"] = source
    if sport:
        where.append("sport_key = :sport")
        params["sport"] = sport
    sql = f"SELECT {_CAL_COLS} FROM mart_calibration"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY source, sport_key, prob_bucket"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]
