from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_MAX_POINTS_PER_SERIES = 500


def _valid_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


@router.get("/movement/{event_id}")
def movement(
    event_id: str,
    market_type: str | None = Query(None),
    engine: Engine = Depends(get_engine),
):
    if not _valid_uuid(event_id):
        return []
    where, params = ["event_id = :eid"], {"eid": event_id}
    if market_type:
        where.append("market_type = :mt")
        params["mt"] = market_type
    sql = ("SELECT market_type, bookmaker, outcome_name, decimal_price, captured_at "
           "FROM sportsbook_odds_history WHERE " + " AND ".join(where) +
           " ORDER BY captured_at ASC")
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(text(sql), params).mappings()]
    # downsample each (bookmaker, outcome) series to <= _MAX_POINTS_PER_SERIES
    series: dict = {}
    for r in rows:
        series.setdefault((r["market_type"], r["bookmaker"], r["outcome_name"]), []).append(r)
    out = []
    for pts in series.values():
        stride = max(1, len(pts) // _MAX_POINTS_PER_SERIES)
        kept = pts[::stride]
        if kept[-1] is not pts[-1]:
            kept.append(pts[-1])      # always keep the latest point
        out.extend(kept)
    out.sort(key=lambda r: str(r["captured_at"]))
    return out


_CLOSE_COLS = ("event_id, market_type, bookmaker, outcome_name, decimal_price, "
               "fair_prob, captured_at, staleness_minutes")


@router.get("/closing-lines")
def closing_lines(
    event_id: str = Query(...),
    engine: Engine = Depends(get_engine),
):
    if not _valid_uuid(event_id):
        return []
    sql = (f"SELECT {_CLOSE_COLS} FROM closing_lines WHERE event_id = :eid "
           "ORDER BY market_type, bookmaker, outcome_name")
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"eid": event_id}).mappings().all()
    return [dict(r) for r in rows]
