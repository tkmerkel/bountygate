from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.web.db import get_engine

router = APIRouter()

_COLS = (
    "opportunity_hash, first_detected_at, last_seen_at, kind, pairing, event_id, "
    "sport_key, home_team, away_team, commence_time, market_segment, player_name, "
    "line, pairing_type, leg_a_kind, leg_a_source, leg_a_outcome, leg_a_point, "
    "leg_a_price, leg_a_stake, leg_b_kind, leg_b_source, leg_b_outcome, leg_b_point, "
    "leg_b_price, leg_b_stake, payout, arb_ev, roi, fee_adjusted_roi, "
    "hours_until_commence, details"
)


@router.get("/arbs")
def list_arbs(
    kind: str | None = Query(None),
    pairing: str | None = Query(None),
    min_roi: float = Query(0.0),
    include_stale: bool = Query(False),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(get_engine),
):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)
    where, params = [], {"lim": limit, "off": offset, "cutoff": cutoff.isoformat()}
    if not include_stale:
        where.append("last_seen_at >= :cutoff")
    if kind is not None:
        where.append("kind = :kind")
        params["kind"] = kind
    if pairing is not None:
        where.append("pairing = :pairing")
        params["pairing"] = pairing
    if min_roi != 0.0:
        where.append("fee_adjusted_roi >= :min_roi")
        params["min_roi"] = min_roi
    sql = f"SELECT {_COLS} FROM arb_opportunities"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY fee_adjusted_roi DESC, last_seen_at DESC LIMIT :lim OFFSET :off"
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(text(sql), params).mappings()]
    # Deserialize `details` JSON TEXT (sqlite stores as TEXT; Postgres returns dict for jsonb)
    for row in rows:
        if row.get("details") is not None and isinstance(row["details"], str):
            row["details"] = json.loads(row["details"])
    return rows
