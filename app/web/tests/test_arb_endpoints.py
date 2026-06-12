"""Tests for GET /arbs endpoint."""
import json
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.web.db import get_engine
from app.web.main import app

_DDL = (
    "CREATE TABLE arb_opportunities ("
    "opportunity_hash text PRIMARY KEY, "
    "first_detected_at text, last_seen_at text, kind text, pairing text, "
    "event_id text, sport_key text, home_team text, away_team text, "
    "commence_time text, market_segment text, player_name text, line real, "
    "pairing_type text, leg_a_kind text, leg_a_source text, leg_a_outcome text, "
    "leg_a_point real, leg_a_price real, leg_a_stake real, "
    "leg_b_kind text, leg_b_source text, leg_b_outcome text, "
    "leg_b_point real, leg_b_price real, leg_b_stake real, "
    "payout real, arb_ev real, roi real, fee_adjusted_roi real, "
    "hours_until_commence real, details text)"
)


def _engine_with(ddl, inserts):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    with engine.begin() as conn:
        conn.execute(text(ddl))
        for ins in inserts:
            conn.execute(text(ins))
    return engine


def _use(engine):
    app.dependency_overrides[get_engine] = lambda: engine
    return TestClient(app)


def _make_inserts():
    now = datetime.now(timezone.utc)
    fresh = now.isoformat()
    stale = (now - timedelta(hours=2)).isoformat()
    details_json = json.dumps({"foo": "bar"})
    return [
        # Row 1: game, book_book, fresh, high roi=0.05
        f"INSERT INTO arb_opportunities VALUES ("
        f"'hash1','{fresh}','{fresh}','game','book_book','eid1','baseball_mlb',"
        f"'TeamA','TeamB','{fresh}','h2h',NULL,NULL,NULL,"
        f"'book','fanduel','home',NULL,2.1,100.0,"
        f"'book','draftkings','away',NULL,2.2,90.9,"
        f"200.0,5.0,0.05,0.05,2.0,'{details_json}')",

        # Row 2: prop, book_venue, fresh, low roi=0.01
        f"INSERT INTO arb_opportunities VALUES ("
        f"'hash2','{fresh}','{fresh}','prop','book_venue','eid2','basketball_nba',"
        f"'TeamC','TeamD','{fresh}','player_points','LeBron James',25.5,NULL,"
        f"'book','fanduel','over',25.5,2.05,100.0,"
        f"'venue','kalshi','yes',NULL,1.05,95.2,"
        f"200.0,1.0,0.01,0.01,5.0,NULL)",

        # Row 3: game, book_book, STALE last_seen_at = 2h ago
        f"INSERT INTO arb_opportunities VALUES ("
        f"'hash3','{stale}','{stale}','game','book_book','eid3','baseball_mlb',"
        f"'TeamE','TeamF','{stale}','h2h',NULL,NULL,NULL,"
        f"'book','betmgm','home',NULL,2.15,100.0,"
        f"'book','caesars','away',NULL,2.18,91.7,"
        f"200.0,3.3,0.03,0.03,1.0,NULL)",
    ]


def test_default_excludes_stale():
    engine = _engine_with(_DDL, _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/arbs").json()
        # Stale row excluded; 2 fresh rows returned
        assert len(body) == 2
        hashes = [r["opportunity_hash"] for r in body]
        assert "hash3" not in hashes
    finally:
        app.dependency_overrides.clear()


def test_default_order_by_fee_adjusted_roi_desc():
    engine = _engine_with(_DDL, _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/arbs").json()
        assert len(body) == 2
        # hash1 has roi 0.05, hash2 has roi 0.01; hash1 first
        assert body[0]["opportunity_hash"] == "hash1"
        assert body[1]["opportunity_hash"] == "hash2"
    finally:
        app.dependency_overrides.clear()


def test_kind_filter():
    engine = _engine_with(_DDL, _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/arbs", params={"kind": "prop"}).json()
        assert len(body) == 1
        assert body[0]["opportunity_hash"] == "hash2"
        assert client.get("/arbs", params={"kind": "game"}).json()[0]["opportunity_hash"] == "hash1"
    finally:
        app.dependency_overrides.clear()


def test_pairing_filter():
    engine = _engine_with(_DDL, _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/arbs", params={"pairing": "book_venue"}).json()
        assert len(body) == 1
        assert body[0]["opportunity_hash"] == "hash2"
        assert client.get("/arbs", params={"pairing": "book_book"}).json()[0]["opportunity_hash"] == "hash1"
    finally:
        app.dependency_overrides.clear()


def test_min_roi_filters_low():
    engine = _engine_with(_DDL, _make_inserts())
    try:
        client = _use(engine)
        # min_roi=0.03 should exclude hash2 (0.01) but keep hash1 (0.05)
        body = client.get("/arbs", params={"min_roi": 0.03}).json()
        assert len(body) == 1
        assert body[0]["opportunity_hash"] == "hash1"
    finally:
        app.dependency_overrides.clear()


def test_include_stale_returns_all_three():
    engine = _engine_with(_DDL, _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/arbs", params={"include_stale": "true"}).json()
        assert len(body) == 3
        hashes = {r["opportunity_hash"] for r in body}
        assert hashes == {"hash1", "hash2", "hash3"}
    finally:
        app.dependency_overrides.clear()


def test_details_json_round_trips_to_dict():
    engine = _engine_with(_DDL, _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/arbs").json()
        # hash1 has details={"foo":"bar"}; hash2 has None
        h1 = next(r for r in body if r["opportunity_hash"] == "hash1")
        h2 = next(r for r in body if r["opportunity_hash"] == "hash2")
        assert isinstance(h1["details"], dict)
        assert h1["details"] == {"foo": "bar"}
        assert h2["details"] is None
    finally:
        app.dependency_overrides.clear()


def test_limit_and_offset():
    engine = _engine_with(_DDL, _make_inserts())
    try:
        client = _use(engine)
        # With include_stale, 3 rows total; limit=1 returns 1
        body = client.get("/arbs", params={"include_stale": "true", "limit": 1}).json()
        assert len(body) == 1
        # offset=1 skips first row
        body2 = client.get("/arbs", params={"include_stale": "true", "limit": 2, "offset": 1}).json()
        assert len(body2) == 2
        # offset=3 returns nothing
        body3 = client.get("/arbs", params={"include_stale": "true", "offset": 3}).json()
        assert body3 == []
    finally:
        app.dependency_overrides.clear()
