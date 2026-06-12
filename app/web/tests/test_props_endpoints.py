"""Tests for GET /props endpoint."""
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.web.db import get_engine
from app.web.main import app

_EVENTS_DDL = (
    "CREATE TABLE sports_events ("
    "event_id text PRIMARY KEY, source_event_id text, sport_key text, "
    "commence_time text, home_team text, away_team text)"
)

_PROPS_DDL = (
    "CREATE TABLE player_props_odds_history ("
    "event_id text NOT NULL, market_key text NOT NULL, player_name text NOT NULL, "
    "line real NOT NULL, side text NOT NULL, bookmaker text NOT NULL, "
    "decimal_price real, captured_at text NOT NULL)"
)


def _engine_with(ddl_list, inserts):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    with engine.begin() as conn:
        for ddl in ddl_list:
            conn.execute(text(ddl))
        for ins in inserts:
            conn.execute(text(ins))
    return engine


def _use(engine):
    app.dependency_overrides[get_engine] = lambda: engine
    return TestClient(app)


def _make_inserts():
    now = datetime.now(timezone.utc)
    recent = now.isoformat()
    older = (now - timedelta(hours=12)).isoformat()
    commence = (now + timedelta(hours=3)).isoformat()

    return [
        # sports_events rows
        f"INSERT INTO sports_events VALUES ('eid1','src1','baseball_mlb','{commence}','RedSox','Yankees')",
        f"INSERT INTO sports_events VALUES ('eid2','src2','basketball_nba','{commence}','Lakers','Celtics')",

        # Same natural key (eid1, player_points, Mike Trout, 1.5, over, fanduel): older row
        f"INSERT INTO player_props_odds_history VALUES "
        f"('eid1','player_points','Mike Trout',1.5,'over','fanduel',1.85,'{older}')",

        # Same natural key: newer row (higher price)
        f"INSERT INTO player_props_odds_history VALUES "
        f"('eid1','player_points','Mike Trout',1.5,'over','fanduel',1.95,'{recent}')",

        # Different player, same event
        f"INSERT INTO player_props_odds_history VALUES "
        f"('eid1','player_points','Aaron Judge',0.5,'over','draftkings',2.10,'{recent}')",

        # NBA event row
        f"INSERT INTO player_props_odds_history VALUES "
        f"('eid2','player_points','LeBron James',25.5,'over','fanduel',1.80,'{recent}')",
    ]


def test_latest_only_not_older():
    """Correlated subquery should return only the max captured_at row per natural key."""
    engine = _engine_with([_EVENTS_DDL, _PROPS_DDL], _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/props").json()
        # Mike Trout appears once (latest price 1.95, not 1.85)
        trout_rows = [r for r in body if r["player_name"] == "Mike Trout"]
        assert len(trout_rows) == 1
        assert trout_rows[0]["decimal_price"] == 1.95
    finally:
        app.dependency_overrides.clear()


def test_player_substring_case_insensitive():
    engine = _engine_with([_EVENTS_DDL, _PROPS_DDL], _make_inserts())
    try:
        client = _use(engine)
        # "trout" (lowercase) should match "Mike Trout"
        body = client.get("/props", params={"player": "trout"}).json()
        assert len(body) == 1
        assert body[0]["player_name"] == "Mike Trout"
        # "LEBRON" (uppercase) should match "LeBron James"
        body2 = client.get("/props", params={"player": "LEBRON"}).json()
        assert len(body2) == 1
        assert body2[0]["player_name"] == "LeBron James"
        # partial match in middle of name
        body3 = client.get("/props", params={"player": "ron jam"}).json()
        assert len(body3) == 1
    finally:
        app.dependency_overrides.clear()


def test_sport_filter():
    engine = _engine_with([_EVENTS_DDL, _PROPS_DDL], _make_inserts())
    try:
        client = _use(engine)
        mlb = client.get("/props", params={"sport": "baseball_mlb"}).json()
        # Mike Trout + Aaron Judge from eid1 (baseball_mlb)
        assert len(mlb) == 2
        assert all(r["sport_key"] == "baseball_mlb" for r in mlb)
        nba = client.get("/props", params={"sport": "basketball_nba"}).json()
        assert len(nba) == 1
        assert nba[0]["player_name"] == "LeBron James"
    finally:
        app.dependency_overrides.clear()


def test_market_key_filter():
    engine = _engine_with([_EVENTS_DDL, _PROPS_DDL], _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/props", params={"market_key": "player_points"}).json()
        assert len(body) == 3  # Mike Trout, Aaron Judge, LeBron
        assert client.get("/props", params={"market_key": "player_strikeouts"}).json() == []
    finally:
        app.dependency_overrides.clear()


def test_empty_search_returns_empty_not_error():
    engine = _engine_with([_EVENTS_DDL, _PROPS_DDL], _make_inserts())
    try:
        client = _use(engine)
        # player that doesn't exist
        body = client.get("/props", params={"player": "zzz_no_such_player"}).json()
        assert body == []
    finally:
        app.dependency_overrides.clear()


def test_limit_and_offset():
    engine = _engine_with([_EVENTS_DDL, _PROPS_DDL], _make_inserts())
    try:
        client = _use(engine)
        body_all = client.get("/props").json()
        total = len(body_all)
        assert total > 0
        body_lim = client.get("/props", params={"limit": 1}).json()
        assert len(body_lim) == 1
        body_off = client.get("/props", params={"offset": total}).json()
        assert body_off == []
    finally:
        app.dependency_overrides.clear()


def test_all_join_columns_present():
    """Response should include joined sports_events columns."""
    engine = _engine_with([_EVENTS_DDL, _PROPS_DDL], _make_inserts())
    try:
        client = _use(engine)
        body = client.get("/props", params={"player": "trout"}).json()
        assert len(body) == 1
        row = body[0]
        assert row["sport_key"] == "baseball_mlb"
        assert row["home_team"] == "RedSox"
        assert row["away_team"] == "Yankees"
        assert row["event_id"] == "eid1"
    finally:
        app.dependency_overrides.clear()
