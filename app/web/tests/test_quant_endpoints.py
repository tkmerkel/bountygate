from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.web.db import get_engine
from app.web.main import app


def _engine_with(ddl, inserts):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text(ddl))
        for ins in inserts:
            conn.execute(text(ins))
    return engine


def _use(engine):
    app.dependency_overrides[get_engine] = lambda: engine
    return TestClient(app)


_FAIR_DDL = (
    "CREATE TABLE mart_fair_odds (event_id text, sport_key text, commence_time text, "
    "home_team text, away_team text, market_type text, outcome_name text, "
    "consensus_prob real, best_price real, best_bookmaker text, edge real, "
    "computed_at text)"
)


def test_fair_odds_filters_and_orders_by_edge():
    engine = _engine_with(_FAIR_DDL, [
        "INSERT INTO mart_fair_odds (event_id, sport_key, market_type, outcome_name, "
        "consensus_prob, edge) VALUES ('e1','baseball_mlb','h2h','Yankees',0.6,0.04)",
        "INSERT INTO mart_fair_odds (event_id, sport_key, market_type, outcome_name, "
        "consensus_prob, edge) VALUES ('e2','basketball_nba','h2h','Knicks',0.5,0.09)",
    ])
    try:
        client = _use(engine)
        body = client.get("/fair-odds").json()
        assert [r["event_id"] for r in body] == ["e2", "e1"]   # edge desc
        only_mlb = client.get("/fair-odds", params={"sport": "baseball_mlb"}).json()
        assert len(only_mlb) == 1 and only_mlb[0]["event_id"] == "e1"
        assert client.get("/fair-odds", params={"market_type": "totals"}).json() == []
    finally:
        app.dependency_overrides.clear()
