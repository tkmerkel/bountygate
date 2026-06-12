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


def test_sharpness_rows():
    engine = _engine_with(
        "CREATE TABLE venue_sharpness (venue_key text, sport_key text, score_window text, "
        "n_games integer, brier real, logloss real, avg_clv real, computed_at text)",
        ["INSERT INTO venue_sharpness (venue_key, sport_key, score_window, n_games, brier) "
         "VALUES ('pinnacle','baseball_mlb','all',250,0.21)"],
    )
    try:
        client = _use(engine)
        body = client.get("/sharpness").json()
        assert len(body) == 1 and body[0]["venue_key"] == "pinnacle"
    finally:
        app.dependency_overrides.clear()


def test_calibration_filters_by_source():
    engine = _engine_with(
        "CREATE TABLE mart_calibration (source text, sport_key text, prob_bucket real, "
        "n integer, predicted_mean real, realized_rate real, computed_at text)",
        ["INSERT INTO mart_calibration (source, sport_key, prob_bucket, n) "
         "VALUES ('consensus_v1','baseball_mlb',0.7,42)",
         "INSERT INTO mart_calibration (source, sport_key, prob_bucket, n) "
         "VALUES ('fanduel','baseball_mlb',0.7,42)"],
    )
    try:
        client = _use(engine)
        assert len(client.get("/calibration").json()) == 2
        only = client.get("/calibration", params={"source": "consensus_v1"}).json()
        assert len(only) == 1 and only[0]["source"] == "consensus_v1"
    finally:
        app.dependency_overrides.clear()
