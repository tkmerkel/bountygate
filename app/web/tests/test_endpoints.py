from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.web.db import get_engine
from app.web.main import app


def _engine_with(ddl, insert):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text(insert))
    return engine


def _use(engine):
    app.dependency_overrides[get_engine] = lambda: engine
    return TestClient(app)


def test_edges_filters_by_signal_type():
    engine = _engine_with(
        "CREATE TABLE mart_edge_signals (signal_id text, detected_at text, venue_key text, "
        "market_id text, outcome_id text, signal_type text, fair_prob real, venue_price real, "
        "edge real, kelly_fraction real)",
        "INSERT INTO mart_edge_signals (signal_id, detected_at, venue_key, signal_type, edge) "
        "VALUES ('s1','2026-06-06','kalshi','ev',0.05)",
    )
    try:
        client = _use(engine)
        assert len(client.get("/edges").json()) == 1
        assert client.get("/edges", params={"signal_type": "arb"}).json() == []
        assert len(client.get("/edges", params={"signal_type": "ev"}).json()) == 1
    finally:
        app.dependency_overrides.clear()


def test_cross_market_returns_rows():
    engine = _engine_with(
        "CREATE TABLE mart_cross_market_prices (question_key text, captured_at text, "
        "kalshi_prob real, polymarket_prob real, sportsbook_consensus_prob real, max_spread real)",
        "INSERT INTO mart_cross_market_prices (question_key, kalshi_prob) VALUES ('q1',0.4)",
    )
    try:
        client = _use(engine)
        body = client.get("/cross-market").json()
        assert len(body) == 1 and body[0]["question_key"] == "q1"
    finally:
        app.dependency_overrides.clear()


def test_history_returns_rows():
    engine = _engine_with(
        "CREATE TABLE mart_market_history (market_id text, resolved_outcome text, "
        "resolution_time text, predicted_prob real, realized integer, clv real)",
        "INSERT INTO mart_market_history (market_id, resolved_outcome) VALUES ('m1','yes')",
    )
    try:
        client = _use(engine)
        body = client.get("/history").json()
        assert len(body) == 1 and body[0]["market_id"] == "m1"
    finally:
        app.dependency_overrides.clear()


def test_market_price_history():
    mid = "11111111-1111-1111-1111-111111111111"
    engine = _engine_with(
        "CREATE TABLE price_history (market_id text, outcome_id text, captured_at text, "
        "price real, bid real, ask real, volume real, liquidity real)",
        "INSERT INTO price_history (market_id, outcome_id, captured_at, price) "
        f"VALUES ('{mid}','o1','2026-06-06',0.5)",
    )
    try:
        client = _use(engine)
        body = client.get(f"/markets/{mid}/history").json()
        assert len(body) == 1 and body[0]["price"] == 0.5
        # valid-but-absent uuid -> []
        assert client.get("/markets/22222222-2222-2222-2222-222222222222/history").json() == []
        # non-uuid id -> [] (guarded; market_id is a uuid column in Postgres,
        # so an unguarded non-uuid would raise a 500 DataError)
        assert client.get("/markets/foo/history").json() == []
    finally:
        app.dependency_overrides.clear()
