from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.web.db import get_engine
from app.web.main import app


def _seed():
    # StaticPool + check_same_thread=False: FastAPI's TestClient runs the sync
    # route handler in a worker thread. The default sqlite:// engine uses a
    # SingletonThreadPool (one connection per thread) and each in-memory
    # connection is its own database, so the handler thread would never see the
    # table seeded here. StaticPool shares one connection across threads.
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE markets (market_id text, venue_key text, external_id text, "
            "title text, category text, status text, open_time text, close_time text, "
            "resolved_outcome text, resolution_time text, updated_at text)"
        ))
        conn.execute(text(
            "INSERT INTO markets (market_id, venue_key, external_id, title, status) "
            "VALUES ('m1','kalshi','KX-1','Will X happen?','active')"
        ))
    return engine


def test_health_ok():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_markets_returns_rows_and_filters_by_venue():
    engine = _seed()
    app.dependency_overrides[get_engine] = lambda: engine
    try:
        client = TestClient(app)
        r = client.get("/markets")
        assert r.status_code == 200
        body = r.json()
        assert len(body) == 1 and body[0]["external_id"] == "KX-1"

        assert client.get("/markets", params={"venue": "polymarket"}).json() == []
        assert len(client.get("/markets", params={"venue": "kalshi"}).json()) == 1
    finally:
        app.dependency_overrides.clear()
