import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.web.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def seed_runs():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    e = create_engine(url)
    with e.begin() as c:
        c.execute(text("DELETE FROM dashboard_runs WHERE run_id LIKE 'test-r-%'"))
        c.execute(
            text(
                "INSERT INTO dashboard_runs (run_id, occurred_at, player, market, outcome, "
                "duration_s, issues, top_finding, video_url, review_url) VALUES "
                "('test-r-1', :t1, 'Dylan Harper', 'player_rebounds', 'failure', 54.8, "
                "'{\"wasted_wait\":[\"BetMGM froze\"]}'::jsonb, 'froze 67s', 'v.mp4', 'r.md')"
            ),
            {"t1": datetime.now(timezone.utc)},
        )
    yield
    with e.begin() as c:
        c.execute(text("DELETE FROM dashboard_runs WHERE run_id LIKE 'test-r-%'"))


def test_api_runs_returns_latest_first():
    resp = client.get("/api/runs?limit=10")
    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == 1
    ids = [r["run_id"] for r in body["runs"]]
    assert "test-r-1" in ids
    r = next(r for r in body["runs"] if r["run_id"] == "test-r-1")
    assert r["player"] == "Dylan Harper"
    assert r["outcome"] == "failure"
    assert r["issues"]["wasted_wait"] == ["BetMGM froze"]
