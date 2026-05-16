import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.web.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def seed_heartbeats():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    e = create_engine(url)
    with e.begin() as c:
        c.execute(text("DELETE FROM watcher_heartbeats WHERE name LIKE 'test-api-%'"))
        c.execute(
            text(
                "INSERT INTO watcher_heartbeats (name, is_running, last_tick_at, pending_count, "
                "oldest_pending_age_s, completed_24h, errors_24h, last_error, expected_interval_s) "
                "VALUES ('test-api-ok', true, :now, 0, NULL, 5, 0, NULL, 60)"
            ),
            {"now": datetime.now(timezone.utc)},
        )
    yield
    with e.begin() as c:
        c.execute(text("DELETE FROM watcher_heartbeats WHERE name LIKE 'test-api-%'"))


def test_api_watchers_returns_status_per_row():
    resp = client.get("/api/watchers")
    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == 1
    assert "checked_at" in body
    names = {w["name"]: w for w in body["watchers"]}
    assert "test-api-ok" in names
    assert names["test-api-ok"]["status"] == "ok"
