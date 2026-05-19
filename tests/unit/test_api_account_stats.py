import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.web.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def seed_stats():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    e = create_engine(url)
    with e.begin() as c:
        c.execute(text("DELETE FROM account_stats WHERE book LIKE 'test-%'"))
        c.execute(
            text(
                "INSERT INTO account_stats (book, balance, pending_wagers, available_liquidity, "
                "pnl_7d, scrape_status, scraped_at) VALUES "
                "('test-fanduel', 2847.23, 340.00, 2507.23, 142.50, 'ok', :now)"
            ),
            {"now": datetime.now(timezone.utc)},
        )
    yield
    with e.begin() as c:
        c.execute(text("DELETE FROM account_stats WHERE book LIKE 'test-%'"))


def test_api_account_stats_returns_book_keyed():
    resp = client.get("/api/account-stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == 1
    assert body["stale_after_minutes"] == 120
    assert body["books"]["test-fanduel"]["scrape_status"] == "ok"
    assert body["books"]["test-fanduel"]["balance"] == 2847.23
