import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.web.watcher_status import compute_status

ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = ROOT / "dashboard"

app = FastAPI(title="BountyGate")

db_url = os.environ.get("DATABASE_URL", "")
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)

engine: Optional[Engine] = None
if db_url:
    engine = create_engine(db_url, pool_pre_ping=True)  # type: ignore[assignment]


@app.get("/health")
def health():
    db_ok = False
    if engine is not None:
        try:
            with engine.connect() as c:
                c.execute(text("select 1"))
            db_ok = True
        except Exception:
            db_ok = False
    return {"status": "ok", "db": db_ok}


@app.get("/")
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/runs")
def api_runs(limit: int = 50):
    limit = max(1, min(limit, 500))
    if engine is None:
        return {"version": 1, "updated_at": None, "stale_after_minutes": 30, "runs": []}
    with engine.connect() as c:
        rows = c.execute(
            text(
                "SELECT run_id, occurred_at, player, market, outcome, duration_s, issues, "
                "top_finding, video_url, review_url FROM dashboard_runs "
                "ORDER BY occurred_at DESC LIMIT :lim"
            ),
            {"lim": limit},
        ).mappings().all()
    runs = []
    latest = None
    for r in rows:
        d = dict(r)
        d["occurred_at"] = d["occurred_at"].isoformat()
        d["duration_s"] = float(d["duration_s"]) if d["duration_s"] is not None else None
        runs.append(d)
        if latest is None or r["occurred_at"] > latest:
            latest = r["occurred_at"]
    return {
        "version": 1,
        "updated_at": latest.isoformat() if latest else None,
        "stale_after_minutes": 30,
        "runs": runs,
    }


@app.get("/api/account-stats")
def api_account_stats():
    if engine is None:
        return {"version": 1, "updated_at": None, "stale_after_minutes": 120, "books": {}}
    with engine.connect() as c:
        rows = c.execute(
            text(
                "SELECT book, balance, pending_wagers, available_liquidity, pnl_7d, "
                "scrape_status, last_error, scraped_at FROM account_stats"
            )
        ).mappings().all()
    books = {}
    latest = None
    for r in rows:
        d = dict(r)
        d["scraped_at"] = d["scraped_at"].isoformat()
        for k in ("balance", "pending_wagers", "available_liquidity", "pnl_7d"):
            d[k] = float(d[k]) if d[k] is not None else None
        books[r["book"]] = d
        if latest is None or r["scraped_at"] > latest:
            latest = r["scraped_at"]
    return {
        "version": 1,
        "updated_at": latest.isoformat() if latest else None,
        "stale_after_minutes": 120,
        "books": books,
    }


@app.get("/api/watchers")
def api_watchers():
    if engine is None:
        return {
            "version": 1,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "watchers": [],
        }
    with engine.connect() as c:
        rows = c.execute(
            text(
                "SELECT name, is_running, last_tick_at, pending_count, oldest_pending_age_s, "
                "completed_24h, errors_24h, last_error, expected_interval_s "
                "FROM watcher_heartbeats ORDER BY name"
            )
        ).mappings().all()
    watchers = []
    for r in rows:
        d = dict(r)
        d["last_tick_at"] = d["last_tick_at"].isoformat() if d["last_tick_at"] else None
        d["status"] = compute_status(r)
        watchers.append(d)
    return {
        "version": 1,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "watchers": watchers,
    }


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
