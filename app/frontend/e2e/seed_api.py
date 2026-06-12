"""Seeded sqlite + real FastAPI for Playwright. Usage: py -3.12 app/frontend/e2e/seed_api.py"""
import os
import pathlib
import sys
import tempfile

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "app" / "shared" / "python"))

db_path = pathlib.Path(tempfile.gettempdir()) / "bg_e2e_seed.db"
if db_path.exists():
    db_path.unlink()
os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

from sqlalchemy import create_engine, text  # noqa: E402

EID = "11111111-1111-1111-1111-111111111111"

DDL = [
    "CREATE TABLE mart_fair_odds (event_id text, sport_key text, commence_time text, "
    "home_team text, away_team text, market_type text, outcome_name text, "
    "consensus_prob real, best_price real, best_bookmaker text, edge real, computed_at text)",
    "CREATE TABLE sportsbook_odds_history (event_id text, market_type text, bookmaker text, "
    "outcome_name text, captured_at text, decimal_price real)",
    "CREATE TABLE closing_lines (event_id text, market_type text, bookmaker text, "
    "outcome_name text, decimal_price real, fair_prob real, captured_at text, staleness_minutes real)",
    "CREATE TABLE mart_cross_market_prices (question_key text, captured_at text, kalshi_prob real, "
    "polymarket_prob real, sportsbook_consensus_prob real, max_spread real)",
    "CREATE TABLE markets (market_id text, venue_key text, external_id text, title text, "
    "category text, status text, open_time text, close_time text, resolved_outcome text, "
    "resolution_time text, updated_at text)",
]

INSERTS = [
    f"INSERT INTO mart_fair_odds VALUES ('{EID}','baseball_mlb','2026-06-10T19:00:00Z',"
    "'New York Yankees','Boston Red Sox','h2h','New York Yankees',0.62,1.72,'fanduel',0.0664,"
    "'2026-06-10T18:00:00Z')",
    f"INSERT INTO mart_fair_odds VALUES ('{EID}','baseball_mlb','2026-06-10T19:00:00Z',"
    "'New York Yankees','Boston Red Sox','h2h','Boston Red Sox',0.38,2.80,'draftkings',0.064,"
    "'2026-06-10T18:00:00Z')",
] + [
    f"INSERT INTO sportsbook_odds_history VALUES ('{EID}','h2h','fanduel','New York Yankees',"
    f"'2026-06-10T1{i}:00:00Z',1.{70 + i})"
    for i in range(5)
] + [
    # closing captured_at matches the last snapshot (mirrors derive_closing, and
    # keeps the chart's ReferenceDot inside the x-domain)
    f"INSERT INTO closing_lines VALUES ('{EID}','h2h','fanduel','New York Yankees',1.74,0.605,"
    "'2026-06-10T14:00:00Z',300.0)",
    f"INSERT INTO closing_lines VALUES ('{EID}','h2h','consensus','New York Yankees',NULL,0.61,"
    "'2026-06-10T14:00:00Z',300.0)",
    "INSERT INTO mart_cross_market_prices VALUES ('mlb:2026-06-10:BOS@NYY:NYY',"
    "'2026-06-10T18:00:00Z',0.60,0.59,0.62,0.03)",
    "INSERT INTO markets VALUES ('22222222-2222-2222-2222-222222222222','kalshi','KX1',"
    "'Yankees beat Red Sox','Sports','open','2026-06-09T00:00:00Z','2026-06-10T19:00:00Z',"
    "NULL,NULL,'2026-06-10T18:00:00Z')",
]

engine = create_engine(os.environ["DATABASE_URL"])
with engine.begin() as conn:
    for stmt in DDL + INSERTS:
        conn.execute(text(stmt))
engine.dispose()

import uvicorn  # noqa: E402

from app.web.main import app  # noqa: E402

uvicorn.run(app, host="127.0.0.1", port=8765)
