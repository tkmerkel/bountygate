"""Seeded sqlite + real FastAPI for Playwright. Usage: py -3.12 app/frontend/e2e/seed_api.py"""
import os
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta, timezone

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "app" / "shared" / "python"))

db_path = pathlib.Path(tempfile.gettempdir()) / "bg_e2e_seed.db"
if db_path.exists():
    db_path.unlink()
os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

from sqlalchemy import create_engine, text  # noqa: E402

EID = "11111111-1111-1111-1111-111111111111"

# Runtime timestamps so they satisfy the routers' Python-computed cutoffs at REQUEST
# time: /arbs has a 30-min last_seen_at cutoff, /props a 24h captured_at cutoff. These
# MUST be computed at seed time (close enough to request time), never hardcoded.
_NOW = datetime.now(timezone.utc)
NOW = _NOW.isoformat()
NOW_MINUS_1H = (_NOW - timedelta(hours=1)).isoformat()
# A future commence so the seeded prop event reads as upcoming (mirrors props UI intent).
COMMENCE_FUTURE = (_NOW + timedelta(hours=6)).isoformat()

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
    # sports_events: props.py JOINs this for matchup dims (mig 005). Simplified columns.
    "CREATE TABLE sports_events (event_id text, source_event_id text, sport_key text, "
    "commence_time text, home_team text, away_team text)",
    # arb_opportunities (mig 018). roi/fee_adjusted_roi + last_seen_at drive the ledger.
    "CREATE TABLE arb_opportunities (opportunity_hash text, first_detected_at text, "
    "last_seen_at text, kind text, pairing text, event_id text, sport_key text, "
    "home_team text, away_team text, commence_time text, market_segment text, "
    "player_name text, line real, pairing_type text, leg_a_kind text, leg_a_source text, "
    "leg_a_outcome text, leg_a_point real, leg_a_price real, leg_a_stake real, "
    "leg_b_kind text, leg_b_source text, leg_b_outcome text, leg_b_point real, "
    "leg_b_price real, leg_b_stake real, payout real, arb_ev real, roi real, "
    "fee_adjusted_roi real, hours_until_commence real, details text)",
    # player_props_odds_history (mig 017). captured_at drives the 24h cutoff.
    "CREATE TABLE player_props_odds_history (event_id text, market_key text, "
    "player_name text, line real, side text, bookmaker text, decimal_price real, "
    "captured_at text)",
    # venue_sharpness + mart_calibration (mig 014). Feed /sharpness and /calibration.
    "CREATE TABLE venue_sharpness (venue_key text, sport_key text, score_window text, "
    "n_games int, brier real, logloss real, avg_clv real, computed_at text)",
    "CREATE TABLE mart_calibration (source text, sport_key text, prob_bucket real, "
    "n int, predicted_mean real, realized_rate real, computed_at text)",
    # mart_edge_signals (mig 006/007). NOT previously seeded — first rows added here.
    "CREATE TABLE mart_edge_signals (signal_id text, detected_at text, venue_key text, "
    "market_id text, outcome_id text, signal_type text, fair_prob real, "
    "venue_price real, edge real, kelly_fraction real)",
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

# A second event for the NBA prop seeds (Jayson Tatum). Distinct id from the MLB EID so
# mart_fair_odds rows are untouched. commence is in the future (props UI intent).
PID = "33333333-3333-3333-3333-333333333333"

# Inserts that depend on runtime timestamps (cutoff-sensitive routers). Built here so
# arb last_seen_at / prop captured_at land inside the 30-min / 24h windows at request time.
RUNTIME_INSERTS = [
    # sports_events row backing the prop event (props.py JOIN). MLB event row too so any
    # future joiners resolve; both harmless.
    f"INSERT INTO sports_events VALUES ('{PID}','nba:BOS@NYK','basketball_nba',"
    f"'{COMMENCE_FUTURE}','New York Knicks','Boston Celtics')",
    f"INSERT INTO sports_events VALUES ('{EID}','mlb:BOS@NYY','baseball_mlb',"
    "'2026-06-10T19:00:00Z','New York Yankees','Boston Red Sox')",
    # --- arb_opportunities: (a) game book×book, (b) prop book×venue. last_seen_at fresh.
    "INSERT INTO arb_opportunities VALUES "
    f"('arb-game-1','{NOW_MINUS_1H}','{NOW}','game','book_book','{EID}','baseball_mlb',"
    "'New York Yankees','Boston Red Sox','2026-06-10T19:00:00Z','h2h',NULL,NULL,NULL,"
    "'book','fanduel','New York Yankees',NULL,2.10,100.0,"
    "'book','betmgm','Boston Red Sox',NULL,2.10,100.0,"
    "210.0,5.0,0.05,0.05,18.0,NULL)",
    "INSERT INTO arb_opportunities VALUES "
    f"('arb-prop-1','{NOW_MINUS_1H}','{NOW}','prop','book_venue','{PID}','basketball_nba',"
    "'New York Knicks','Boston Celtics','" + COMMENCE_FUTURE + "','player_points',"
    "'Jayson Tatum',29.5,NULL,"
    "'book','fanduel','Over',29.5,1.95,100.0,"
    "'venue','kalshi','Yes',NULL,0.45,100.0,"
    "211.0,5.7,0.0569,0.0569,6.0,'{\"title\": \"Tatum 29.5+ pts\"}')",
    # --- player_props_odds_history: Jayson Tatum, player_points, line 29.5,
    #     over+under x fanduel+betmgm. captured_at now → inside 24h cutoff.
    f"INSERT INTO player_props_odds_history VALUES ('{PID}','player_points',"
    f"'Jayson Tatum',29.5,'over','fanduel',1.95,'{NOW}')",
    f"INSERT INTO player_props_odds_history VALUES ('{PID}','player_points',"
    f"'Jayson Tatum',29.5,'over','betmgm',2.05,'{NOW}')",
    f"INSERT INTO player_props_odds_history VALUES ('{PID}','player_points',"
    f"'Jayson Tatum',29.5,'under','fanduel',1.90,'{NOW}')",
    f"INSERT INTO player_props_odds_history VALUES ('{PID}','player_points',"
    f"'Jayson Tatum',29.5,'under','betmgm',1.85,'{NOW}')",
    # --- venue_sharpness: pinnacle sharper (lower brier) than fanduel.
    f"INSERT INTO venue_sharpness VALUES ('pinnacle','baseball_mlb','all',250,0.21,"
    f"0.55,0.012,'{NOW}')",
    f"INSERT INTO venue_sharpness VALUES ('fanduel','baseball_mlb','all',250,0.24,"
    f"0.60,-0.004,'{NOW}')",
    # --- mart_calibration: 4 buckets for pinnacle (visible line + dots).
    f"INSERT INTO mart_calibration VALUES ('pinnacle','baseball_mlb',0.3,40,0.30,0.28,'{NOW}')",
    f"INSERT INTO mart_calibration VALUES ('pinnacle','baseball_mlb',0.5,40,0.50,0.52,'{NOW}')",
    f"INSERT INTO mart_calibration VALUES ('pinnacle','baseball_mlb',0.7,40,0.70,0.69,'{NOW}')",
    f"INSERT INTO mart_calibration VALUES ('pinnacle','baseball_mlb',0.9,40,0.90,0.88,'{NOW}')",
    # --- mart_edge_signals: one EV signal.
    f"INSERT INTO mart_edge_signals VALUES ('44444444-4444-4444-4444-444444444444','{NOW}',"
    "'kalshi','55555555-5555-5555-5555-555555555555',NULL,'ev',0.55,0.48,0.07,0.03)",
]

INSERTS = INSERTS + RUNTIME_INSERTS

engine = create_engine(os.environ["DATABASE_URL"])
with engine.begin() as conn:
    for stmt in DDL + INSERTS:
        conn.execute(text(stmt))
engine.dispose()

import uvicorn  # noqa: E402

from app.web.main import app  # noqa: E402

uvicorn.run(app, host="127.0.0.1", port=8765)
