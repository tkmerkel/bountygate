"""bg_arb_pipeline: self-contained arb data pipeline.

Pulls odds from the-odds-api, builds the cartesian of arb-able pairs across
all four std/alt pairing directions, writes:
  - bg_arb_stage_lines              (replaced every run)
  - bg_arbitrage_opportunities      (replaced every run)
  - bg_arb_opportunities_history    (appended)

Decoupled from bg_unified_* — owns its own ingest and tables.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.sdk import Asset
from sqlalchemy import create_engine, text

# Ensure the per-package library is importable inside Airflow runtime.
_dag_dir = os.path.dirname(os.path.abspath(__file__))
if _dag_dir not in sys.path:
    sys.path.insert(0, _dag_dir)

from bg_arb_pipeline_lib.builder import build_opportunities  # noqa: E402
from bg_arb_pipeline_lib.db import bulk_append_new, bulk_replace  # noqa: E402
from bg_arb_pipeline_lib.ingest import ingest_all  # noqa: E402

# Shared helpers (already on PYTHONPATH inside the Airflow image).
from bountygate.utils.db_connection import fetch_data  # noqa: E402
from bountygate.utils.etl_assets import odds_apiKey, odds_url  # noqa: E402

# Completion asset (downstream DAGs / bot can wait on this).
arb_opportunities_ready_asset = Asset("bg_arbitrage_opportunities_ready")

STAGE_TABLE = "bg_arb_stage_lines"
OPP_TABLE = "bg_arbitrage_opportunities"
HISTORY_TABLE = "bg_arb_opportunities_history"
QUEUE_TABLE = "bot_execution_queue"
BASE_WAGER = 100.0

# Executor-side filter — must match arbitrage_executor/opportunity.py.
# Only opportunities that satisfy these conditions are bet-eligible (FD×MGM
# only; DK/Caesars rows from PR #8 flow into the opps table for price
# comparison but are gated out of execution).
#
# This is the **DAG enqueue gate**: whether the DAG inserts a PENDING row
# that wakes the worker. Should be set in lockstep with the executor's
# MIN_ROI_THRESHOLD (arbitrage_executor/opportunity.py) — if the DAG
# enqueue gate is stricter than the executor gate, the worker never
# fires on the looser band. See arbitrage_executor/LOGIC.md "ROI
# gating — two gates, two purposes".
MIN_QUALIFYING_ROI = float(os.environ.get("MIN_QUALIFYING_ROI", "0.005"))
EXECUTABLE_BOOKS = ("fanduel", "betmgm")
EXECUTABLE_SPORTS = ("NBA", "NHL", "NFL", "MLB")

# Builder-side storage-protection floor (NOT a policy gate). Default 0.0
# drops negative-ROI cartesian pairs before writing to keep table cardinality
# sane. Lower (e.g. -0.01) via env var to surface near-miss arbs for
# analysis. The policy gate is the executor's MIN_ROI_THRESHOLD. See
# arbitrage_executor/LOGIC.md.
BUILDER_MIN_ROI = float(os.environ.get("BUILDER_MIN_ROI", "0.0"))


@dag(
    dag_id="bg_arb_pipeline",
    schedule="*/5 * * * *",  # every 5 minutes; tune via Airflow UI
    catchup=False,
    max_active_runs=1,  # one run at a time — bulk_replace TRUNCATEs the table
    start_date=datetime(2026, 5, 15, tzinfo=timezone.utc),
    tags=["arb", "the-odds-api"],
    default_args={"retries": 1},
)
def bg_arb_pipeline_dag():

    @task()
    def ingest_odds_task() -> int:
        """Fetch every (sport, event) odds page and write to bg_arb_stage_lines."""
        rows = ingest_all(api_key=odds_apiKey, base_url=odds_url)
        if not rows:
            print("[ingest] no rows fetched")
            bulk_replace(pd.DataFrame(), STAGE_TABLE)
            return 0
        df = pd.DataFrame(rows)
        bulk_replace(df, STAGE_TABLE)
        print(f"[ingest] wrote {len(df)} stage rows")
        return int(len(df))

    @task(outlets=[arb_opportunities_ready_asset])
    def build_opportunities_task(stage_row_count: int) -> int:
        """Read stage lines, build opportunities, write bg_arbitrage_opportunities."""
        if stage_row_count == 0:
            print("[build] no stage rows; clearing opportunities table")
            bulk_replace(pd.DataFrame(), OPP_TABLE)
            return 0
        lines = fetch_data(f"SELECT * FROM {STAGE_TABLE}")
        if lines is None or lines.empty:
            bulk_replace(pd.DataFrame(), OPP_TABLE)
            return 0
        opps = build_opportunities(lines, base_wager=BASE_WAGER, min_roi=BUILDER_MIN_ROI)
        if opps.empty:
            print("[build] no arb-able pairs found")
            bulk_replace(pd.DataFrame(), OPP_TABLE)
            return 0
        bulk_replace(opps, OPP_TABLE)
        print(f"[build] wrote {len(opps)} opportunities")
        return int(len(opps))

    @task()
    def record_history_task(opportunity_count: int) -> int:
        """Append unseen opportunity hashes to history."""
        if opportunity_count == 0:
            return 0
        current = fetch_data(f"SELECT * FROM {OPP_TABLE}")
        if current is None or current.empty:
            return 0
        inserted = bulk_append_new(current, HISTORY_TABLE, key_column="opportunity_hash")
        print(f"[history] appended {inserted} new rows")
        return int(inserted)

    @task()
    def enqueue_opportunities_task(opportunity_count: int) -> int:
        """Insert a PENDING row into bot_execution_queue when a fresh FD×MGM arb
        exists and the queue isn't already backed up.

        Replaces the queue-insert task from the retired bg_arbitrage_player_props
        DAG (asset-trigger no longer fires after PR #2). Strategy:

          1. If no opportunities at all this run, nothing to do.
          2. If no FD×MGM-executable opportunity above MIN_QUALIFYING_ROI in
             the eligible time window, nothing to do — the executor would
             skip it anyway.
          3. If a PENDING row already exists, nothing to do — task_worker
             hasn't drained the previous tick yet, no point piling up.
          4. Otherwise, insert one PENDING row. task_worker picks it up
             within 15s and runs execute_arb against the current opps table.

        Returns the number of PENDING rows inserted (0 or 1).
        """
        if opportunity_count == 0:
            return 0

        books_sql = ",".join(f"'{b}'" for b in EXECUTABLE_BOOKS)
        sports_sql = ",".join(f"'{s}'" for s in EXECUTABLE_SPORTS)
        # Mirror the executor's NOT EXISTS dedup in arbitrage_executor/
        # opportunity.py — without it we'd enqueue PENDING rows for opps
        # the executor immediately deduplicates (a successful bet today
        # gets recorded in bg_executed_opportunities; same player+market
        # opp later that day is skipped by the executor but would still
        # have woken the worker for nothing). Per-leg OR match because a
        # successful bet's (under_market_key, over_market_key) pair should
        # block ANY future opp sharing either side.
        eligible_query = f"""
            SELECT COUNT(*) AS n FROM {OPP_TABLE} bao
            WHERE under_book IN ({books_sql})
              AND over_book IN ({books_sql})
              AND sport_title IN ({sports_sql})
              AND roi >= {MIN_QUALIFYING_ROI}
              AND hours_until_commence BETWEEN 0.03 AND 24
              AND NOT EXISTS (
                  SELECT 1 FROM bg_executed_opportunities eo
                  WHERE eo.player_name = bao.player_name
                    AND (eo.under_market_key = bao.under_market_key
                         OR eo.over_market_key = bao.over_market_key)
                    AND eo.executed_at_utc >= CURRENT_DATE
              )
        """
        df = fetch_data(eligible_query)
        n_eligible = int(df.iloc[0, 0]) if df is not None and not df.empty else 0
        if n_eligible == 0:
            print("[enqueue] no FD×MGM executable arbs; not enqueuing")
            return 0

        pending_query = f"SELECT COUNT(*) AS n FROM {QUEUE_TABLE} WHERE status = 'PENDING'"
        df = fetch_data(pending_query)
        n_pending = int(df.iloc[0, 0]) if df is not None and not df.empty else 0
        if n_pending > 0:
            print(f"[enqueue] {n_pending} PENDING already; not piling up")
            return 0

        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            from bountygate.utils.db_connection import DATABASE_URL as _DB
            database_url = _DB
        engine = create_engine(database_url)
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    f"INSERT INTO {QUEUE_TABLE} (status) VALUES ('PENDING')"
                ))
            print(f"[enqueue] inserted 1 PENDING row ({n_eligible} eligible arb(s) waiting)")
            return 1
        finally:
            engine.dispose()

    stage_count = ingest_odds_task()
    opp_count = build_opportunities_task(stage_count)
    record_history_task(opp_count)
    enqueue_opportunities_task(opp_count)


dag = bg_arb_pipeline_dag()
