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
BASE_WAGER = 100.0


@dag(
    dag_id="bg_arb_pipeline",
    schedule="*/10 * * * *",  # every 10 minutes; tune via Airflow UI
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
        opps = build_opportunities(lines, base_wager=BASE_WAGER)
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

    stage_count = ingest_odds_task()
    opp_count = build_opportunities_task(stage_count)
    record_history_task(opp_count)


dag = bg_arb_pipeline_dag()
