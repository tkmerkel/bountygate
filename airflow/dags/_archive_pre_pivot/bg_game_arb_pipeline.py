"""bg_game_arb_pipeline: game-line arbitrage detection (h2h/spreads/totals).

Self-contained sidecar pipeline. Pulls bulk odds via /v4/sports/{sport}/odds
for 5 sports × 4 books × 3 markets, builds 2-leg arbs across books, writes:
  - bg_game_arb_stage_lines             (replaced every run)
  - bg_arbitrage_game_opportunities     (replaced every run)
  - bg_arb_game_opportunities_history   (appended)

Discord alerts fire on new high-value opportunities only. v1 is detection-only;
no executor integration. Decoupled from bg_arb_pipeline (player props) and
bg_unified_* — owns its own ingest and tables.

DataFrames cannot move through XCom (Airflow 3 rejects them — see
bg_arbitrage_player_props.py for the same constraint). The history+alert
task fetches the current opportunities table itself rather than receiving it.

See: docs/superpowers/specs/2026-05-19-game-line-arb-pipeline-design.md
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.sdk import Asset

_dag_dir = os.path.dirname(os.path.abspath(__file__))
if _dag_dir not in sys.path:
    sys.path.insert(0, _dag_dir)

from bg_game_arb_pipeline_lib.alerts import notify_high_value_game_opportunities  # noqa: E402
from bg_game_arb_pipeline_lib.builder import build_game_opportunities  # noqa: E402
from bg_game_arb_pipeline_lib.db import bulk_append_new, bulk_replace  # noqa: E402
from bg_game_arb_pipeline_lib.ingest import ingest_all  # noqa: E402

from bountygate.utils.db_connection import fetch_data  # noqa: E402
from bountygate.utils.etl_assets import odds_apiKey, odds_url  # noqa: E402

game_arb_opportunities_ready_asset = Asset("bg_arbitrage_game_opportunities_ready")

STAGE_TABLE = "bg_game_arb_stage_lines"
OPP_TABLE = "bg_arbitrage_game_opportunities"
HISTORY_TABLE = "bg_arb_game_opportunities_history"
BASE_WAGER = 100.0


@dag(
    dag_id="bg_game_arb_pipeline",
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2026, 5, 19, tzinfo=timezone.utc),
    tags=["arb", "game-lines", "the-odds-api"],
    default_args={"retries": 1},
)
def bg_game_arb_pipeline_dag():

    @task()
    def ingest_game_odds_task() -> int:
        rows = ingest_all(api_key=odds_apiKey, base_url=odds_url)
        if not rows:
            print("[ingest] no rows fetched")
            bulk_replace(pd.DataFrame(), STAGE_TABLE)
            return 0
        df = pd.DataFrame(rows)
        bulk_replace(df, STAGE_TABLE)
        print(f"[ingest] wrote {len(df)} stage rows")
        return int(len(df))

    @task(outlets=[game_arb_opportunities_ready_asset])
    def build_game_opportunities_task(stage_row_count: int) -> int:
        if stage_row_count == 0:
            print("[build] no stage rows; clearing opportunities table")
            bulk_replace(pd.DataFrame(), OPP_TABLE)
            return 0
        lines = fetch_data(f"SELECT * FROM {STAGE_TABLE}")
        if lines is None or lines.empty:
            bulk_replace(pd.DataFrame(), OPP_TABLE)
            return 0
        opps = build_game_opportunities(lines, base_wager=BASE_WAGER)
        if opps.empty:
            print("[build] no arb-able game-line pairs found")
            bulk_replace(pd.DataFrame(), OPP_TABLE)
            return 0
        bulk_replace(opps, OPP_TABLE)
        print(f"[build] wrote {len(opps)} game-line opportunities")
        return int(len(opps))

    @task()
    def record_history_and_alert_task(opportunity_count: int) -> int:
        """Diff current opps against history, append new rows, alert on high-value.

        Returns the number of opportunities posted to Discord. New rows stay
        in-memory inside this task — DataFrames don't pass through XCom in
        Airflow 3.
        """
        if opportunity_count == 0:
            return 0
        current = fetch_data(f"SELECT * FROM {OPP_TABLE}")
        if current is None or current.empty:
            return 0

        try:
            existing = fetch_data(f"SELECT opportunity_hash FROM {HISTORY_TABLE}")
            existing_hashes = (
                set(existing["opportunity_hash"].astype(str).tolist())
                if existing is not None and not existing.empty
                else set()
            )
        except Exception as e:
            print(f"[history] failed reading existing hashes; treating all as new: {e}")
            existing_hashes = set()

        new_rows = current[~current["opportunity_hash"].astype(str).isin(existing_hashes)].copy()
        if new_rows.empty:
            print("[history] no new opportunities this run")
            return 0

        bulk_append_new(new_rows, HISTORY_TABLE, key_column="opportunity_hash")
        print(f"[history] appended {len(new_rows)} new rows")

        posted = notify_high_value_game_opportunities(new_rows)
        print(f"[alert] posted {posted} high-value opportunities to Discord")
        return posted

    stage_count = ingest_game_odds_task()
    opp_count = build_game_opportunities_task(stage_count)
    record_history_and_alert_task(opp_count)


dag = bg_game_arb_pipeline_dag()
