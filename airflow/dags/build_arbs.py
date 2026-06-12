"""Arb detection: asset-triggered by the book + venue time-series tables.

Runs the pure ``bountygate.arb`` builders over fresh book/venue rows and upserts
into arb_opportunities (hash PK; ON CONFLICT bumps last_seen_at). Emits the
``arb_opportunities`` asset downstream consumers can subscribe to.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.arb.build import run_build_arbs

ODDS_ASSET = Asset(name="sportsbook_odds_history")
PROPS_ASSET = Asset(name="player_props_odds_history")
PRICE_ASSET = Asset(name="price_history")


@dag(
    dag_id="build_arbs",
    schedule=[ODDS_ASSET, PROPS_ASSET, PRICE_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["arb", "detect"],
)
def build_arbs():
    @task(outlets=[Asset(name="arb_opportunities")])
    def detect() -> dict:
        counts = run_build_arbs()
        print(f"[build_arbs] {counts}")
        return counts

    detect()


dag = build_arbs()
