"""normalized -> marts. Asset-triggered by the normalized tables."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.transforms.marts import build_edge_signals, build_market_history

ODDS_ASSET = Asset(name="sportsbook_odds_history")
PRICE_ASSET = Asset(name="price_history")


@dag(
    dag_id="build_marts",
    schedule=[ODDS_ASSET, PRICE_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["transform", "marts"],
)
def build_marts():
    @task(outlets=[Asset(name="mart_edge_signals")])
    def edges() -> int:
        n = build_edge_signals()
        print(f"[build_marts] edge_signals rows={n}")
        return n

    @task(outlets=[Asset(name="mart_market_history")])
    def history() -> int:
        n = build_market_history()
        print(f"[build_marts] market_history rows={n}")
        return n

    edges()
    history()


dag = build_marts()
