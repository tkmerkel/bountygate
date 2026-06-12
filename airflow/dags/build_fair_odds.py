"""sportsbook snapshots -> fair_prices + consensus_v1 + mart_fair_odds."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.models import build_fair_prices

ODDS_ASSET = Asset(name="sportsbook_odds_history")


@dag(
    dag_id="build_fair_odds",
    schedule=[ODDS_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["quant", "tier1"],
)
def build_fair_odds():
    @task(outlets=[Asset(name="fair_prices")])
    def fair() -> int:
        n = build_fair_prices()
        print(f"[build_fair_odds] fair_prices rows={n}")
        return n

    fair()


dag = build_fair_odds()
