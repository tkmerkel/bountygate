"""Hourly ESPN finals -> game_results."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.models import ingest_game_results


@dag(
    dag_id="ingest_results",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["quant", "ingest"],
)
def ingest_results():
    @task(outlets=[Asset(name="game_results")])
    def fetch() -> int:
        n = ingest_game_results()
        print(f"[ingest_results] upserted {n} finals")
        return n

    fetch()


dag = ingest_results()
