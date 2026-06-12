"""game_results -> venue_sharpness + mart_calibration (full recompute)."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.models import score_results_db

RESULTS_ASSET = Asset(name="game_results")


@dag(
    dag_id="score_results",
    schedule=[RESULTS_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["quant", "tier1"],
)
def score_results():
    @task(outlets=[Asset(name="venue_sharpness"), Asset(name="mart_calibration")])
    def score() -> dict:
        stats = score_results_db()
        print(f"[score_results] {stats}")
        return stats

    score()


dag = score_results()
