"""raw_market_snapshots -> normalized tables. Asset-triggered by the raw firehose."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.transforms.normalize import run_normalize

RAW_ASSET = Asset(name="raw_market_snapshots")
NORMALIZED_ASSETS = [
    Asset(name="markets"), Asset(name="market_outcomes"), Asset(name="price_history"),
    Asset(name="sports_events"), Asset(name="sportsbook_odds_history"),
    Asset(name="player_props_odds_history"),
]


@dag(
    dag_id="normalize",
    schedule=[RAW_ASSET],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["transform", "normalize"],
)
def normalize():
    @task(outlets=NORMALIZED_ASSETS)
    def normalize_raw() -> dict:
        counts = run_normalize()
        print(f"[normalize] {counts}")
        return counts

    normalize_raw()


dag = normalize()
