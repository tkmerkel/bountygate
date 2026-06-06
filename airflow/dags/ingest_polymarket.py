"""Read-only Polymarket ingestion poller (Gamma API). Every 5 min -> raw_market_snapshots."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.connectors.landing import RAW_TABLE, land_raw
from bountygate.connectors.registry import get_connector

RAW_ASSET = Asset(f"postgres://{RAW_TABLE}")


@dag(
    dag_id="ingest_polymarket",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["ingest", "polymarket"],
)
def ingest_polymarket():
    @task(outlets=[RAW_ASSET])
    def fetch_and_land() -> int:
        records = get_connector("polymarket").fetch_snapshots()
        n = land_raw(records)
        print(f"[ingest_polymarket] landed {n} records")
        return n

    fetch_and_land()


dag = ingest_polymarket()
