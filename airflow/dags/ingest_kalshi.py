"""Read-only Kalshi ingestion poller. Fetches market snapshots every 5 min and
lands them in raw_market_snapshots. Emits the raw asset for downstream normalization."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.connectors.landing import RAW_TABLE, land_raw
from bountygate.connectors.registry import get_connector

RAW_ASSET = Asset(f"postgres://{RAW_TABLE}")


@dag(
    dag_id="ingest_kalshi",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["ingest", "kalshi"],
)
def ingest_kalshi():
    @task(outlets=[RAW_ASSET])
    def fetch_and_land() -> int:
        records = get_connector("kalshi").fetch_snapshots()
        n = land_raw(records)
        print(f"[ingest_kalshi] landed {n} records")
        return n

    fetch_and_land()


dag = ingest_kalshi()
