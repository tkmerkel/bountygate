"""Hourly closing-line derivation (last pre-commence snapshot) + ingest-gap alert."""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

from bountygate.models import derive_closing_lines_db


@dag(
    dag_id="derive_closing_lines",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["quant", "tier1"],
)
def derive_closing_lines():
    @task(outlets=[Asset(name="closing_lines")])
    def derive() -> int:
        n_events, stale = derive_closing_lines_db()
        print(f"[derive_closing_lines] events={n_events} stale={len(stale)}")
        if stale:
            from bountygate.utils.discord_notify import notify
            lines = ", ".join(f"{eid[:8]}…({mins}m)" for eid, mins in stale[:10])
            notify(
                f"closing-line staleness >60m on {len(stale)} event(s): {lines}",
                level="warning", source="derive_closing_lines",
            )
        return n_events

    derive()


dag = derive_closing_lines()
