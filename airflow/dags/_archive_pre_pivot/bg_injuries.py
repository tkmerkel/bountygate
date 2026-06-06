"""DAG: bg_injuries — player availability snapshots from ESPN.

Independent hourly cron. For each in-season sport (distinct sport_key on
upcoming dim_event rows, restricted to sports ESPN covers), pull the league
injury report and append rows into dim_player_status.

status_hash = md5(player_id|report_at_utc|source) per migration 011, so a given
report for a player lands once; a new report (new date) is a new row, and
vw_player_current_status surfaces the latest per player.

Player matching is name-based (uuid5 over sport_key|normalized_name), identical
to the dim model, so statuses key to the same player_id as props/stats.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from bg_arb_pipeline_lib.db import bulk_append_new
from bountygate.enrichment import clients, results
from bountygate.utils.db_connection import fetch_data


def _status_hash(player_id, report_at_utc, source) -> str:
    raw = f"{player_id}|{report_at_utc}|{source}"
    return hashlib.md5(raw.encode()).hexdigest()


@dag(
    dag_id="bg_injuries",
    description="Snapshot player injury/availability from ESPN per in-season sport",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "enrichment", "injuries"],
)
def bg_injuries_pipeline() -> None:

    @task()
    def active_sports() -> list[str]:
        df = fetch_data(
            "SELECT DISTINCT sport_key FROM dim_event "
            "WHERE commence_at_utc >= now() - interval '1 day' "
            "  AND commence_at_utc <= now() + interval '7 days'"
        )
        if df is None or df.empty:
            raise AirflowSkipException("No in-season sports")
        sports = [
            s for s in df["sport_key"].dropna().astype(str).tolist()
            if s in clients.ESPN_PATHS
        ]
        if not sports:
            raise AirflowSkipException("No ESPN-covered in-season sports")
        return sports

    @task()
    def snapshot_injuries(sports: list[str]) -> int:
        captured_at = pd.Timestamp.utcnow().tz_localize(None)
        rows: list[dict] = []
        for sport_key in sports:
            payload = clients.fetch_json(clients.build_espn_injuries_url(sport_key))
            if not payload:
                continue
            for r in results.parse_espn_injuries(payload, sport_key):
                report_at = r.get("report_at_utc") or str(captured_at)
                rows.append(
                    {
                        "status_hash": _status_hash(
                            r["player_id"], report_at, r["source"]
                        ),
                        "player_id": r["player_id"],
                        "player_name": r["player_name"],
                        "sport_key": r["sport_key"],
                        "team_id": None,
                        "status": r["status"],
                        "description": r["description"],
                        "report_at_utc": report_at,
                        "source": r["source"],
                    }
                )
        if not rows:
            raise AirflowSkipException("No injury rows returned")
        out = pd.DataFrame(rows).drop_duplicates(subset=["status_hash"])
        out["report_at_utc"] = pd.to_datetime(out["report_at_utc"], errors="coerce", utc=True)
        out["report_at_utc"] = out["report_at_utc"].dt.tz_localize(None)
        return int(bulk_append_new(out, "dim_player_status", "status_hash"))

    snapshot_injuries(active_sports())


dag = bg_injuries_pipeline()
