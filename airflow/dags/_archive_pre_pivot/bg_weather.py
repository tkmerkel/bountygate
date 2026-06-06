"""DAG: bg_weather — first-pitch weather forecast for upcoming outdoor games.

Independent 3-hourly cron. For upcoming MLB events at outdoor parks, look up the
ballpark lat/lon (dim_event.venue is empty, so we key on the home team via
bountygate.enrichment.venues), fetch the Open-Meteo hourly forecast, and record
the hour closest to commence into fact_weather.

weather_hash = md5(bg_event_id|forecast_at_utc) per migration 011, so each
distinct forecast hour lands once; re-runs that resolve to the same hour are
deduped, while a refreshed forecast (different forecast hour) accumulates.
Domed parks are skipped (weather is a no-op indoors).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from bg_arb_pipeline_lib.db import bulk_append_new
from bountygate.enrichment import clients, venues, weather
from bountygate.utils.db_connection import fetch_data


def _weather_hash(bg_event_id, forecast_at_utc) -> str:
    raw = f"{bg_event_id}|{forecast_at_utc}"
    return hashlib.md5(raw.encode()).hexdigest()


@dag(
    dag_id="bg_weather",
    description="Open-Meteo first-pitch forecast for upcoming outdoor MLB games",
    schedule="0 */3 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "enrichment", "weather"],
)
def bg_weather_pipeline() -> None:

    @task()
    def upcoming_outdoor_events() -> list[dict]:
        df = fetch_data(
            "SELECT bg_event_id, sport_key, home_team_name, commence_at_utc "
            "FROM dim_event "
            "WHERE sport_key = 'baseball_mlb' "
            "  AND commence_at_utc >= now() - interval '1 hour' "
            "  AND commence_at_utc <= now() + interval '30 hours'"
        )
        if df is None or df.empty:
            raise AirflowSkipException("No upcoming MLB events")
        df = df.copy()
        df["commence_at_utc"] = df["commence_at_utc"].astype(str)
        return df.to_dict("records")

    @task()
    def fetch_forecasts(events: list[dict]) -> int:
        rows: list[dict] = []
        for ev in events:
            park = venues.lookup_park(ev.get("home_team_name"))
            if park is None or park["is_dome"]:
                continue
            payload = clients.fetch_json(
                weather.build_forecast_url(park["lat"], park["lon"])
            )
            if not payload:
                continue
            fc = weather.nearest_hourly(payload, ev.get("commence_at_utc"))
            if fc is None:
                continue
            rows.append(
                {
                    "weather_hash": _weather_hash(
                        ev["bg_event_id"], fc["forecast_at_utc"]
                    ),
                    "bg_event_id": ev["bg_event_id"],
                    "sport_key": ev["sport_key"],
                    "forecast_at_utc": fc["forecast_at_utc"],
                    "commence_at_utc": ev.get("commence_at_utc"),
                    "temp_f": fc["temp_f"],
                    "wind_mph": fc["wind_mph"],
                    "wind_dir_deg": fc["wind_dir_deg"],
                    "precip_prob": fc["precip_prob"],
                    "humidity": fc["humidity"],
                    "source": "open-meteo",
                    "lat": park["lat"],
                    "lon": park["lon"],
                }
            )
        if not rows:
            raise AirflowSkipException("No outdoor forecasts produced")
        out = pd.DataFrame(rows).drop_duplicates(subset=["weather_hash"])
        for col in ("forecast_at_utc", "commence_at_utc"):
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True).dt.tz_localize(None)
        return int(bulk_append_new(out, "fact_weather", "weather_hash"))

    fetch_forecasts(upcoming_outdoor_events())


dag = bg_weather_pipeline()
