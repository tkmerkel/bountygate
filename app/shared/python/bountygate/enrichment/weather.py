"""Open-Meteo forecast URL + nearest-hour parse.

The weather DAG looks up a ballpark's lat/lon (venues.lookup_park), fetches the
hourly forecast, and selects the hour closest to first pitch. Times are
requested in UTC (timezone=UTC) so they compare directly to commence_at_utc.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

_BASE = "https://api.open-meteo.com/v1/forecast"
_HOURLY = (
    "temperature_2m,wind_speed_10m,wind_direction_10m,"
    "precipitation_probability,relative_humidity_2m"
)


def build_forecast_url(lat: float, lon: float, *, forecast_days: int = 2) -> str:
    return (
        f"{_BASE}?latitude={lat}&longitude={lon}"
        f"&hourly={_HOURLY}"
        "&temperature_unit=fahrenheit&wind_speed_unit=mph"
        "&timezone=UTC"
        f"&forecast_days={int(forecast_days)}"
    )


def _parse_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def nearest_hourly(payload: dict, target) -> Optional[dict]:
    """Return the forecast fields for the hour closest to ``target`` (ISO/naive
    UTC accepted), or None if the payload or target is unusable."""
    target_dt = _parse_dt(target)
    if target_dt is None:
        return None
    hourly = (payload or {}).get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return None

    best_idx = None
    best_delta = None
    for i, t in enumerate(times):
        tdt = _parse_dt(t)
        if tdt is None:
            continue
        delta = abs((tdt - target_dt).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = i
    if best_idx is None:
        return None

    def _at(field):
        arr = hourly.get(field) or []
        return arr[best_idx] if best_idx < len(arr) else None

    return {
        "temp_f": _at("temperature_2m"),
        "wind_mph": _at("wind_speed_10m"),
        "wind_dir_deg": _at("wind_direction_10m"),
        "precip_prob": _at("precipitation_probability"),
        "humidity": _at("relative_humidity_2m"),
        "forecast_at_utc": times[best_idx],
    }
