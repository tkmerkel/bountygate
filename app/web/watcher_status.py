"""Pure function mapping a watcher_heartbeats row to ok|amber|red.

Lives next to the FastAPI route so the dashboard never needs threshold constants.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Mapping

Status = Literal["ok", "amber", "red"]


def compute_status(hb: Mapping[str, Any]) -> Status:
    now = datetime.now(timezone.utc)
    interval = int(hb["expected_interval_s"])
    last_tick = hb["last_tick_at"]
    if last_tick.tzinfo is None:
        last_tick = last_tick.replace(tzinfo=timezone.utc)
    tick_age_s = (now - last_tick).total_seconds()

    if int(hb.get("errors_24h") or 0) > 0:
        return "red"
    if tick_age_s > 6 * interval:
        return "red"
    if (hb.get("scrape_status") or "ok") != "ok":
        return "red"

    pending = int(hb.get("pending_count") or 0)
    oldest = hb.get("oldest_pending_age_s")
    if pending > 0 and oldest is not None and oldest > 15 * 60:
        return "amber"
    if tick_age_s > 2 * interval:
        return "amber"

    return "ok"
