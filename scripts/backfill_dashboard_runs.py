#!/usr/bin/env python3
"""One-time backfill: read dashboard/data.json and insert each run into dashboard_runs.

Idempotent via ON CONFLICT (run_id) DO NOTHING. Safe to re-run.

Handles the actual data.json shape (recording/review top-level keys, naive
timestamps without timezone) — the plan-spec sketch had slightly different
field names; this is the live schema.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def _parse_ts(s: str) -> datetime:
    """Parse the data.json timestamp. Treats naive ISO as UTC."""
    s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        from datetime import timezone
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main() -> int:
    load_dotenv()
    data_path = Path(__file__).resolve().parents[1] / "dashboard" / "data.json"
    if not data_path.exists():
        print(f"no file at {data_path} — nothing to backfill")
        return 0

    payload = json.loads(data_path.read_text(encoding="utf-8"))
    runs = payload.get("runs", [])
    if not runs:
        print("file has no runs — done")
        return 0

    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    engine = create_engine(url)
    inserted = 0
    skipped = 0
    with engine.begin() as conn:
        for r in runs:
            issues = r.get("issues", {})
            occurred_at = _parse_ts(r["timestamp"])
            video_url = r.get("recording") or (r.get("links") or {}).get("video")
            review_url = r.get("review") or (r.get("links") or {}).get("review")
            res = conn.execute(text(
                """
                INSERT INTO dashboard_runs (
                    run_id, occurred_at, player, market, outcome, duration_s, issues,
                    top_finding, video_url, review_url
                ) VALUES (
                    :run_id, :occurred_at, :player, :market, :outcome, :duration_s, :issues,
                    :top_finding, :video_url, :review_url
                )
                ON CONFLICT (run_id) DO NOTHING
                """
            ), {
                "run_id": r["run_id"],
                "occurred_at": occurred_at,
                "player": r.get("player", ""),
                "market": r.get("market", ""),
                "outcome": r["outcome"],
                "duration_s": r.get("duration_s"),
                "issues": json.dumps(issues),
                "top_finding": r.get("top_finding"),
                "video_url": video_url,
                "review_url": review_url,
            })
            if res.rowcount:
                inserted += 1
            else:
                skipped += 1
    print(f"inserted {inserted} new rows, skipped {skipped} existing (of {len(runs)} in file)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
