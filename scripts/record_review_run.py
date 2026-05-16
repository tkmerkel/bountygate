#!/usr/bin/env python3
"""Called by the review-watcher session at the end of each review.

Inserts the run into dashboard_runs and updates the review-watcher heartbeat.
Replaces the legacy step 5 (append-to-data.json) so all runtime state lives
in Postgres.

Usage:
    python scripts/record_review_run.py <audit_dir>
        --outcome success|failure|skipped|review_failed
        --duration-s <float>
        --issues-json <path-to-issues-json>
        --top-finding <"sentence">
        --pending-count <int>
        --oldest-pending-age-s <int|none>
        --completed-24h <int>

The watcher session computes the heartbeat counters (pending_count etc.) from
disk state by listing review.pending / review.done files. This script takes
those as args rather than re-deriving them.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


REPO = Path(__file__).resolve().parents[1]
SHARED = REPO / "app" / "shared" / "python"
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

from bountygate.watcher_heartbeat import heartbeat  # noqa: E402


_DIR_TS_RE = re.compile(r"^(\d{8}_\d{6})_")


def _parse_audit_dir(audit_dir: Path) -> dict:
    """Pull player/market/timestamp from the dir basename and opportunity_info.json."""
    base = audit_dir.name
    m = _DIR_TS_RE.match(base)
    if not m:
        raise SystemExit(f"audit dir basename does not start with timestamp: {base}")
    ts_str = m.group(1)
    # 20260515_093206 → 2026-05-15T09:32:06+00:00
    occurred_at = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)

    info_path = audit_dir / "opportunity_info.json"
    player = market = ""
    if info_path.exists():
        info = json.loads(info_path.read_text(encoding="utf-8"))
        player = info.get("player", "") or ""
        market = info.get("market", "") or ""

    return {
        "run_id": base,
        "occurred_at": occurred_at,
        "player": player,
        "market": market,
    }


def _file_url(p: Path) -> str:
    return f"file:///{p.as_posix().lstrip('/')}"


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("audit_dir", type=Path)
    ap.add_argument("--outcome", required=True,
                    choices=["success", "failure", "skipped", "review_failed"])
    ap.add_argument("--duration-s", type=float, default=None)
    ap.add_argument("--issues-json", type=Path, default=None,
                    help="path to a JSON file whose top-level object is the `issues` dict")
    ap.add_argument("--top-finding", default=None)
    ap.add_argument("--pending-count", type=int, default=0)
    ap.add_argument("--oldest-pending-age-s", type=int, default=None)
    ap.add_argument("--completed-24h", type=int, default=0)
    args = ap.parse_args()

    if not args.audit_dir.exists():
        raise SystemExit(f"audit dir does not exist: {args.audit_dir}")

    meta = _parse_audit_dir(args.audit_dir)
    outcome = args.outcome
    # dashboard_runs schema CHECK constraint accepts only success|failure|skipped.
    # Map review_failed → failure so we still get a row for visibility.
    db_outcome = "failure" if outcome == "review_failed" else outcome

    issues: dict = {}
    if args.issues_json and args.issues_json.exists():
        issues = json.loads(args.issues_json.read_text(encoding="utf-8"))

    recording = args.audit_dir / "recording.mp4"
    review = args.audit_dir / "review.md"

    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO dashboard_runs (
                run_id, occurred_at, player, market, outcome, duration_s, issues,
                top_finding, video_url, review_url
            ) VALUES (
                :run_id, :occurred_at, :player, :market, :outcome, :duration_s, :issues,
                :top_finding, :video_url, :review_url
            )
            ON CONFLICT (run_id) DO UPDATE SET
                outcome = EXCLUDED.outcome,
                duration_s = EXCLUDED.duration_s,
                issues = EXCLUDED.issues,
                top_finding = EXCLUDED.top_finding,
                video_url = EXCLUDED.video_url,
                review_url = EXCLUDED.review_url
            """
        ), {
            **meta,
            "outcome": db_outcome,
            "duration_s": args.duration_s,
            "issues": json.dumps(issues),
            "top_finding": args.top_finding,
            "video_url": _file_url(recording) if recording.exists() else None,
            "review_url": _file_url(review) if review.exists() else None,
        })

    heartbeat(
        "review-watcher",
        is_running=True,
        expected_interval_s=900,  # ~15 min between runs typically
        pending_count=args.pending_count,
        oldest_pending_age_s=args.oldest_pending_age_s,
        completed_24h=args.completed_24h,
        errors_24h=1 if outcome == "review_failed" else 0,
    )

    print(f"recorded {meta['run_id']} outcome={outcome}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
