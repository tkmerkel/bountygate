from datetime import datetime, timedelta, timezone

from app.web.watcher_status import compute_status


def _hb(**overrides):
    base = {
        "name": "x",
        "is_running": True,
        "last_tick_at": datetime.now(timezone.utc),
        "pending_count": 0,
        "oldest_pending_age_s": None,
        "completed_24h": 0,
        "errors_24h": 0,
        "last_error": None,
        "expected_interval_s": 60,
    }
    base.update(overrides)
    return base


def test_status_ok_when_fresh_and_no_errors():
    assert compute_status(_hb()) == "ok"


def test_status_amber_when_backlog_older_than_15min():
    assert compute_status(_hb(pending_count=2, oldest_pending_age_s=16 * 60)) == "amber"


def test_status_amber_when_tick_older_than_2x_interval():
    stale = datetime.now(timezone.utc) - timedelta(seconds=130)
    assert compute_status(_hb(expected_interval_s=60, last_tick_at=stale)) == "amber"


def test_status_red_when_errors_in_24h():
    assert compute_status(_hb(errors_24h=1)) == "red"


def test_status_red_when_tick_older_than_6x_interval():
    very_stale = datetime.now(timezone.utc) - timedelta(seconds=400)
    assert compute_status(_hb(expected_interval_s=60, last_tick_at=very_stale)) == "red"


def test_status_red_beats_amber():
    stale = datetime.now(timezone.utc) - timedelta(seconds=200)
    assert compute_status(_hb(errors_24h=1, last_tick_at=stale)) == "red"
