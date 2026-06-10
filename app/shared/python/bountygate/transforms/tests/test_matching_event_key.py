from datetime import datetime, timezone

from bountygate.transforms.matching.event_key import (
    parse_kalshi_external_id, parse_ts, within_window,
)


def test_parse_kalshi_with_time():
    p = parse_kalshi_external_id("KXMLBGAME-26MAY031415LADSTL-LAD", "KXMLBGAME")
    assert p["sport"] == "MLB"
    assert p["team"] == "LAD"
    assert p["opponent"] == "STL"
    assert p["has_time"] is True
    assert p["dt"] == datetime(2026, 5, 3, 14, 15, tzinfo=timezone.utc)


def test_parse_kalshi_date_only():
    p = parse_kalshi_external_id("KXNBAGAME-26APR30NYKATL-NYK", "KXNBAGAME")
    assert p["sport"] == "NBA"
    assert {p["team"], p["opponent"]} == {"NYK", "ATL"}
    assert p["has_time"] is False
    assert p["dt"] == datetime(2026, 4, 30, tzinfo=timezone.utc)


def test_parse_kalshi_unknown_series_returns_none():
    assert parse_kalshi_external_id("KXNHLGAME-26MAY01XYZABC-XYZ", "KXNHLGAME") is None


def test_within_window_tight_when_time():
    g = datetime(2026, 5, 3, 14, 15, tzinfo=timezone.utc)
    assert within_window(g, True, datetime(2026, 5, 3, 18, 0, tzinfo=timezone.utc))      # ~3.75h
    assert not within_window(g, True, datetime(2026, 5, 4, 0, 0, tzinfo=timezone.utc))   # ~9.75h


def test_within_window_wide_when_date_only():
    g = datetime(2026, 4, 30, tzinfo=timezone.utc)
    assert within_window(g, False, datetime(2026, 4, 30, 23, 30, tzinfo=timezone.utc))   # 23.5h


def test_parse_ts_handles_z_and_datetime():
    assert parse_ts("2026-05-03T14:15:00Z") == datetime(2026, 5, 3, 14, 15, tzinfo=timezone.utc)
    assert parse_ts(datetime(2026, 5, 3, tzinfo=timezone.utc)).year == 2026
    assert parse_ts(None) is None
