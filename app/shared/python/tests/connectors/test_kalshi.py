import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from bountygate.connectors.kalshi import SERIES_BY_SPORT, KalshiConnector

FIX = Path(__file__).parent.parent / "fixtures" / "connectors" / "kalshi_events.json"


def test_normalize_emits_market_rawrecords():
    raw = json.loads(FIX.read_text())
    ts = datetime(2026, 6, 5, tzinfo=timezone.utc)

    records = KalshiConnector.normalize(raw, series_ticker="KXNFLGAME", captured_at=ts)

    assert len(records) == 1
    r = records[0]
    assert r.source == "kalshi"
    assert r.source_key == "KXNFLGAME-26SEP07DALNYG-DAL"
    assert r.record_type == "market"
    assert r.captured_at == ts
    assert r.payload["yes_bid"] == 0.52
    assert r.payload["no_ask"] == 0.48
    assert r.payload["series_ticker"] == "KXNFLGAME"
    assert r.payload["open_interest"] == 1234.0


# --- _series_to_fetch / BG_KALSHI_PROP_SERIES tests ---

def test_series_to_fetch_default_when_env_unset(monkeypatch):
    """When BG_KALSHI_PROP_SERIES is unset, _series_to_fetch returns only the default series."""
    monkeypatch.delenv("BG_KALSHI_PROP_SERIES", raising=False)
    connector = KalshiConnector()
    result = connector._series_to_fetch()
    assert result == list(SERIES_BY_SPORT.values())


def test_series_to_fetch_union_new_ticker(monkeypatch):
    """A new prop ticker is appended to the list."""
    monkeypatch.setenv("BG_KALSHI_PROP_SERIES", "KXNBAPTS")
    connector = KalshiConnector()
    result = connector._series_to_fetch()
    assert list(SERIES_BY_SPORT.values()) == result[: len(SERIES_BY_SPORT)]
    assert "KXNBAPTS" in result
    assert len(result) == len(SERIES_BY_SPORT) + 1


def test_series_to_fetch_deduplicates_existing_ticker(monkeypatch):
    """A ticker already in series_by_sport is not duplicated."""
    monkeypatch.setenv("BG_KALSHI_PROP_SERIES", "KXNBAGAME")  # already in default
    connector = KalshiConnector()
    result = connector._series_to_fetch()
    assert result.count("KXNBAGAME") == 1
    assert len(result) == len(SERIES_BY_SPORT)


def test_series_to_fetch_union_with_duplicate_and_new(monkeypatch):
    """Mix of a duplicate and a new ticker: only the new one is added once."""
    monkeypatch.setenv("BG_KALSHI_PROP_SERIES", "KXNBAGAME,KXNBAPTS")
    connector = KalshiConnector()
    result = connector._series_to_fetch()
    assert result.count("KXNBAGAME") == 1
    assert "KXNBAPTS" in result
    assert len(result) == len(SERIES_BY_SPORT) + 1


def test_series_to_fetch_whitespace_tolerated(monkeypatch):
    """Whitespace around tickers in BG_KALSHI_PROP_SERIES is stripped."""
    monkeypatch.setenv("BG_KALSHI_PROP_SERIES", "  KXNHLPTS , KXNHLAST  ")
    connector = KalshiConnector()
    result = connector._series_to_fetch()
    assert "KXNHLPTS" in result
    assert "KXNHLAST" in result
    # No blank entries
    assert all(t.strip() == t and t for t in result)
