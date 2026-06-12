"""Integration test for bountygate.arb.build.run_build_arbs (sqlite in-memory).

Seeds the minimal source tables the orchestrator queries, runs detection against
an injected sqlite engine, and asserts:
  * a book x book h2h arb is detected at the expected ROI;
  * the Kalshi Yes/No -> team resolution feeds the book x venue game path;
  * a second identical run inserts 0 and refreshes (bumps last_seen_at).

Sqlite stores timestamps as ISO strings; build.py's ``_as_dt`` handles the
str/datetime duality, which this test exercises end to end.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from bountygate.arb import build

UTC = timezone.utc

# Sqlite-typed DDL covering exactly the columns the orchestrator's queries touch.
_DDL = [
    """CREATE TABLE sports_events (
        event_id text PRIMARY KEY,
        source_event_id text,
        sport_key text,
        commence_time text,
        home_team text,
        away_team text
    )""",
    """CREATE TABLE sportsbook_odds_history (
        event_id text,
        market_type text,
        bookmaker text,
        outcome_name text,
        point real,
        decimal_price real,
        captured_at text
    )""",
    """CREATE TABLE player_props_odds_history (
        event_id text,
        market_key text,
        player_name text,
        line real,
        side text,
        bookmaker text,
        decimal_price real,
        captured_at text
    )""",
    """CREATE TABLE markets (
        market_id text PRIMARY KEY,
        venue_key text,
        external_id text,
        title text,
        category text,
        status text,
        resolved_outcome text
    )""",
    """CREATE TABLE market_outcomes (
        outcome_id text PRIMARY KEY,
        market_id text,
        outcome_name text
    )""",
    """CREATE TABLE price_history (
        outcome_id text,
        market_id text,
        captured_at text,
        ask real
    )""",
    """CREATE TABLE market_event_links (
        market_id text,
        event_id text
    )""",
    """CREATE TABLE arb_opportunities (
        opportunity_hash text PRIMARY KEY,
        first_detected_at text DEFAULT (datetime('now')),
        last_seen_at text DEFAULT (datetime('now')),
        kind text, pairing text, event_id text, sport_key text,
        home_team text, away_team text, commence_time text, market_segment text,
        player_name text, line real, pairing_type text,
        leg_a_kind text, leg_a_source text, leg_a_outcome text,
        leg_a_point real, leg_a_price real, leg_a_stake real,
        leg_b_kind text, leg_b_source text, leg_b_outcome text,
        leg_b_point real, leg_b_price real, leg_b_stake real,
        payout real, arb_ev real, roi real, fee_adjusted_roi real,
        hours_until_commence real, details text
    )""",
]


def _iso(dt: datetime) -> str:
    return dt.isoformat()


@pytest.fixture()
def seeded_engine(monkeypatch):
    # Freshness windows generous; the seed captured_at is 5 min ago.
    monkeypatch.setattr(build, "_GAME_FRESH_MIN", 120.0)
    monkeypatch.setattr(build, "_PROPS_FRESH_MIN", 120.0)
    monkeypatch.setattr(build, "_VENUE_FRESH_MIN", 120.0)

    engine = create_engine("sqlite://")  # in-memory, single connection
    now = datetime.now(UTC)
    captured = now - timedelta(minutes=5)
    commence = now + timedelta(hours=6)

    with engine.begin() as conn:
        for ddl in _DDL:
            conn.execute(text(ddl))

        # MLB game: Phillies @ Brewers (home=MIL, away=PHI).
        conn.execute(text(
            "INSERT INTO sports_events VALUES "
            "(:eid, :src, :sport, :commence, :home, :away)"),
            {"eid": "evt1", "src": "src1", "sport": "baseball_mlb",
             "commence": _iso(commence),
             "home": "Milwaukee Brewers", "away": "Philadelphia Phillies"})

        # Book x book h2h: two books, 2.10/2.10 crossing on the two teams.
        sb = (
            ("evt1", "h2h", "draftkings", "Milwaukee Brewers", None, 2.10, _iso(captured)),
            ("evt1", "h2h", "fanduel", "Philadelphia Phillies", None, 2.10, _iso(captured)),
        )
        for r in sb:
            conn.execute(text(
                "INSERT INTO sportsbook_odds_history VALUES "
                "(:e, :mt, :bk, :on, :pt, :dp, :ca)"),
                dict(zip(("e", "mt", "bk", "on", "pt", "dp", "ca"), r)))

        # Kalshi GAME market: ticker resolves Yes->MIL, No->PHI.
        conn.execute(text(
            "INSERT INTO markets VALUES (:mid, :vk, :ext, :title, :cat, :st, :ro)"),
            {"mid": "mkt1", "vk": "kalshi",
             "ext": "KXMLBGAME-26JUN13MILPHI-MIL",
             "title": "Will the Brewers beat the Phillies?",
             "cat": "KXMLBGAME", "st": "active", "ro": None})
        conn.execute(text("INSERT INTO market_event_links VALUES ('mkt1', 'evt1')"))

        # Outcomes Yes/No with asks 0.40 each (book 2.10 vs ask 0.40 clears +fee).
        outcomes = (("oc_yes", "Yes", 0.40), ("oc_no", "No", 0.40))
        for oid, name, ask in outcomes:
            conn.execute(text(
                "INSERT INTO market_outcomes VALUES (:oid, 'mkt1', :name)"),
                {"oid": oid, "name": name})
            conn.execute(text(
                "INSERT INTO price_history VALUES (:oid, 'mkt1', :ca, :ask)"),
                {"oid": oid, "ca": _iso(captured), "ask": ask})

    return engine


def _captured_notify(monkeypatch):
    calls = []
    monkeypatch.setattr(build, "notify",
                        lambda msg, **kw: calls.append((msg, kw)))
    return calls


def test_book_venue_roi_sanity():
    # Guard: the seeded book 2.10 vs ask 0.40 clears positive after fee.
    from bountygate.arb.fees import book_venue_roi
    roi_pre, roi_fee = book_venue_roi(2.10, 0.40, "kalshi")
    assert roi_fee > 0


def test_detects_and_upserts(seeded_engine, monkeypatch):
    calls = _captured_notify(monkeypatch)
    res = build.run_build_arbs(engine=seeded_engine)

    # Book x book h2h arb at roi ~0.05 (2.10/2.10).
    assert res["game_bb"] >= 1
    with seeded_engine.begin() as conn:
        bb = conn.execute(text(
            "SELECT roi FROM arb_opportunities WHERE pairing='book_book'"
        )).scalars().all()
    assert any(r == pytest.approx(0.05, abs=1e-4) for r in bb)

    # Book x venue game path: Kalshi Yes/No resolved to MIL/PHI; both
    # directions should pair (book MIL x venue No(PHI), book PHI x venue Yes(MIL)).
    assert res["game_bv"] >= 1
    assert res["venue_stats"]["paired"] >= 1

    # Everything inserted as NEW on the first run; a Discord alert fired.
    assert res["inserted"] == res["candidates"]
    assert res["inserted"] >= 2
    assert res["refreshed"] == 0
    assert res["alerts"] >= 1
    assert len(calls) == 1
    assert calls[0][1].get("level") == "warning"


def test_second_run_refreshes(seeded_engine, monkeypatch):
    _captured_notify(monkeypatch)
    first = build.run_build_arbs(engine=seeded_engine)

    with seeded_engine.begin() as conn:
        before = conn.execute(text(
            "SELECT opportunity_hash, last_seen_at FROM arb_opportunities"
        )).mappings().all()
    seen_before = {r["opportunity_hash"]: r["last_seen_at"] for r in before}

    # Re-run with identical data: nothing new, all refreshed.
    second = build.run_build_arbs(engine=seeded_engine)
    assert second["inserted"] == 0
    assert second["refreshed"] == first["candidates"]
    assert second["alerts"] == 0  # alerts only on NEW opps

    with seeded_engine.begin() as conn:
        after = conn.execute(text(
            "SELECT opportunity_hash, last_seen_at FROM arb_opportunities"
        )).mappings().all()
    # Same set of hashes, and last_seen_at advanced (or held) for each.
    assert {r["opportunity_hash"] for r in after} == set(seen_before)
    for r in after:
        assert r["last_seen_at"] >= seen_before[r["opportunity_hash"]]


def test_new_hash_detection_with_preexisting_row(seeded_engine, monkeypatch):
    # Pre-insert ONE of the opportunity rows; that hash must count as refreshed,
    # not inserted, on the next detection run.
    _captured_notify(monkeypatch)
    first = build.run_build_arbs(engine=seeded_engine)
    assert first["inserted"] >= 2

    # Wipe and pre-seed a single known hash, then re-run.
    with seeded_engine.begin() as conn:
        hashes = conn.execute(text(
            "SELECT opportunity_hash FROM arb_opportunities"
        )).scalars().all()
        keep = hashes[0]
        conn.execute(text(
            "DELETE FROM arb_opportunities WHERE opportunity_hash != :h"),
            {"h": keep})

    res = build.run_build_arbs(engine=seeded_engine)
    # keep already existed -> refreshed; the rest are re-inserted as new.
    assert res["refreshed"] >= 1
    assert res["inserted"] == res["candidates"] - res["refreshed"]
    assert res["inserted"] >= 1
