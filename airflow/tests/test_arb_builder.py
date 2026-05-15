from datetime import datetime, timezone

import pandas as pd
import pytest

from bg_arb_pipeline_lib.builder import build_opportunities


def _stage_row(**overrides):
    base = {
        "event_id": "evt_test",
        "sport_title": "NBA",
        "home_team": "Cleveland Cavaliers",
        "away_team": "Detroit Pistons",
        "commence_time_utc": datetime(2026, 5, 15, 23, 0, tzinfo=timezone.utc),
        "player_name": "Dennis Schroder",
        "bookmaker_key": "fanduel",
        "market_key": "player_assists",
        "line": 2.5,
        "side": "under",
        "price": 2.00,
        "fetched_at_utc": datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _lines_df(rows):
    return pd.DataFrame(rows)


class TestBuildOpportunities:
    def test_std_under_std_over_emits_std_std_row(self):
        # FD under 2.10 × MGM over 2.20 → implied = 0.476 + 0.455 = 0.931 < 1.0 = arb
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        row = opps.iloc[0]
        assert row["pairing_type"] == "std_std"
        assert row["under_book"] == "fanduel"
        assert row["over_book"] == "betmgm"
        assert row["under_market_key"] == "player_assists"
        assert row["over_market_key"] == "player_assists"
        assert row["canonical_market"] == "player_assists"
        assert row["roi"] > 0

    def test_std_under_alt_over_emits_std_alt_row(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="betmgm",  market_key="player_assists",           side="under", price=2.10),
            _stage_row(bookmaker_key="fanduel", market_key="player_assists_alternate", side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        assert opps.iloc[0]["pairing_type"] == "std_alt"
        assert opps.iloc[0]["canonical_market"] == "player_assists"

    def test_alt_under_std_over_emits_alt_std_row(self):
        # The case the old code never produced.
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", market_key="player_assists_alternate", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  market_key="player_assists",           side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        assert opps.iloc[0]["pairing_type"] == "alt_std"

    def test_alt_under_alt_over_emits_alt_alt_row(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", market_key="player_assists_alternate", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  market_key="player_assists_alternate", side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        assert opps.iloc[0]["pairing_type"] == "alt_alt"

    def test_intra_book_pair_emits_nothing(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10),
            _stage_row(bookmaker_key="fanduel", side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_line_mismatch_emits_nothing(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10, line=2.5),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20, line=3.5),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_negative_roi_pair_emits_nothing(self):
        # Overround >= 1.0 means no arb.
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=1.85),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=1.85),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_economics_columns_are_populated(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        row = opps.iloc[0]
        assert row["wager_under"] > 0
        assert row["wager_over"] > 0
        assert row["payout"] > 100.0
        assert row["arb_ev"] > 0
        assert row["roi"] > 0
        # Wager-under × under-price ≈ payout (arb invariant)
        assert abs(row["wager_under"] * 2.10 - row["payout"]) < 0.01
        assert abs(row["wager_over"] * 2.20 - row["payout"]) < 0.01

    def test_hours_until_commence_computed_from_commence_time(self):
        commence = datetime(2026, 5, 15, 23, 0, tzinfo=timezone.utc)
        fetched  = datetime(2026, 5, 15, 19, 0, tzinfo=timezone.utc)
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10,
                       commence_time_utc=commence, fetched_at_utc=fetched),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20,
                       commence_time_utc=commence, fetched_at_utc=fetched),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        assert opps.iloc[0]["hours_until_commence"] == pytest.approx(4.0, abs=0.01)

    def test_empty_input_returns_empty_dataframe(self):
        lines = _lines_df([])
        opps = build_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_opportunity_hash_is_unique_per_row(self):
        lines = _lines_df([
            _stage_row(bookmaker_key="fanduel", side="under", price=2.10),
            _stage_row(bookmaker_key="betmgm",  side="over",  price=2.20),
            _stage_row(bookmaker_key="betmgm",  market_key="player_assists_alternate", side="over", price=2.30),
        ])
        opps = build_opportunities(lines, base_wager=100.0)
        # 1 under × 2 overs (different markets) = 2 opportunities, distinct hashes.
        assert len(opps) == 2
        assert opps["opportunity_hash"].nunique() == 2
