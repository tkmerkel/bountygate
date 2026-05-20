from datetime import datetime, timezone

import pandas as pd
import pytest

from bg_game_arb_pipeline_lib.builder import build_game_opportunities
from bg_game_arb_pipeline_lib.hashing import opportunity_hash


COMMENCE = datetime(2026, 5, 19, 23, 0, tzinfo=timezone.utc)
FETCHED = datetime(2026, 5, 19, 19, 0, tzinfo=timezone.utc)


def _stage_row(**overrides):
    base = {
        "event_id": "evt_test",
        "sport_key": "basketball_nba",
        "sport_title": "NBA",
        "home_team": "Cleveland Cavaliers",
        "away_team": "Detroit Pistons",
        "commence_time_utc": COMMENCE,
        "bookmaker_key": "fanduel",
        "market_key": "h2h",
        "outcome": "Cleveland Cavaliers",
        "point": None,
        "price": 2.10,
        "fetched_at_utc": FETCHED,
    }
    base.update(overrides)
    return base


def _df(rows):
    return pd.DataFrame(rows)


class TestH2H:
    def test_h2h_two_books_arb_emits_one_row(self):
        # 1/2.10 + 1/2.20 = 0.476 + 0.455 = 0.931 < 1 → arb
        lines = _df([
            _stage_row(bookmaker_key="fanduel", outcome="Cleveland Cavaliers", price=2.10),
            _stage_row(bookmaker_key="betmgm",  outcome="Detroit Pistons",     price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        row = opps.iloc[0]
        assert row["market_key"] == "h2h"
        # Canonical leg ordering is alphabetical by book.
        assert row["leg_a_book"] == "betmgm"
        assert row["leg_b_book"] == "fanduel"
        assert row["roi"] > 0

    def test_h2h_same_book_emits_nothing(self):
        lines = _df([
            _stage_row(bookmaker_key="fanduel", outcome="Cleveland Cavaliers", price=2.10),
            _stage_row(bookmaker_key="fanduel", outcome="Detroit Pistons",     price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_h2h_no_arb_when_overround_above_one(self):
        # Both sides priced at 1.90 → overround = 1.053 > 1 → no arb.
        lines = _df([
            _stage_row(bookmaker_key="fanduel", outcome="Cleveland Cavaliers", price=1.90),
            _stage_row(bookmaker_key="betmgm",  outcome="Detroit Pistons",     price=1.90),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert opps.empty


class TestSpreads:
    def test_spreads_with_mirrored_points_arb_emits_one_row(self):
        # Cavs -7.5 @ 2.10 (FD)  ×  Pistons +7.5 @ 2.20 (MGM)
        lines = _df([
            _stage_row(market_key="spreads", bookmaker_key="fanduel",
                       outcome="Cleveland Cavaliers", point=-7.5, price=2.10),
            _stage_row(market_key="spreads", bookmaker_key="betmgm",
                       outcome="Detroit Pistons", point=7.5, price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        row = opps.iloc[0]
        assert row["market_key"] == "spreads"
        # leg_a_point and leg_b_point are opposite signs.
        assert row["leg_a_point"] == -row["leg_b_point"]
        assert row["roi"] > 0

    def test_spreads_with_different_points_emits_nothing(self):
        # Cavs -7.5 vs. Pistons +6.5 — line mismatch, no arb.
        lines = _df([
            _stage_row(market_key="spreads", bookmaker_key="fanduel",
                       outcome="Cleveland Cavaliers", point=-7.5, price=2.10),
            _stage_row(market_key="spreads", bookmaker_key="betmgm",
                       outcome="Detroit Pistons", point=6.5, price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert opps.empty


class TestTotals:
    def test_totals_with_matching_threshold_arb_emits_one_row(self):
        # Over 224.5 @ 2.10 (FD)  ×  Under 224.5 @ 2.20 (MGM)
        lines = _df([
            _stage_row(market_key="totals", bookmaker_key="fanduel",
                       outcome="Over",  point=224.5, price=2.10),
            _stage_row(market_key="totals", bookmaker_key="betmgm",
                       outcome="Under", point=224.5, price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        row = opps.iloc[0]
        assert row["market_key"] == "totals"
        assert row["leg_a_point"] == row["leg_b_point"] == 224.5
        # One leg Over, the other Under.
        outcomes = {row["leg_a_outcome"].lower(), row["leg_b_outcome"].lower()}
        assert outcomes == {"over", "under"}
        assert row["roi"] > 0

    def test_totals_with_different_threshold_emits_nothing(self):
        lines = _df([
            _stage_row(market_key="totals", bookmaker_key="fanduel",
                       outcome="Over",  point=224.5, price=2.10),
            _stage_row(market_key="totals", bookmaker_key="betmgm",
                       outcome="Under", point=225.5, price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_totals_intra_book_emits_nothing(self):
        lines = _df([
            _stage_row(market_key="totals", bookmaker_key="fanduel",
                       outcome="Over",  point=224.5, price=2.10),
            _stage_row(market_key="totals", bookmaker_key="fanduel",
                       outcome="Under", point=224.5, price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert opps.empty


class TestEconomics:
    def test_arb_invariants_hold(self):
        lines = _df([
            _stage_row(bookmaker_key="fanduel", outcome="Cleveland Cavaliers", price=2.10),
            _stage_row(bookmaker_key="betmgm",  outcome="Detroit Pistons",     price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert len(opps) == 1
        row = opps.iloc[0]
        # Both legs payouts equal the unified payout.
        assert row["wager_leg_a"] * row["leg_a_price"] == pytest.approx(row["payout"], abs=0.0001)
        assert row["wager_leg_b"] * row["leg_b_price"] == pytest.approx(row["payout"], abs=0.0001)
        # Total stake = base wager.
        assert row["wager_leg_a"] + row["wager_leg_b"] == pytest.approx(100.0, abs=0.0001)
        # ROI matches (payout - base) / base.
        assert row["roi"] == pytest.approx((row["payout"] - 100.0) / 100.0, abs=1e-9)

    def test_hours_until_commence_computed_correctly(self):
        commence = datetime(2026, 5, 19, 23, 0, tzinfo=timezone.utc)
        fetched = datetime(2026, 5, 19, 19, 0, tzinfo=timezone.utc)
        lines = _df([
            _stage_row(bookmaker_key="fanduel", outcome="Cleveland Cavaliers",
                       price=2.10, commence_time_utc=commence, fetched_at_utc=fetched),
            _stage_row(bookmaker_key="betmgm", outcome="Detroit Pistons",
                       price=2.20, commence_time_utc=commence, fetched_at_utc=fetched),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert opps.iloc[0]["hours_until_commence"] == pytest.approx(4.0, abs=0.01)


class TestHashing:
    def test_opportunity_hash_invariant_under_leg_swap(self):
        """The same arb shouldn't produce two different hashes when leg roles are swapped."""
        row1 = {
            "event_id": "evt1",
            "market_key": "h2h",
            "leg_a_book": "fanduel", "leg_a_outcome": "Lakers", "leg_a_point": None, "leg_a_price": 2.10,
            "leg_b_book": "betmgm",  "leg_b_outcome": "Celtics", "leg_b_point": None, "leg_b_price": 2.20,
        }
        row2 = {
            "event_id": "evt1",
            "market_key": "h2h",
            "leg_a_book": "betmgm",  "leg_a_outcome": "Celtics", "leg_a_point": None, "leg_a_price": 2.20,
            "leg_b_book": "fanduel", "leg_b_outcome": "Lakers", "leg_b_point": None, "leg_b_price": 2.10,
        }
        assert opportunity_hash(row1) == opportunity_hash(row2)

    def test_opportunity_hash_changes_on_price_change(self):
        row1 = {
            "event_id": "evt1", "market_key": "h2h",
            "leg_a_book": "fanduel", "leg_a_outcome": "Lakers", "leg_a_point": None, "leg_a_price": 2.10,
            "leg_b_book": "betmgm",  "leg_b_outcome": "Celtics", "leg_b_point": None, "leg_b_price": 2.20,
        }
        row2 = dict(row1)
        row2["leg_a_price"] = 2.11
        assert opportunity_hash(row1) != opportunity_hash(row2)


class TestEdgeCases:
    def test_empty_input_returns_empty_dataframe(self):
        opps = build_game_opportunities(_df([]), base_wager=100.0)
        assert opps.empty

    def test_multiple_markets_in_one_event(self):
        # Same event, h2h arb AND totals arb should both surface.
        lines = _df([
            # h2h
            _stage_row(market_key="h2h", bookmaker_key="fanduel",
                       outcome="Cleveland Cavaliers", price=2.10),
            _stage_row(market_key="h2h", bookmaker_key="betmgm",
                       outcome="Detroit Pistons",     price=2.20),
            # totals
            _stage_row(market_key="totals", bookmaker_key="fanduel",
                       outcome="Over",  point=224.5, price=2.10),
            _stage_row(market_key="totals", bookmaker_key="betmgm",
                       outcome="Under", point=224.5, price=2.20),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert len(opps) == 2
        markets = set(opps["market_key"].tolist())
        assert markets == {"h2h", "totals"}

    def test_event_already_started_emits_nothing(self):
        # commence < fetched → hours_until_commence < 0 → filtered out.
        past_commence = datetime(2026, 5, 19, 18, 0, tzinfo=timezone.utc)
        future_fetched = datetime(2026, 5, 19, 19, 0, tzinfo=timezone.utc)
        lines = _df([
            _stage_row(bookmaker_key="fanduel", outcome="Cleveland Cavaliers", price=2.10,
                       commence_time_utc=past_commence, fetched_at_utc=future_fetched),
            _stage_row(bookmaker_key="betmgm", outcome="Detroit Pistons", price=2.20,
                       commence_time_utc=past_commence, fetched_at_utc=future_fetched),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        assert opps.empty

    def test_three_book_h2h_yields_multiple_pairings(self):
        # FD over-priced, MGM/DK both undervalue the opposite side.
        lines = _df([
            _stage_row(bookmaker_key="fanduel",    outcome="Cleveland Cavaliers", price=2.10),
            _stage_row(bookmaker_key="betmgm",     outcome="Detroit Pistons",     price=2.20),
            _stage_row(bookmaker_key="draftkings", outcome="Detroit Pistons",     price=2.30),
        ])
        opps = build_game_opportunities(lines, base_wager=100.0)
        # FD-Cavs × MGM-Pistons AND FD-Cavs × DK-Pistons = 2 arbs (both unique hashes).
        assert len(opps) == 2
        assert opps["opportunity_hash"].nunique() == 2
