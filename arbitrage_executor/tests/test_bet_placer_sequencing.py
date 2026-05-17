"""Regression tests for bet-placement sequencing guards."""

import pytest

from bet_placer import BetPlacer, BetPlacerError
from _fakes import FakeElement, FakeLocator, FakePage  # noqa: F401


AUDIT_DIR = "audit_logs/test_bet_placer_sequencing"


def test_fanduel_clear_all_still_runs_post_clear_verification():
    clear_all = FakeElement(visible=True)
    leftover_remove = FakeElement(visible=True)
    page = FakePage(
        locators={
            'div[role="button"]:has-text("Remove all selections")': FakeLocator([clear_all]),
            'button[aria-label*="remove" i]': FakeLocator([leftover_remove]),
        }
    )
    placer = BetPlacer(page, "fanduel", AUDIT_DIR)

    with pytest.raises(BetPlacerError, match="FanDuel slip-clear failed"):
        placer._clear_betslip_fanduel()

    assert clear_all.clicked


def test_betmgm_clear_all_still_runs_post_clear_verification():
    clear_all = FakeElement(visible=True)
    page = FakePage(
        locators={
            'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i': FakeLocator(
                [FakeElement(visible=True, text="1 Bet slip")]
            ),
            'span:has-text("Clear All")': FakeLocator([clear_all]),
        }
    )
    placer = BetPlacer(page, "betmgm", AUDIT_DIR)

    with pytest.raises(BetPlacerError, match="BetMGM slip-clear failed"):
        placer._clear_betslip_betmgm_precheck()

    assert clear_all.clicked


def test_fanduel_validation_does_not_accept_ambiguous_slip_state():
    page = FakePage()
    placer = BetPlacer(page, "fanduel", AUDIT_DIR)

    with pytest.raises(BetPlacerError, match="FanDuel slip is empty after bet click"):
        placer.assert_betslip_has_bet()


def test_fanduel_validation_accepts_current_remove_all_signal():
    page = FakePage(
        locators={
            'div[role="button"]:has-text("Remove all selections")': FakeLocator(
                [FakeElement(visible=True)]
            ),
        }
    )
    placer = BetPlacer(page, "fanduel", AUDIT_DIR)

    placer.assert_betslip_has_bet()


# ---- Unified-name wrapper tests (added in A3 of bet-placer-rewrite) ----

def test_get_actual_odds_dispatches_to_fanduel(monkeypatch):
    page = FakePage()
    placer = BetPlacer(page, "fanduel", AUDIT_DIR)
    called = []
    monkeypatch.setattr(placer, "get_actual_odds_fanduel",
                        lambda: called.append("fd") or 2.5)
    assert placer.get_actual_odds() == 2.5
    assert called == ["fd"]


def test_get_actual_odds_dispatches_to_betmgm(monkeypatch):
    page = FakePage()
    placer = BetPlacer(page, "betmgm", AUDIT_DIR)
    called = []
    monkeypatch.setattr(placer, "get_actual_odds_betmgm",
                        lambda: called.append("mgm") or 1.91)
    assert placer.get_actual_odds() == 1.91
    assert called == ["mgm"]


def test_discover_max_wager_raises_on_betmgm():
    page = FakePage()
    placer = BetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(NotImplementedError, match="max-wager discovery"):
        placer.discover_max_wager()


def test_check_limit_alert_raises_on_fanduel():
    page = FakePage()
    placer = BetPlacer(page, "fanduel", AUDIT_DIR)
    with pytest.raises(NotImplementedError, match="limit-alert check"):
        placer.check_limit_alert()
