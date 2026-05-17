"""Tests for FanduelBetPlacer migrated methods."""
import pytest

from _fakes import FakeElement, FakeLocator, FakePage
from bet_placer import BetPlacerError
from bet_placer_fanduel import FanduelBetPlacer, FANDUEL_THRESHOLD_ONE_LABELS

AUDIT_DIR = "audit_logs/test_bet_placer_fanduel"


def test_threshold_one_labels_has_22_entries():
    assert len(FANDUEL_THRESHOLD_ONE_LABELS) == 22


def test_threshold_one_labels_singular_and_plural_agree():
    assert FANDUEL_THRESHOLD_ONE_LABELS["Single"] == FANDUEL_THRESHOLD_ONE_LABELS["Singles"]
    assert FANDUEL_THRESHOLD_ONE_LABELS["RBI"] == FANDUEL_THRESHOLD_ONE_LABELS["RBIs"]
    assert FANDUEL_THRESHOLD_ONE_LABELS["Stolen Base"] == FANDUEL_THRESHOLD_ONE_LABELS["Stolen Bases"]


def test_rbi_uses_an_article():
    _, article, _ = FANDUEL_THRESHOLD_ONE_LABELS["RBI"]
    assert article == "An"


# ---- B2: navigation method tests ----

def test_dismiss_modal_is_noop_when_no_modal():
    page = FakePage()  # no modal locator
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer._dismiss_fanduel_modal()  # must not raise

    assert page.waits == []


def test_dismiss_modal_invisible_modal_is_noop():
    page = FakePage(locators={
        'div[role="dialog"][aria-modal="true"]': FakeLocator(
            [FakeElement(visible=False)]
        ),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer._dismiss_fanduel_modal()  # must not raise

    assert page.waits == []


# ---- B3: slip-clearing tests ----

def test_clear_slip_fast_path_when_already_empty(capsys):
    page = FakePage(text_locators={
        "Betslip empty": FakeLocator([FakeElement(visible=True)]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer._clear_betslip_fanduel()

    captured = capsys.readouterr()
    assert "Slip already empty." in captured.out


def test_clear_slip_via_remove_all_button():
    clear_all = FakeElement(visible=True)
    page = FakePage(locators={
        'div[role="button"]:has-text("Remove all selections")':
            FakeLocator([clear_all]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    placer._clear_betslip_fanduel()

    assert clear_all.clicked


def test_clear_slip_post_clear_verification_raises_when_remove_button_remains():
    clear_all = FakeElement(visible=True)
    leftover_remove = FakeElement(visible=True)
    page = FakePage(locators={
        'div[role="button"]:has-text("Remove all selections")':
            FakeLocator([clear_all]),
        'button[aria-label*="remove" i]': FakeLocator([leftover_remove]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    with pytest.raises(BetPlacerError, match="FanDuel slip-clear failed"):
        placer._clear_betslip_fanduel()

    assert clear_all.clicked
