"""Tests for BetmgmBetPlacer migrated methods."""
import pytest

from _fakes import FakeElement, FakeLocator, FakePage
from bet_placer import BetPlacerError
from bet_placer_betmgm import BetmgmBetPlacer

AUDIT_DIR = "audit_logs/test_bet_placer_betmgm"


def test_skeleton_imports_cleanly():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer.site == "betmgm"


# ---- C2: navigation tests ----

def test_select_market_sub_tab_noop_when_no_label():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    placer._select_market_sub_tab_betmgm({})  # no sub_tab_label
    assert page.waits == []


def test_select_market_sub_tab_clicks_first_matching_selector():
    sub_tab = FakeElement(visible=True)
    page = FakePage(locators={
        'div[role="tablist"] button:has-text("Combo stats")':
            FakeLocator([sub_tab]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer._select_market_sub_tab_betmgm({"sub_tab_label": "Combo stats"})

    assert sub_tab.clicked


def test_select_market_sub_tab_raises_when_not_found():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Could not find BetMGM sub-tab"):
        placer._select_market_sub_tab_betmgm({"sub_tab_label": "Missing"})
