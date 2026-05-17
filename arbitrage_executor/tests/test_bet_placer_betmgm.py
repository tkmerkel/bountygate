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


# ---- C3: slip-clearing + slip-opening tests ----

def test_clear_slip_fast_path_when_pill_shows_zero(capsys):
    page = FakePage(locators={
        'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i':
            FakeLocator([FakeElement(visible=True, text="Bet slip (0)")]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer._clear_betslip_betmgm_precheck()

    captured = capsys.readouterr()
    assert "Slip already empty" in captured.out


def test_clear_slip_post_clear_verification_raises_when_pill_still_nonzero():
    clear_all = FakeElement(visible=True)
    page = FakePage(locators={
        'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i':
            FakeLocator([FakeElement(visible=True, text="1 Bet slip")]),
        'span:has-text("Clear All")': FakeLocator([clear_all]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    with pytest.raises(BetPlacerError, match="BetMGM slip-clear failed"):
        placer._clear_betslip_betmgm_precheck()

    assert clear_all.clicked


def test_open_slip_noop_when_stake_input_already_visible():
    stake_input = FakeElement(visible=True)
    page = FakePage(locators={
        'app-stake-input input': FakeLocator([stake_input]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer._open_betmgm_slip()

    # Should have returned early without clicking the pill
    assert page.waits == []


# ---- C4: slip-inspection tests ----

def test_slip_has_bet_false_when_empty_marker_visible():
    page = FakePage(text_locators={
        "No bet selections": FakeLocator([FakeElement(visible=True)]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer._betmgm_slip_has_bet() is False


def test_slip_has_bet_true_when_pill_shows_count():
    page = FakePage(locators={
        'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i':
            FakeLocator([FakeElement(visible=True, text="Bet slip (2)")]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer._betmgm_slip_has_bet() is True


def test_assert_betslip_has_bet_raises_when_empty():
    page = FakePage(text_locators={
        "No bet selections": FakeLocator([FakeElement(visible=True)]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="BetMGM slip is empty"):
        placer.assert_betslip_has_bet()


# ---- C5: find_and_click_bet tests ----

def test_find_and_click_raises_when_no_pick_matches():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    opp = {"player_name": "LeBron James", "over_line": 25.5, "under_line": 25.5}
    with pytest.raises(BetPlacerError,
                       match="No bet found for LeBron James over 25.5"):
        placer.find_and_click_bet(opp, "over", {})


# ---- C6: wager entry tests ----

def test_enter_wager_raises_when_input_not_found():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Could not find BetMGM wager input"):
        placer._enter_wager_betmgm(10.00)


def test_enter_wager_picks_last_empty_when_multiple_inputs():
    """Legacy invariant: when slip-clear has failed and multiple bets
    accumulate, _enter_wager_betmgm must pick the LAST empty stake input
    (the just-added bet), not the first or last filled."""
    filled_a = FakeElement(visible=True, input_value="5.00")
    empty_b = FakeElement(visible=True, input_value="")
    empty_c = FakeElement(visible=True, input_value="")  # LAST empty — should be picked
    filled_d = FakeElement(visible=True, input_value="3.00")
    page = FakePage(locators={
        'app-stake-input input': FakeLocator([filled_a, empty_b, empty_c, filled_d]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    placer._enter_wager_betmgm(10.00)

    # The last empty (empty_c) should have been clicked
    assert empty_c.clicked
    assert not empty_b.clicked
    assert not filled_a.clicked
    assert not filled_d.clicked


# ---- C7: place_bet + check_limit_alert tests ----

def test_place_bet_raises_when_button_not_found():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Place Bet button not found"):
        placer._place_bet_betmgm()


def test_check_limit_alert_returns_false_false_when_no_alert():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    limit_hit, adjusted = placer.check_limit_alert()
    assert limit_hit is False
    assert adjusted is None


def test_check_limit_alert_parses_adjusted_stake():
    alert = FakeElement(visible=True,
                        text="Your requested bet is over the allowed limit. ...")
    stake = FakeElement(visible=True, text="$6.76")
    page = FakePage(locators={
        'p.alert-content__message': FakeLocator([alert]),
        'span.betslip-summary-value': FakeLocator([stake]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)

    limit_hit, adjusted = placer.check_limit_alert()

    assert limit_hit is True
    assert adjusted == 6.76
