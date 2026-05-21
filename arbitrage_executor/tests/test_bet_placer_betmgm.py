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


# ---- C5a: pick-format detection + alt-yes dispatch ----

class _FakeAccordionLocator:
    """Mimic the Locator returned by ``_accordion_root_locator``.

    Production: ``acc.locator('ms-event-pick')`` to find picks inside
    the specific market panel. Tests inject a fixed list of picks.
    """
    def __init__(self, picks):
        self._picks = picks

    def locator(self, selector):
        if selector == 'ms-event-pick':
            return FakeLocator(self._picks)
        return FakeLocator()


def _stub_accordion_root(placer, picks):
    """Monkey-patch ``_accordion_root_locator`` to return picks directly,
    sidestepping the live ``:has-text`` + ``xpath=ancestor::`` chain.
    Use ``picks=None`` to simulate "no accordion found"."""
    if picks is None:
        placer._accordion_root_locator = lambda name: None
    else:
        placer._accordion_root_locator = lambda name: _FakeAccordionLocator(picks)


def test_detect_pick_format_returns_std_for_over_under_picks():
    """NHL panel: picks read 'O 4.5 2.25' / 'U 4.5 1.57' → std path."""
    placer = BetmgmBetPlacer(FakePage(), "betmgm", AUDIT_DIR)
    _stub_accordion_root(placer, [
        FakeElement(visible=True, text="O 4.5 2.25"),
        FakeElement(visible=True, text="U 4.5 1.57"),
    ])
    assert placer._detect_pick_format("Player shots") == "std"


def test_detect_pick_format_returns_alt_for_yes_picks():
    """NBA panel: picks read 'Yes 1.07' → alt path."""
    placer = BetmgmBetPlacer(FakePage(), "betmgm", AUDIT_DIR)
    _stub_accordion_root(placer, [
        FakeElement(visible=True, text="Yes 1.07"),
        FakeElement(visible=True, text="Yes 1.279"),
    ])
    assert placer._detect_pick_format("Player points") == "alt"


def test_detect_pick_format_defaults_to_std_when_panel_empty():
    """Empty panel should NOT route to alt — alt path would silently
    misclick under-direction bets. Default to std so the std path's loud
    'No bet found' error surfaces."""
    placer = BetmgmBetPlacer(FakePage(), "betmgm", AUDIT_DIR)
    _stub_accordion_root(placer, [])
    assert placer._detect_pick_format("Player points") == "std"


def test_detect_pick_format_defaults_to_std_when_no_accordion_match():
    """If the toggle button can't be found at all (page not ready,
    accordion name typo'd in YAML), default to std so the std path's
    loud 'No bet found' diagnostic surfaces. The earlier scoped-string
    selector silently returned 0 picks AND defaulted to std on every
    BetMGM call — this guard ensures the dispatch is testable."""
    placer = BetmgmBetPlacer(FakePage(), "betmgm", AUDIT_DIR)
    _stub_accordion_root(placer, None)
    assert placer._detect_pick_format("Player points") == "std"


# ---- C5b: structural-skip predicate (alt-sibling-of-missing-std) ----

def test_alt_sibling_if_std_missing_returns_sibling_when_std_absent():
    """When the std "O/U" accordion isn't on the page but the merged-alt
    sibling IS, return the alt sibling name so the caller can raise
    BetPlacerSkipError. See LOGIC.md."""
    visible = ["Player blocks", "Player rebounds + assists",
               "Player double-double", "First field goal scorer"]
    sibling = BetmgmBetPlacer._alt_sibling_if_std_missing(
        "Player rebounds + assists O/U", visible
    )
    assert sibling == "Player rebounds + assists"


def test_alt_sibling_if_std_missing_returns_none_when_no_o_u_suffix():
    """Std-only accordions without "O/U" suffix (e.g. Player blocks) and
    alt accordions can't trigger the structural-skip path."""
    visible = ["Player blocks", "Player rebounds + assists"]
    assert BetmgmBetPlacer._alt_sibling_if_std_missing(
        "Player blocks", visible
    ) is None
    assert BetmgmBetPlacer._alt_sibling_if_std_missing(
        "Player rebounds + assists", visible
    ) is None


def test_alt_sibling_if_std_missing_returns_none_when_alt_also_absent():
    """If neither the std nor the alt sibling is on the page, that's a
    real YAML/UI drift — must NOT raise SkipError. Caller falls through
    to the loud BetPlacerError."""
    visible = ["Player blocks", "First field goal scorer"]
    assert BetmgmBetPlacer._alt_sibling_if_std_missing(
        "Player rebounds + assists O/U", visible
    ) is None


def test_alt_sibling_if_std_missing_tolerates_whitespace_and_case():
    """BetMGM's accordion labels occasionally ship with case wobble or
    extra whitespace. Normalize both sides before comparing."""
    visible = ["  player REBOUNDS  +  assists  "]
    sibling = BetmgmBetPlacer._alt_sibling_if_std_missing(
        "Player rebounds + assists O/U", visible
    )
    assert sibling == "Player rebounds + assists"


def test_find_and_click_raises_loud_on_alt_under_direction():
    """BetMGM alt-only accordions only ship a Yes pick per row — there
    is no symmetric No pick, so direction='under' on a confirmed-alt
    panel cannot be expressed. Fail loud instead of silently misclicking."""
    placer = BetmgmBetPlacer(FakePage(), "betmgm", AUDIT_DIR)
    _stub_accordion_root(placer, [FakeElement(visible=True, text="Yes 1.07")])
    opp = {"player_name": "Shai Gilgeous-Alexander",
           "over_line": 19.5, "under_line": 19.5}
    market_config = {
        "accordion_name": "Player points",
        "has_threshold_tabs": True,
    }
    with pytest.raises(BetPlacerError,
                       match="alt-only accordion can't take direction='under'"):
        placer.find_and_click_bet(opp, "under", market_config)


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


# ---- C8: get_actual_odds tests ----

def test_get_actual_odds_parses_decimal():
    odds_elem = FakeElement(visible=True, text="1.75")
    page = FakePage(locators={
        'span.odds-indicator__lite--default': FakeLocator([odds_elem]),
    })
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer.get_actual_odds() == 1.75


def test_get_actual_odds_returns_none_when_not_found():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer.get_actual_odds() is None
