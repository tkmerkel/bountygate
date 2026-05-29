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


# The Remove-all-selections click flow and post-clear verification raise
# are both covered by the humanized tests
# (``test_lazy_clear_runs_full_dance_when_slip_has_bet`` in
# tests/test_bet_placer_fanduel_humanized.py) against the new public
# ``clear_betslip`` surface — the legacy click went through
# ``locator.click()`` which the rewrite replaced with ``mouse_click``,
# so the old ``.clicked`` signal no longer fires.


# ---- B4: slip-inspection tests ----

def test_slip_has_bet_returns_false_when_betslip_empty_marker_visible():
    page = FakePage(text_locators={
        "Betslip empty": FakeLocator([FakeElement(visible=True)]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    assert placer._fanduel_slip_has_bet() is False


def test_slip_has_bet_returns_true_when_no_empty_marker():
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    assert placer._fanduel_slip_has_bet() is True


def test_assert_betslip_has_bet_raises_on_ambiguous_slip_state():
    # No empty marker AND no positive-selection signal -> raise (per
    # _fanduel_slip_has_visible_selection's contract)
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="FanDuel slip is empty"):
        placer.assert_betslip_has_bet()


def test_assert_betslip_has_bet_passes_with_remove_all_signal():
    page = FakePage(locators={
        'div[role="button"]:has-text("Remove all selections")':
            FakeLocator([FakeElement(visible=True)]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    # Should not raise
    placer.assert_betslip_has_bet()


def test_slip_is_empty_recognizes_no_bet_selections_marker():
    page = FakePage(text_locators={
        "No bet selections": FakeLocator([FakeElement(visible=True)]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    assert placer._fanduel_slip_is_empty() is True


# ---- B5: find_and_click_bet tests ----

def test_find_and_click_raises_when_no_candidates(monkeypatch):
    # No tiles render for the player -> select_unique finds nothing and the
    # standard path must raise a BetPlacerError (NoPickError) fail-loud, never
    # a benign skip.
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    opp = {
        "player_name": "Jake Fraley",
        "over_line": 0.5, "under_line": 0.5,
        "market_key": "player_points",
    }
    with pytest.raises(BetPlacerError):
        placer.find_and_click_bet(opp, "over", {"display_names": ["Points"]})


def test_find_and_click_routes_batter_market_to_alternate_path(monkeypatch):
    """MLB batter_ markets MUST go through the alternate (threshold) path
    even when market_config doesn't say is_alternate=True. This is a
    load-bearing invariant of the FanDuel migration."""
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    routed_alt = []
    monkeypatch.setattr(
        placer, "_find_and_click_alternate_bet_fanduel",
        lambda opp, dir, mc, pn, ln: routed_alt.append((pn, ln)) or True
    )

    opp = {
        "player_name": "Jake Fraley",
        "over_line": 0.5, "under_line": 0.5,
        "market_key": "batter_hits",
    }
    placer.find_and_click_bet(opp, "over", {"is_alternate": False})

    assert routed_alt == [("Jake Fraley", 0.5)]


# ---- B6: wager entry + max-wager discovery tests ----

def test_discover_max_wager_returns_99999_when_no_alert(monkeypatch):
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    monkeypatch.setattr(placer, "_enter_wager_fanduel", lambda amount: True)

    amount, text = placer.discover_max_wager()

    assert amount == 99999.00
    assert "No limit" in text


def test_discover_max_wager_parses_dollar_amount(monkeypatch):
    max_wager_elem = FakeElement(visible=True, text="MAX WAGER $250.00")
    page = FakePage(text_locators={
        # text_locators is keyed by .pattern when a regex is passed; the
        # legacy code uses re.compile(r"MAX\s*WAGER", re.I).
        r"MAX\s*WAGER": FakeLocator([max_wager_elem]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    monkeypatch.setattr(placer, "_enter_wager_fanduel", lambda amount: True)

    amount, text = placer.discover_max_wager()

    assert amount == 250.00
    assert "MAX WAGER" in text


def test_enter_wager_diagnostic_dump_on_miss_raises():
    page = FakePage()  # no inputs anywhere
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Could not find FanDuel wager input"):
        placer._enter_wager_fanduel(10.00)


# ---- B7: place_bet tests ----

def test_place_bet_raises_when_button_not_found():
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    with pytest.raises(BetPlacerError, match="Place Bet button not found"):
        placer._place_bet_fanduel()


# The ACCEPTED/UNKNOWN return paths from the old ``_place_bet_fanduel``
# internal test the legacy locator-driven click. The Task 14/15
# humanization replaced that with ``mouse_click`` (which needs a
# bounding_box on the Place Bet handle); the shadow-mode short-circuit
# is covered by ``test_place_bet_short_circuits_in_shadow_mode_fd`` in
# tests/test_bet_placer_fanduel_humanized.py against the new public
# ``place_bet`` surface. The status-return paths are now exercised
# end-to-end via the live-run validators rather than unit-mocked.


# ---- B8: get_actual_odds tests ----

def test_get_actual_odds_parses_aria_label():
    odds_elem = FakeElement(
        visible=True,
        attributes={"aria-label": "Odds 2.94"},
        text="2.94",
    )
    page = FakePage(locators={
        '[aria-label^="Odds "]': FakeLocator([odds_elem]),
    })
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)

    assert placer.get_actual_odds() == 2.94


def test_get_actual_odds_returns_none_when_not_found():
    page = FakePage()
    placer = FanduelBetPlacer(page, "fanduel", AUDIT_DIR)
    assert placer.get_actual_odds() is None


def test_fanduel_no_wrong_side_fallback():
    # Only an Under tile exists for the player; an over request must RAISE,
    # never fall back to the under tile.
    from pick_matcher import select_unique, NoPickError
    import pytest
    tiles = [("under_el", "Anthony Davis, Under, 9.5, Rebounds")]
    with pytest.raises(NoPickError):
        select_unique(tiles, 9.5, "over")
    assert select_unique(tiles, 9.5, "under") == "under_el"

def test_fanduel_line_substring_not_matched():
    from pick_matcher import select_unique, NoPickError
    import pytest
    tiles = [("el", "Stephen Curry, Over, 11.5, Points")]
    with pytest.raises(NoPickError):
        select_unique(tiles, 1.5, "over")   # 1.5 must NOT match 11.5
