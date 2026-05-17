"""Regression tests for bet-placement sequencing guards."""

import pytest

from bet_placer import BetPlacer, BetPlacerError


AUDIT_DIR = "audit_logs/test_bet_placer_sequencing"


class FakeElement:
    def __init__(self, *, visible=True, text="", on_click=None):
        self.visible = visible
        self.text = text
        self.on_click = on_click
        self.clicked = False

    def is_visible(self):
        return self.visible

    def click(self, *args, **kwargs):
        self.clicked = True
        if self.on_click:
            self.on_click()

    def text_content(self):
        return self.text

    def get_attribute(self, name):
        return None


class FakeLocator:
    def __init__(self, elements=None):
        self.elements = list(elements or [])

    @property
    def first(self):
        return self.elements[0] if self.elements else FakeElement(visible=False)

    def count(self):
        return len(self.elements)

    def nth(self, index):
        return self.elements[index]

    def is_visible(self):
        return self.first.is_visible()

    def click(self, *args, **kwargs):
        return self.first.click(*args, **kwargs)

    def text_content(self):
        return self.first.text_content()


class FakePage:
    def __init__(self, *, locators=None, text_locators=None):
        self.locators = locators or {}
        self.text_locators = text_locators or {}
        self.waits = []

    def locator(self, selector):
        return self.locators.get(selector, FakeLocator())

    def get_by_text(self, text, exact=False):
        return self.text_locators.get(text, FakeLocator())

    def wait_for_timeout(self, ms):
        self.waits.append(ms)

    def screenshot(self, *args, **kwargs):
        return None


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
