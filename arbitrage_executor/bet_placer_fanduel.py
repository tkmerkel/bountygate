"""FanDuel-specific bet placement implementation."""

import os
import re
from datetime import datetime
from typing import Dict, Tuple, Optional

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from selector_finder import SelectorFinder, is_alternate_market, calculate_alternate_tab_value
from execution_logger import ExecutionLogger
from text_match import fuzzy_score, fuzzy_contains
from bet_placer import BetPlacer, BetPlacerError


# FanDuel MLB threshold=1 labels: maps display name -> (verb_phrase, article, singular_noun)
# e.g., "To Hit A Single, Jake Fraley, 2.55" instead of "1+ Singles"
FANDUEL_THRESHOLD_ONE_LABELS = {
    "Single": ("To Hit", "A", "Single"),
    "Singles": ("To Hit", "A", "Single"),
    "Double": ("To Hit", "A", "Double"),
    "Doubles": ("To Hit", "A", "Double"),
    "Triple": ("To Hit", "A", "Triple"),
    "Triples": ("To Hit", "A", "Triple"),
    "Home Run": ("To Hit", "A", "Home Run"),
    "Home Runs": ("To Hit", "A", "Home Run"),
    "Hit": ("To Record", "A", "Hit"),
    "Hits": ("To Record", "A", "Hit"),
    "RBI": ("To Record", "An", "RBI"),
    "RBIs": ("To Record", "An", "RBI"),
    "Run": ("To Record", "A", "Run"),
    "Runs": ("To Record", "A", "Run"),
    "Total Base": ("To Record", "A", "Total Base"),
    "Total Bases": ("To Record", "A", "Total Base"),
    "Stolen Base": ("To Record", "A", "Stolen Base"),
    "Stolen Bases": ("To Record", "A", "Stolen Base"),
    "Strikeout": ("To Record", "A", "Strikeout"),
    "Strikeouts": ("To Record", "A", "Strikeout"),
    "Walk": ("To Record", "A", "Walk"),
    "Walks": ("To Record", "A", "Walk"),
}


class FanduelBetPlacer(BetPlacer):
    """Handles bet placement on FanDuel."""

    # ---- Abstract methods (stubs until migrated) ----

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        """Navigate FanDuel to player search results.

        FanDuel ignores ``market_config`` and ``direction`` — its bet
        finder is name+line based, not accordion based like BetMGM.
        """
        self._navigate_fanduel(opportunity)

    def _navigate_fanduel(self, opportunity: Dict):
        """Navigate FanDuel to player search results."""
        player_name = opportunity['player_name']

        print(f"[FANDUEL] Navigating to search...")
        self.page.goto("https://mo.sportsbook.fanduel.com/search", wait_until="domcontentloaded")
        self.page.wait_for_timeout(2000)

        # Dismiss any blocking modal first. FanDuel pops a "Reality Check"
        # responsible-gambling modal every ~270 minutes (button label
        # varies — "Done", "Got it", "OK"). It uses div[role="dialog"]
        # aria-modal=true and intercepts all pointer events until
        # dismissed. Left up, our slip-clear and search-input selectors
        # silently miss everything.
        self._dismiss_fanduel_modal()

        # Clear any leftover bets from a previous failed run. Otherwise the
        # bet-button click on the next step toggles a still-Selected bet OFF
        # and the slip ends up empty when we expect it populated.
        self._clear_betslip_fanduel()

        # Find search input. Scope to visible text-typed inputs only —
        # the bare `div.aq input` fallback can match hidden checkbox
        # inputs left over from the slip-clear overlay.
        search_input = None
        for sel in (
            'input[placeholder="Search"]',
            'input[type="text"][placeholder*="search" i]',
            'div.aq input[type="text"]',
            'input[type="search"]',
            'input[type="text"]:not([readonly]):not([aria-hidden="true"])',
        ):
            try:
                loc = self.page.locator(sel)
                for i in range(min(loc.count(), 5)):
                    cand = loc.nth(i)
                    if not cand.is_visible():
                        continue
                    if cand.get_attribute("readonly") is not None:
                        continue
                    if cand.get_attribute("aria-hidden") == "true":
                        continue
                    search_input = cand
                    break
                if search_input is not None:
                    break
            except Exception:
                continue

        if search_input is None:
            self._screenshot("search_input_not_found")
            raise BetPlacerError("Could not find FanDuel search input")

        print(f"[FANDUEL] Searching for: {player_name}")
        search_input.fill(player_name)
        self.page.keyboard.press("Enter")
        self.page.wait_for_timeout(3000)

        self._screenshot("search_results")
        print(f"[FANDUEL] ✓ Search results loaded")

    def _dismiss_fanduel_modal(self) -> None:
        """Dismiss any FanDuel modal blocking the page.

        Primary case: the "Reality Check" responsible-gambling modal
        that fires every ~270 minutes of session activity. Implemented
        as `<div role="dialog" aria-modal="true">` and intercepts every
        pointer event until dismissed. Button label varies — observed:
        "Done", "Got it", "OK". The modal contains a single button so
        we click it regardless of label.

        Idempotent and silent — if no modal is present, return.
        """
        try:
            modal = self.page.locator('div[role="dialog"][aria-modal="true"]')
            if modal.count() == 0:
                return
            if not modal.first.is_visible():
                return
            # Capture the modal's inner text for the log (helps if FD
            # introduces a different modal type that needs a separate
            # dismissal path).
            try:
                t = (modal.first.inner_text() or "").strip()[:120]
            except Exception:
                t = "(no text)"
            print(f"[FANDUEL] Dismissing modal: {t!r}")
            buttons = modal.first.locator("button")
            if buttons.count() > 0:
                buttons.first.click()
                self.page.wait_for_timeout(1500)
                print(f"[FANDUEL] Modal dismissed.")
        except Exception as e:
            print(f"[FANDUEL] ⚠ Modal-dismiss probe failed: {e} (continuing)")

    def clear_betslip(self):
        raise NotImplementedError("migrated in Task B3")

    def assert_betslip_has_bet(self):
        raise NotImplementedError("migrated in Task B4")

    def assert_betslip_empty(self):
        raise NotImplementedError("migrated in Task B4")

    def find_and_click_bet(self, opportunity, direction, market_config):
        raise NotImplementedError("migrated in Task B5")

    def enter_wager(self, amount):
        raise NotImplementedError("migrated in Task B6")

    def place_bet(self):
        raise NotImplementedError("migrated in Task B7")

    def get_actual_odds(self):
        raise NotImplementedError("migrated in Task B8")

    # discover_max_wager is FD-specific
    def discover_max_wager(self):
        raise NotImplementedError("migrated in Task B6")
