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
        """Clear the FanDuel betslip and fail if it remains non-empty."""
        self._clear_betslip_fanduel()

    def _clear_betslip_fanduel(self) -> None:
        """Empty the FanDuel betslip if it isn't already.

        Best-effort. Stale bets in the slip cause the next bet-button click
        to toggle the SAME bet OFF (the aria-label still says " Selected"),
        leaving the slip empty when we go to enter a wager. Run this before
        every search to start from a known-empty state.

        Does not raise — if removal fails for any reason, we log and
        continue. The downstream _fanduel_slip_has_bet check after the
        click is the real safety net.
        """
        try:
            # Open the slip first; the remove buttons live inside its panel.
            try:
                wins_pattern = self.page.get_by_text(
                    re.compile(r"\$[\d.]+ wins \$[\d.]+", re.I)
                )
                if wins_pattern.count() > 0 and wins_pattern.first.is_visible():
                    wins_pattern.first.click()
                    self.page.wait_for_timeout(600)
            except Exception:
                pass

            # If already empty, bail.
            try:
                empty = self.page.get_by_text("Betslip empty", exact=False)
                if empty.count() > 0 and empty.first.is_visible():
                    print(f"[FANDUEL] Slip already empty.")
                    return
            except Exception:
                pass

            clicked_clear_all = False

            # 1. Try a single "Remove all" / "Clear" affordance.
            # NOTE: the visible "Remove all selections" element is a
            # div[role="button"], NOT a <button> tag. Earlier
            # button:has-text() selectors silently missed it across an
            # entire session — every iteration's "slip cleared" log was
            # a false positive while the slip retained a stale tease.
            for sel in (
                '[data-testid="remove-all-selections-button"]',
                '[data-test-id="remove-all-selections-button"]',
                'div[role="button"]:has-text("Remove all selections")',
                'div[role="button"]:has-text("Remove all")',
                '[role="button"]:has-text("Remove all selections")',
                'button[aria-label*="remove all" i]',
                'button[aria-label*="clear all" i]',
                'button:has-text("Remove all")',
                'button:has-text("Clear all")',
            ):
                try:
                    loc = self.page.locator(sel)
                    if loc.count() > 0 and loc.first.is_visible():
                        print(f"[FANDUEL] Clearing slip via {sel}")
                        loc.first.click()
                        self.page.wait_for_timeout(800)
                        clicked_clear_all = True
                        break
                except Exception:
                    continue

            # 2. Otherwise, click individual remove buttons until empty.
            if not clicked_clear_all:
                for _ in range(10):  # safety cap
                    removed = False
                    for sel in (
                        'button[aria-label*="remove" i]',
                        'button[aria-label*="delete" i]',
                        '[data-testid*="remove-selection" i]',
                        '[data-test-id*="remove-selection" i]',
                    ):
                        try:
                            loc = self.page.locator(sel)
                            n = loc.count()
                            for i in range(n):
                                cand = loc.nth(i)
                                try:
                                    if cand.is_visible():
                                        cand.click(timeout=2000)
                                        self.page.wait_for_timeout(400)
                                        removed = True
                                        break
                                except Exception:
                                    continue
                            if removed:
                                break
                        except Exception:
                            continue
                    if not removed:
                        break
            print(f"[FANDUEL] Slip cleared (best-effort).")
        except Exception as e:
            print(f"[FANDUEL] ⚠ Slip clear failed: {e} (continuing).")

        # Post-clear verification: if the slip still has bets, halt loud.
        # A stale slip causes the next bet click to toggle OFF instead of ON,
        # leaving wager entry against an empty slip — wastes the run and may
        # leak browser state into subsequent attempts.
        try:
            empty_marker = self.page.get_by_text("Betslip empty", exact=False)
            if empty_marker.count() > 0 and empty_marker.first.is_visible():
                return  # confirmed empty
        except Exception:
            pass
        # Fallback signal: look for a non-zero count near "Bet slip (N)" or
        # any remaining remove-selection button. Either is evidence of items.
        try:
            for sel in (
                'button[aria-label*="remove" i]',
                '[data-testid*="remove-selection" i]',
            ):
                loc = self.page.locator(sel)
                for i in range(loc.count()):
                    try:
                        if loc.nth(i).is_visible():
                            raise BetPlacerError(
                                f"FanDuel slip-clear failed: remove control "
                                f"still present ({sel!r})"
                            )
                    except BetPlacerError:
                        raise
                    except Exception:
                        continue
        except BetPlacerError:
            raise
        except Exception:
            # Verification probe itself failed — don't escalate; the original
            # best-effort clear may have succeeded.
            pass

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
