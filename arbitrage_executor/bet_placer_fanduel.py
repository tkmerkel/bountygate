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
        """Assert a selected bet actually reached the slip.

        Used by the selector validation harness. It intentionally stops
        before wager entry or placement.
        """
        if not self._fanduel_slip_has_visible_selection():
            self._screenshot("validation_slip_empty")
            raise BetPlacerError("FanDuel slip is empty after bet click")

    def assert_betslip_empty(self):
        """Assert the slip is empty after cleanup."""
        if not self._fanduel_slip_is_empty():
            self._screenshot("validation_slip_not_empty")
            raise BetPlacerError("FanDuel slip still appears to contain a bet")

    def _fanduel_slip_has_bet(self) -> bool:
        """Return True if the FanDuel betslip currently holds at least one
        bet. Used to verify a click actually targeted a bet button — an
        aria-label without button-restriction can match a player profile
        link, which navigates away without populating the slip.

        Conservative on the True side: if we can't determine state cleanly,
        return True so the calling flow proceeds (better to attempt wager
        entry and fail there than to mis-claim an empty-slip condition).
        """
        try:
            empty_marker = self.page.get_by_text("Betslip empty", exact=False)
            if empty_marker.count() > 0 and empty_marker.first.is_visible():
                return False
        except Exception:
            pass
        try:
            empty_marker = self.page.get_by_text("No bet selections", exact=False)
            if empty_marker.count() > 0 and empty_marker.first.is_visible():
                return False
        except Exception:
            pass
        return True

    def _fanduel_slip_is_empty(self) -> bool:
        """Return True only when FanDuel exposes a clear empty-slip signal."""
        try:
            for text in ("Betslip empty", "No bet selections"):
                empty_marker = self.page.get_by_text(text, exact=False)
                if empty_marker.count() > 0 and empty_marker.first.is_visible():
                    return True
        except Exception:
            pass
        try:
            for sel in (
                'button[aria-label*="remove" i]',
                '[data-testid*="remove-selection" i]',
            ):
                loc = self.page.locator(sel)
                for i in range(min(loc.count(), 5)):
                    try:
                        if loc.nth(i).is_visible():
                            return False
                    except Exception:
                        continue
        except Exception:
            pass
        return False

    def _fanduel_slip_has_visible_selection(self) -> bool:
        """Return True only when FanDuel exposes a concrete slip-selection signal."""
        try:
            if self._fanduel_slip_is_empty():
                return False
        except Exception:
            pass

        try:
            wins_pattern = self.page.get_by_text(
                re.compile(r"\$[\d.]+ wins \$[\d.]+", re.I)
            )
            if wins_pattern.count() > 0 and wins_pattern.first.is_visible():
                return True
        except Exception:
            pass

        for sel in (
            'button[aria-label*="remove" i]',
            'div[role="button"]:has-text("Remove all selections")',
            '[role="button"]:has-text("Remove all selections")',
            '[data-testid*="remove-selection" i]',
            '[data-test-id*="remove-selection" i]',
            'input[aria-label*="wager" i]',
            'input[name*="wager" i]',
            'button:has-text("Place"):has-text("bet")',
            'button:has-text("Place Bet")',
        ):
            try:
                loc = self.page.locator(sel)
                for i in range(min(loc.count(), 5)):
                    try:
                        if loc.nth(i).is_visible():
                            return True
                    except Exception:
                        continue
            except Exception:
                continue

        return False

    def find_and_click_bet(self, opportunity, direction, market_config):
        """Find and click the bet button for the specified player/line/direction.

        Returns:
            True if bet successfully added to betslip

        Raises:
            BetPlacerError: If bet cannot be found or clicked
        """
        player_name = opportunity['player_name']
        line = opportunity['over_line'] if direction == 'over' else opportunity['under_line']
        market_key = opportunity.get('over_market_key') if direction == 'over' else opportunity.get('under_market_key')
        if not market_key:
            market_key = opportunity['market_key']

        print(f"[FANDUEL] Finding bet: {player_name} {direction} {line}")

        # FanDuel MLB markets always use threshold format ("2+ Stolen Bases",
        # "To Hit A Single") even when the market_key doesn't have
        # _alternate suffix (primary table rows)
        is_alternate = (
            market_config.get('is_alternate', False)
            or is_alternate_market(market_key)
            or market_key.startswith("batter_")
            or market_key.startswith("pitcher_")
        )

        if is_alternate:
            return self._find_and_click_alternate_bet_fanduel(
                opportunity, direction, market_config, player_name, line
            )

        # Use SelectorFinder to locate the bet (non-alternate path)
        display_names = market_config.get('display_names', [market_key])
        candidates = SelectorFinder.find_candidates_by_text(
            self.page, display_names, player_name, line
        )

        if not candidates:
            # Diagnostic dumps on the failure path
            try:
                aria_loc = self.page.locator(f'[aria-label*="{player_name}"]')
                aria_dump = []
                for i in range(min(aria_loc.count(), 10)):
                    try:
                        aria_dump.append(aria_loc.nth(i).get_attribute("aria-label"))
                    except Exception:
                        continue
                print(f"[FANDUEL] aria-labels mentioning {player_name!r} "
                      f"({len(aria_dump)}): {aria_dump!r}")
            except Exception:
                pass
            try:
                btn_dump = []
                btn_loc = self.page.locator(f'button:has-text("{player_name}")')
                for i in range(min(btn_loc.count(), 10)):
                    try:
                        txt = (btn_loc.nth(i).text_content() or "").strip()[:120]
                        btn_dump.append(txt)
                    except Exception:
                        continue
                print(f"[FANDUEL] buttons mentioning {player_name!r} "
                      f"({len(btn_dump)}): {btn_dump!r}")
            except Exception:
                pass
            self._screenshot("bet_not_found")
            raise BetPlacerError(f"No bet found for {player_name} {direction} {line}")

        # Filter by direction
        direction_candidates = [
            c for c in candidates
            if (direction == 'over' and '[over]' in c.preview_text.lower()) or
               (direction == 'under' and '[under]' in c.preview_text.lower())
        ]
        if not direction_candidates:
            print(f"⚠ Could not filter by direction, using first candidate")
            selected = candidates[0]
        else:
            selected = direction_candidates[0]

        print(f"[FANDUEL] Clicking bet: {selected.preview_text[:60]}")

        try:
            # Pick the first VISIBLE match. FanDuel renders hidden DOM
            # duplicates (mobile-layout, promo cards) ahead of the real
            # tile, and `.first` will silently grab one of those and
            # time out on click.
            loc = self.page.locator(selected.selector)
            count = loc.count()
            locator = None
            for i in range(count):
                cand = loc.nth(i)
                try:
                    if cand.is_visible():
                        locator = cand
                        break
                except Exception:
                    continue
            if locator is None:
                raise BetPlacerError(
                    f"Selector matched {count} elements but none were visible: {selected.selector}"
                )
            locator.click(timeout=10000)
            self.page.wait_for_timeout(1500)

            # Expand viewport for betslip interaction
            print(f"[FANDUEL] Expanding viewport to 1920x945...")
            self.page.set_viewport_size({"width": 1920, "height": 945})
            self.page.wait_for_timeout(500)

            self._screenshot("bet_clicked")
            print(f"[FANDUEL] ✓ Bet added to slip")
            return True
        except Exception as e:
            self._screenshot("click_failed")
            raise BetPlacerError(f"Failed to click bet: {e}")

    def _find_and_click_alternate_bet_fanduel(self, opportunity: Dict, direction: str,
                                              market_config: Dict, player_name: str, line: float) -> bool:
        """Find threshold-based bet on FanDuel for alternate markets.

        For alternate markets, FanDuel shows bets like "4+ Points" instead of "Over 4.5 Points".
        This method searches for the threshold format.
        """
        threshold = calculate_alternate_tab_value(line)
        display_names = market_config.get('display_names', ['Points'])
        base_display = display_names[0] if display_names else 'Points'

        print(f"[FANDUEL] Alternate market: searching for {player_name} {threshold}+ {base_display}")

        # Build selector patterns for threshold-based bets
        selector_patterns = []

        # For threshold=1 (line=0.5), FanDuel MLB uses "To Hit A Single" / "To Record An RBI" style
        if threshold == 1:
            label_info = FANDUEL_THRESHOLD_ONE_LABELS.get(base_display)
            if label_info:
                verb, article, noun = label_info
                # e.g., [aria-label*="To Hit"][aria-label*="A Single"][aria-label*="Jake Fraley"]
                selector_patterns.extend([
                    f'[aria-label*="{verb}"][aria-label*="{article} {noun}"][aria-label*="{player_name}"]',
                    f'[aria-label*="{verb} {article} {noun}"][aria-label*="{player_name}"]',
                ])

        # Standard N+ patterns. Button-restricted variants come FIRST because
        # an unscoped [aria-label*=...] matches any element with the label
        # (player avatars, section headers, profile links) — clicking those
        # navigates away from the bet without adding to slip.
        selector_patterns.extend([
            # Button-restricted, most specific
            f'button[aria-label*="{player_name}"][aria-label*="{threshold}+"][aria-label*="{base_display}"]',
            f'[role="button"][aria-label*="{player_name}"][aria-label*="{threshold}+"][aria-label*="{base_display}"]',
            f'button[aria-label*="{player_name}"][aria-label*="{threshold} or more"][aria-label*="{base_display}"]',
            # Unrestricted aria-label fallbacks
            f'[aria-label*="{player_name}"][aria-label*="{threshold}+"][aria-label*="{base_display}"]',
            f'[aria-label*="{player_name}"][aria-label*="{threshold} or more"][aria-label*="{base_display}"]',
            f'[aria-label*="{player_name}"][aria-label*="{threshold}"][aria-label*="{base_display}"]',
            # Text-based patterns with market type
            f'button:has-text("{player_name}"):has-text("{threshold}+"):has-text("{base_display}")',
            f'div:has-text("{player_name}"):has-text("{threshold}+"):has-text("{base_display}") button',
        ])

        for selector in selector_patterns:
            try:
                locator = self.page.locator(selector)
                if locator.count() > 0:
                    print(f"[FANDUEL] Found alternate bet using: {selector}")

                    # Get the first visible match
                    for i in range(locator.count()):
                        elem = locator.nth(i)
                        if elem.is_visible():
                            # Capture pre-click state so we can tell whether
                            # the click was a TOGGLE (FanDuel bet buttons
                            # toggle between selected/unselected — clicking
                            # an already-Selected one removes it from slip).
                            try:
                                tag = elem.evaluate("e => e.tagName") or "?"
                                aria_before = elem.get_attribute("aria-label") or ""
                                role = elem.get_attribute("role") or ""
                                was_selected = " Selected" in aria_before
                                clicked_desc = (
                                    f"tag={tag} role={role!r} "
                                    f"aria={aria_before[:80]!r} "
                                    f"was_selected={was_selected}"
                                )
                            except Exception:
                                was_selected = False
                                clicked_desc = "<unknown>"

                            elem.click(timeout=10000)
                            self.page.wait_for_timeout(1500)

                            # If the bet started Selected, the click likely
                            # toggled it OFF — click again to re-add. Re-
                            # locate first; the DOM may have re-rendered.
                            if was_selected:
                                try:
                                    elem2 = self.page.locator(selector).first
                                    aria_after = elem2.get_attribute("aria-label") or ""
                                    if " Selected" not in aria_after:
                                        print(f"[FANDUEL] Click deselected an "
                                              f"already-Selected bet; re-clicking to add.")
                                        elem2.click(timeout=10000)
                                        self.page.wait_for_timeout(1500)
                                except Exception as e:
                                    print(f"[FANDUEL] Re-locate after toggle failed: {e}")

                            # Expand viewport for betslip interaction
                            print(f"[FANDUEL] Expanding viewport to 1920x945...")
                            self.page.set_viewport_size({"width": 1920, "height": 945})
                            self.page.wait_for_timeout(500)

                            self._screenshot("alternate_bet_clicked")

                            # Verify the click actually added a bet.
                            if not self._fanduel_slip_has_bet():
                                print(f"[FANDUEL] ⚠ Slip still empty after click "
                                      f"using {selector} — clicked element was "
                                      f"{clicked_desc}. Aborting (next opportunity will retry).")
                                self._screenshot("alternate_bet_did_not_add_to_slip")
                                raise BetPlacerError(
                                    f"FanDuel bet click did not add to slip "
                                    f"(selector={selector!r}, clicked={clicked_desc})"
                                )

                            print(f"[FANDUEL] ✓ Alternate bet added to slip")
                            return True
            except BetPlacerError:
                raise
            except Exception as e:
                print(f"[FANDUEL] Selector pattern failed: {selector} - {e}")
                continue

        # Fallback: try the standard search with threshold as line
        print(f"[FANDUEL] ⚠ Direct selectors failed, trying standard search with threshold...")
        candidates = SelectorFinder.find_candidates_by_text(
            self.page, display_names, player_name, threshold
        )

        if candidates:
            selected = candidates[0]
            try:
                locator = self.page.locator(selected.selector).first
                locator.click(timeout=10000)
                self.page.wait_for_timeout(1500)

                self.page.set_viewport_size({"width": 1920, "height": 945})
                self.page.wait_for_timeout(500)

                self._screenshot("alternate_bet_clicked")
                print(f"[FANDUEL] ✓ Alternate bet added to slip (via fallback)")
                return True
            except Exception as e:
                self._screenshot("alternate_click_failed")
                raise BetPlacerError(f"Failed to click alternate bet: {e}")

        self._screenshot("alternate_bet_not_found")
        raise BetPlacerError(f"No alternate bet found for {player_name} {threshold}+ {base_display}")

    def enter_wager(self, amount):
        """Enter wager amount in betslip."""
        print(f"[FANDUEL] Entering wager: ${amount:.2f}")
        return self._enter_wager_fanduel(amount)

    def discover_max_wager(self):
        """Enter a large amount to discover FanDuel's max wager limit.

        Returns:
            (max_wager_amount, raw_text)

        Raises:
            BetPlacerError: If max wager cannot be discovered
        """
        print(f"[FANDUEL] Discovering max wager (entering 99999)...")

        try:
            self._enter_wager_fanduel(99999.00)
            self.page.wait_for_timeout(1500)

            # Look for MAX WAGER text
            max_wager_candidates = self.page.get_by_text(re.compile(r"MAX\s*WAGER", re.I))

            if max_wager_candidates.count() == 0:
                self._screenshot("max_wager_not_found")
                print(f"⚠ No MAX WAGER alert found, assuming unlimited")
                return 99999.00, "No limit detected"

            # Find the visible MAX WAGER with highest amount
            best_amount = None
            best_text = None

            for i in range(max_wager_candidates.count()):
                try:
                    loc = max_wager_candidates.nth(i)
                    if not loc.is_visible():
                        continue

                    text = loc.text_content() or ""
                    match = re.search(r"\$\s*([0-9,]+(?:\.[0-9]{2})?)", text)
                    if match:
                        amount = float(match.group(1).replace(",", ""))
                        if best_amount is None or amount > best_amount:
                            best_amount = amount
                            best_text = text
                except Exception:
                    continue

            if best_amount is None:
                self._screenshot("max_wager_parse_failed")
                print(f"⚠ Could not parse MAX WAGER amount, assuming $500")
                return 500.00, "Parse failed"

            self._screenshot("max_wager_discovered")
            print(f"[FANDUEL] ✓ Max wager: ${best_amount:.2f}")
            return best_amount, best_text

        except Exception as e:
            self._screenshot("max_wager_discovery_failed")
            raise BetPlacerError(f"Max wager discovery failed: {e}")

    def _enter_wager_fanduel(self, amount: float) -> bool:
        """Enter wager on FanDuel.

        FanDuel's betslip is a React panel whose CSS class names rotate
        between deploys (we've seen ``.hk``, ``.jo``, ``.bt``, ``.cg``).
        Class-based selectors here are best-effort fallbacks behind more
        durable ones (accessible names, ``inputmode="decimal"``, the
        ``"$X wins $Y"`` text pattern). When everything fails, the diagnostic
        block at the bottom dumps every visible input to the audit log so a
        follow-up selector update has data to work from.
        """
        try:
            # Step 1: Make sure the betslip panel is actually open. After
            # ``find_and_click_bet`` the slip *usually* auto-expands on a
            # desktop viewport, but a re-rendered DOM can leave it collapsed,
            # in which case the wager input doesn't exist yet.
            betslip_open_selectors = [
                # Durable patterns first
                '[data-test-id*="betslip" i] button',
                '[data-testid*="betslip" i] button',
                '[data-test-id*="bet-slip" i] button',
                'button[aria-label*="bet slip" i]',
                'button:has-text("Bet Slip")',
                'aside button:has-text("Bet Slip")',
                # Old class-based fallbacks (rotate frequently)
                'div.ay > div > div > div > div.hk > div',
                'div.ay > div > div > div > div.jo > div',
            ]

            betslip_opened = False
            for selector in betslip_open_selectors:
                try:
                    panel = self.page.locator(selector).first
                    if panel.count() > 0 and panel.is_visible():
                        print(f"[FANDUEL] Clicking betslip to open using: {selector}")
                        panel.click()
                        self.page.wait_for_timeout(800)
                        betslip_opened = True
                        break
                except Exception:
                    continue

            # Text-based fallback: the collapsed slip header shows
            # "$X wins $Y" — clicking that opens the panel.
            if not betslip_opened:
                try:
                    wins_pattern = self.page.get_by_text(
                        re.compile(r"\$[\d.]+ wins \$[\d.]+", re.I)
                    )
                    if wins_pattern.count() > 0:
                        print(f"[FANDUEL] Clicking betslip using wins pattern...")
                        wins_pattern.first.click()
                        self.page.wait_for_timeout(800)
                        betslip_opened = True
                except Exception:
                    pass

            if not betslip_opened:
                print(f"[FANDUEL] ⚠ Could not click betslip panel; the wager "
                      f"input may already be visible, continuing...")

            self._screenshot("betslip_opened")

            # Step 2: Find the wager input. Order matters — most durable
            # locators first so we don't latch onto the wrong field.
            wager_input = None

            # 2a. Accessible name "WAGER $" (set by FD's design system)
            try:
                aria_input = self.page.get_by_label("WAGER $")
                if aria_input.count() > 0 and aria_input.first.is_visible():
                    wager_input = aria_input.first
                    print(f"[FANDUEL] Found wager input via get_by_label('WAGER $')")
            except Exception:
                pass

            # 2b. Partial accessible name match
            if wager_input is None:
                try:
                    aria_input = self.page.get_by_label(re.compile(r"WAGER", re.I))
                    if aria_input.count() > 0 and aria_input.first.is_visible():
                        wager_input = aria_input.first
                        print(f"[FANDUEL] Found wager input via partial label")
                except Exception:
                    pass

            # 2c. inputmode is the most durable structural attribute for
            # monetary inputs — FD's wager input has inputmode="decimal".
            if wager_input is None:
                inputmode_selectors = [
                    'input[inputmode="decimal"]',
                    'input[inputmode="numeric"]',
                    'input[type="number"]',
                ]
                for sel in inputmode_selectors:
                    try:
                        loc = self.page.locator(sel)
                        for i in range(loc.count()):
                            cand = loc.nth(i)
                            try:
                                if not cand.is_visible():
                                    continue
                                placeholder = (cand.get_attribute("placeholder") or "").lower()
                                if "search" in placeholder:
                                    continue
                                wager_input = cand
                                print(f"[FANDUEL] Found wager input via {sel}")
                                break
                            except Exception:
                                continue
                        if wager_input is not None:
                            break
                    except Exception:
                        continue

            # 2d. Attribute-based fallbacks scoped to the slip area
            if wager_input is None:
                attr_selectors = [
                    'input[aria-label*="wager" i]',
                    'input[aria-label*="stake" i]',
                    'input[name*="wager" i]',
                    'input[name*="stake" i]',
                ]
                for sel in attr_selectors:
                    try:
                        loc = self.page.locator(sel)
                        if loc.count() > 0 and loc.first.is_visible():
                            wager_input = loc.first
                            print(f"[FANDUEL] Found wager input via {sel}")
                            break
                    except Exception:
                        continue

            # 2e. Old class-chained fallbacks (rotate frequently)
            if wager_input is None:
                legacy_selectors = [
                    '#main div > div > div.bt input',
                    '#main div > div > div.cg input',
                    '#main input[type="text"]',
                ]
                for sel in legacy_selectors:
                    try:
                        loc = self.page.locator(sel)
                        for i in range(loc.count()):
                            cand = loc.nth(i)
                            try:
                                if not cand.is_visible():
                                    continue
                                placeholder = (cand.get_attribute("placeholder") or "").lower()
                                if "search" in placeholder:
                                    continue
                                wager_input = cand
                                print(f"[FANDUEL] Found wager input via legacy {sel}")
                                break
                            except Exception:
                                continue
                        if wager_input is not None:
                            break
                    except Exception:
                        continue

            if wager_input is None:
                # Diagnostic: dump every visible input so the next selector
                # update has data to work from. Cheap, and only runs once per
                # failure.
                try:
                    all_inputs = self.page.locator("input")
                    dump = []
                    for i in range(min(all_inputs.count(), 25)):
                        elem = all_inputs.nth(i)
                        try:
                            if not elem.is_visible():
                                continue
                            dump.append({
                                "type": elem.get_attribute("type"),
                                "name": elem.get_attribute("name"),
                                "aria": elem.get_attribute("aria-label"),
                                "placeholder": elem.get_attribute("placeholder"),
                                "inputmode": elem.get_attribute("inputmode"),
                                "id": elem.get_attribute("id"),
                            })
                        except Exception:
                            continue
                    print(f"[FANDUEL] visible inputs at failure: {dump}")
                except Exception:
                    pass
                self._screenshot("wager_input_not_found")
                raise BetPlacerError("Could not find FanDuel wager input")

            # Step 3: enter the amount. React inputs sometimes ignore `fill`
            # for the clear step, so use a select-all + type pattern that
            # reliably fires the change event.
            wager_input.click()
            try:
                wager_input.press("Control+A")
                wager_input.press("Delete")
            except Exception:
                wager_input.fill("")
            self.page.wait_for_timeout(200)
            wager_input.type(f"{amount:.2f}", delay=50)
            self.page.wait_for_timeout(1000)

            self._screenshot("wager_entered")
            print(f"[FANDUEL] ✓ Wager entered: ${amount:.2f}")
            return True

        except Exception as e:
            self._screenshot("wager_entry_failed")
            raise BetPlacerError(f"Failed to enter wager: {e}")

    def place_bet(self):
        """Click the Place Bet button and check for success/failure."""
        print(f"[FANDUEL] Placing bet...")
        return self._place_bet_fanduel()

    def _place_bet_fanduel(self) -> Tuple[str, str]:
        """Place bet on FanDuel.

        FanDuel labels the button as "Place $X.YZ bet" (dollar amount
        included). The data-testid rotates between deploys — observed
        "place-bet-button" historically; on 2026-05-12 the testid was
        missing and only the text pattern matched. Try multiple strategies
        in order of durability.
        """
        try:
            place_btn = None

            # 1. Text pattern — "Place $X.YZ bet" (most durable, matches
            #    the label users actually see).
            try:
                cand = self.page.get_by_role(
                    "button", name=re.compile(r"Place\s*\$[\d.]+\s*bet", re.I)
                )
                if cand.count() > 0 and cand.first.is_visible():
                    place_btn = cand.first
                    print(f"[FANDUEL] Found Place Bet via role+name=Place $X bet")
            except Exception:
                pass

            # 2. Generic "Place ... bet" or "Place Bet"
            if place_btn is None:
                try:
                    cand = self.page.get_by_role(
                        "button", name=re.compile(r"^Place.*bet$", re.I)
                    )
                    if cand.count() > 0 and cand.first.is_visible():
                        place_btn = cand.first
                        print(f"[FANDUEL] Found Place Bet via role+name='Place...bet'")
                except Exception:
                    pass

            # 3. Legacy data-testid (kept as fallback in case FD restores it)
            if place_btn is None:
                cand = self.page.locator('[data-testid="place-bet-button"]')
                if cand.count() > 0 and cand.first.is_visible():
                    place_btn = cand.first
                    print(f"[FANDUEL] Found Place Bet via data-testid")

            # 4. data-test-id (different attribute name, FD has mixed both)
            if place_btn is None:
                cand = self.page.locator('[data-test-id="place-bet-button"]')
                if cand.count() > 0 and cand.first.is_visible():
                    place_btn = cand.first
                    print(f"[FANDUEL] Found Place Bet via data-test-id")

            if place_btn is None:
                # Diagnostic: list buttons whose text starts with "Place" so
                # next selector update has data.
                try:
                    btns = self.page.locator('button:has-text("Place")')
                    dump = []
                    for i in range(min(btns.count(), 10)):
                        try:
                            txt = (btns.nth(i).text_content() or "").strip()[:80]
                            dump.append(txt)
                        except Exception:
                            continue
                    print(f"[FANDUEL] buttons starting with 'Place' ({len(dump)}): {dump!r}")
                except Exception:
                    pass
                self._screenshot("place_bet_not_found")
                raise BetPlacerError("Place Bet button not found")

            print(f"[FANDUEL] Clicking Place Bet...")
            place_btn.click()
            self.page.wait_for_timeout(2000)

            # Success detection — try several signals because the
            # data-testid for the "Done" receipt button has gone stale
            # before. Poll briefly to give the confirmation time to render.
            success_signals = [
                ('[data-testid="bet-receipt-done-btn"]', "legacy data-testid"),
                ('[data-test-id="bet-receipt-done-btn"]', "data-test-id variant"),
            ]
            success_texts = [
                "Bet placed",
                "Bet accepted",
                "Your bet has been placed",
                "Wager Accepted",
            ]
            for _ in range(6):  # ~3s polling
                for sel, label in success_signals:
                    try:
                        loc = self.page.locator(sel)
                        if loc.count() > 0 and loc.first.is_visible():
                            self._screenshot("bet_placed_success")
                            print(f"[FANDUEL] ✓ Bet ACCEPTED ({label})")
                            return "ACCEPTED", "Bet placed successfully"
                    except Exception:
                        continue
                for text in success_texts:
                    try:
                        loc = self.page.get_by_text(text, exact=False)
                        if loc.count() > 0 and loc.first.is_visible():
                            self._screenshot("bet_placed_success")
                            print(f"[FANDUEL] ✓ Bet ACCEPTED (text={text!r})")
                            return "ACCEPTED", f"Bet placed ({text})"
                    except Exception:
                        continue
                self.page.wait_for_timeout(500)

            # Check for error messages
            error_indicators = [
                self.page.get_by_text(re.compile(r"error|fail|reject", re.I)),
                self.page.get_by_text(re.compile(r"limit exceeded", re.I)),
            ]

            for indicator in error_indicators:
                if indicator.count() > 0:
                    msg = indicator.first.text_content() or "Unknown error"
                    self._screenshot("bet_rejected")
                    print(f"[FANDUEL] ✗ Bet REJECTED: {msg}")
                    return "REJECTED", msg

            # Unknown state — diagnostic dump so the next pass can map a
            # new confirmation signal.
            try:
                btns = self.page.locator("button")
                dump = []
                for i in range(min(btns.count(), 15)):
                    try:
                        btn = btns.nth(i)
                        if btn.is_visible():
                            dump.append((btn.text_content() or "").strip()[:60])
                    except Exception:
                        continue
                print(f"[FANDUEL] post-place visible buttons ({len(dump)}): {dump!r}")
            except Exception:
                pass
            self._screenshot("bet_status_unknown")
            print(f"[FANDUEL] ? Bet status UNKNOWN")
            return "UNKNOWN", "Could not determine bet status"

        except Exception as e:
            self._screenshot("place_bet_failed")
            raise BetPlacerError(f"Place bet failed: {e}")

    def get_actual_odds(self):
        """Extract actual odds from FanDuel betslip.

        FanDuel displays odds in a span with aria-label="Odds X.XX"
        Example: <span aria-label="Odds 2.94" class="...">2.94</span>

        Returns:
            Decimal odds as float, or None if not found
        """
        try:
            # Primary method: Look for span with aria-label="Odds X.XX"
            odds_selectors = [
                '[aria-label^="Odds "]',  # aria-label starts with "Odds "
                'span[aria-label^="Odds "]',
            ]

            for selector in odds_selectors:
                try:
                    odds_elem = self.page.locator(selector)
                    if odds_elem.count() > 0:
                        # Try aria-label first (more reliable)
                        aria_label = odds_elem.first.get_attribute("aria-label") or ""
                        odds_match = re.search(r'Odds\s+(\d+\.?\d*)', aria_label)
                        if odds_match:
                            decimal_odds = float(odds_match.group(1))
                            print(f"[FANDUEL] Extracted odds from aria-label: {decimal_odds:.3f}")
                            return decimal_odds

                        # Fallback to text content
                        text = odds_elem.first.text_content() or ""
                        text = text.strip()
                        decimal_match = re.search(r'(\d+\.?\d*)', text)
                        if decimal_match:
                            decimal_odds = float(decimal_match.group(1))
                            print(f"[FANDUEL] Extracted odds from text: {decimal_odds:.3f}")
                            return decimal_odds
                except Exception:
                    continue

            print(f"[FANDUEL] ⚠ Could not extract odds from betslip")
            return None

        except Exception as e:
            print(f"[FANDUEL] ⚠ Error extracting odds: {e}")
            return None
