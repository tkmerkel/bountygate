"""FanDuel humanized bet placer.

Rewrite of the legacy bet_placer_fanduel.py against the new
``arbitrage_executor/human/`` primitives. Public interface preserved
(see ``bet_placer.BetPlacer`` ABC).

Task 14 surface: ``__init__``, ``navigate_and_expand_market``,
``clear_betslip``, ``assert_betslip_has_bet``, ``assert_betslip_empty``.
The find/click/wager/place/odds/limit methods remain Task 15 stubs.
"""

import re
from typing import Dict, Optional, Tuple

from bet_placer import BetPlacer, BetPlacerError
from human.mouse import CursorState, click as mouse_click
from human.typing import TypingProfile, humanized_type
from human.waiting import settle


class FanduelBetPlacer(BetPlacer):
    """Handles bet placement on FanDuel."""

    def __init__(self, page, site, audit_dir):
        super().__init__(page, site, audit_dir)
        # Per-session humanization state. CursorState carries cursor
        # position across calls so successive Bezier paths stitch
        # naturally; TypingProfile rotates daily so typing rhythm is
        # consistent within a day but drifts across days.
        self._cursor = CursorState()
        self._typing = TypingProfile.for_today()

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def navigate_and_expand_market(self, opportunity: Dict, market_config: Dict,
                                   direction: str = None) -> None:
        """Navigate FanDuel to player search results.

        FanDuel ignores ``market_config`` and ``direction`` — its bet
        finder is name+line based, not accordion based like BetMGM.
        """
        self._navigate_fanduel(opportunity)

    def _navigate_fanduel(self, opportunity: Dict) -> None:
        """Navigate FanDuel to player search results — humanized.

        Sequence (matches legacy ``_navigate_fanduel``):
          1. goto /search
          2. clear stale slip (lazy fast-path inside)
          3. re-navigate /search to reset a slip-takeover view that hides
             the search header
          4. find the visible search input
          5. humanized-type the player name; pre-submit dwell; Enter
          6. settle for search results

        Inline modal dismissal is intentionally dropped: the Task 17
        ModalWatcher owns Reality-Check / responsible-gambling modal
        handling. The ``_dismiss_fanduel_modal`` helper is preserved
        below in case the watcher needs to delegate to it.
        """
        player_name = opportunity['player_name']

        print(f"[FANDUEL] Navigating to search...")
        self.page.goto(
            "https://mo.sportsbook.fanduel.com/search",
            wait_until="domcontentloaded",
        )
        settle(self.page, "page_load", rng=self._typing.rng)

        # Clear any leftover bets from a previous failed run. Otherwise the
        # bet-button click on the next step toggles a still-Selected bet OFF
        # and the slip ends up empty when we expect it populated.
        self.clear_betslip()

        # FanDuel's "Remove all selections" click can open the betslip in a
        # full-screen takeover view that hides the search input (URL stays
        # /search but the search header is no longer rendered). Re-navigate
        # to /search to reset to a known DOM regardless of whether the slip
        # was actually dirty — one extra request per FD phase, cheap
        # insurance.
        self.page.goto(
            "https://mo.sportsbook.fanduel.com/search",
            wait_until="domcontentloaded",
        )
        settle(self.page, "page_load", rng=self._typing.rng)

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
        # Focus the input via humanized mouse, then humanized-type the
        # player name. ``humanized_type`` does NOT focus on its own
        # (see human/typing.py docstring), so the explicit mouse_click
        # is load-bearing.
        mouse_click(self.page, search_input, state=self._cursor,
                    rng=self._typing.rng)
        settle(self.page, "micro_pause", rng=self._typing.rng)
        humanized_type(self.page, search_input, player_name,
                       profile=self._typing)
        settle(self.page, "pre_submit_dwell", rng=self._typing.rng)
        self.page.keyboard.press("Enter")
        settle(self.page, "search_results", rng=self._typing.rng)

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

        Preserved as a helper for the Task 17 ModalWatcher to delegate
        to; ``_navigate_fanduel`` no longer calls this inline.

        Idempotent and silent — if no modal is present, return.
        """
        try:
            modal = self.page.locator('div[role="dialog"][aria-modal="true"]')
            if modal.count() == 0:
                return
            if not modal.first.is_visible():
                return
            try:
                t = (modal.first.inner_text() or "").strip()[:120]
            except Exception:
                t = "(no text)"
            print(f"[FANDUEL] Dismissing modal: {t!r}")
            buttons = modal.first.locator("button")
            if buttons.count() > 0:
                mouse_click(self.page, buttons.first, state=self._cursor,
                            rng=self._typing.rng)
                settle(self.page, "modal_dismiss", rng=self._typing.rng)
                print(f"[FANDUEL] Modal dismissed.")
        except Exception as e:
            print(f"[FANDUEL] ⚠ Modal-dismiss probe failed: {e} (continuing)")

    # ------------------------------------------------------------------
    # Slip clearing — lazy fast-path
    # ------------------------------------------------------------------

    def clear_betslip(self) -> None:
        """Empty the FanDuel betslip and fail if it remains non-empty.

        **Lazy fast-path:** the very first action is the cheap
        "Betslip empty" text probe. If FD already exposes that empty
        signal, return immediately — no slip open, no sweep, no
        viewport changes. Stale-bet cleanup only runs when the slip
        actually has items.
        """
        # Fast-path: "Betslip empty" visible → done. Skip the slip open,
        # the Clear-All click, and even the per-bet remove sweep.
        try:
            empty_marker = self.page.get_by_text("Betslip empty", exact=False)
            if empty_marker.count() > 0 and empty_marker.first.is_visible():
                print(f"[FANDUEL] Slip already empty.")
                return
        except Exception:
            # If the probe itself blew up, fall through to the full clear.
            pass

        self._clear_betslip_fanduel()

    def _clear_betslip_fanduel(self) -> None:
        """Empty the FanDuel betslip if it isn't already.

        Best-effort. Stale bets in the slip cause the next bet-button click
        to toggle the SAME bet OFF (the aria-label still says " Selected"),
        leaving the slip empty when we go to enter a wager.

        Does not raise on the best-effort sweep; the post-clear
        verification at the bottom raises if remove controls remain.
        """
        try:
            # Open the slip first; the remove buttons live inside its panel.
            try:
                wins_pattern = self.page.get_by_text(
                    re.compile(r"\$[\d.]+ wins \$[\d.]+", re.I)
                )
                if (wins_pattern.count() > 0
                        and wins_pattern.first.is_visible()):
                    mouse_click(self.page, wins_pattern.first,
                                state=self._cursor, rng=self._typing.rng)
                    settle(self.page, "ui_expansion", rng=self._typing.rng)
            except Exception:
                pass

            # Redundant empty re-check after opening — covers the case
            # where opening the slip itself surfaced the empty marker
            # that the fast-path probe couldn't see before expansion.
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
                        mouse_click(self.page, loc.first, state=self._cursor,
                                    rng=self._typing.rng)
                        settle(self.page, "slip_update",
                               rng=self._typing.rng)
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
                                        mouse_click(self.page, cand,
                                                    state=self._cursor,
                                                    rng=self._typing.rng)
                                        settle(self.page, "slip_update",
                                               rng=self._typing.rng)
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
        # Fallback signal: any remaining remove-selection button is
        # evidence of items still in the slip.
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

    # ------------------------------------------------------------------
    # Slip assertions
    # ------------------------------------------------------------------

    def assert_betslip_has_bet(self) -> None:
        """Assert a selected bet actually reached the slip.

        Used by the selector validation harness. It intentionally stops
        before wager entry or placement.
        """
        if not self._fanduel_slip_has_visible_selection():
            self._screenshot("validation_slip_empty")
            raise BetPlacerError("FanDuel slip is empty after bet click")

    def assert_betslip_empty(self) -> None:
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
            empty_marker = self.page.get_by_text(
                "No bet selections", exact=False
            )
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
                if (empty_marker.count() > 0
                        and empty_marker.first.is_visible()):
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
        """Return True only when FanDuel exposes a concrete slip-selection
        signal."""
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

    # ------------------------------------------------------------------
    # Task 15 stubs
    # ------------------------------------------------------------------

    def find_and_click_bet(self, opportunity, direction, market_config):
        raise NotImplementedError("Task 15")

    def enter_wager(self, amount):
        raise NotImplementedError("Task 15")

    def place_bet(self):
        raise NotImplementedError("Task 15")

    def get_actual_odds(self):
        raise NotImplementedError("Task 15")

    def discover_max_wager(self):
        raise NotImplementedError("Task 15")
