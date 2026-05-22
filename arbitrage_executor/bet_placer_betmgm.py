"""BetMGM humanized bet placer.

Rewrite of the legacy bet_placer_betmgm.py against the new
``arbitrage_executor/human/`` primitives. Public interface preserved
(see ``bet_placer.BetPlacer`` ABC).

Task 12 surface: ``__init__``, ``navigate_and_expand_market``,
``clear_betslip``, ``assert_betslip_has_bet``, ``assert_betslip_empty``.
Find/click/wager/place land in Task 13.
"""

import re
from typing import Dict, Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from bet_placer import BetPlacer, BetPlacerError, BetPlacerSkipError
from human.mouse import CursorState, click as mouse_click
from human.navigation import click_through
from human.typing import TypingProfile, humanized_type
from human.waiting import settle


# Slip-pill regex selector — matches "Bet slip", "Bet slip (N)", and
# "N Bet slip" variants the prod page ships. Used three times: lazy
# short-circuit, post-clear verification, and slip-has-bet probe.
_SLIP_PILL_SELECTOR = (
    'text=/^\\s*(?:\\d+\\s+)?Bet slip\\s*(?:\\(\\s*\\d+\\s*\\))?\\s*$/i'
)


def _pill_count(text: str) -> Optional[int]:
    """Parse the slip-pill text (e.g. ``"Bet slip (3)"`` / ``"3 Bet slip"``).
    Returns the parsed count, or None if the text doesn't match either form.
    """
    m = re.search(r"\((\d+)\)|^\s*(\d+)\s+Bet slip", text or "", re.I)
    if not m:
        return None
    return int(m.group(1) or m.group(2))


class BetmgmBetPlacer(BetPlacer):
    """Handles bet placement on BetMGM."""

    def __init__(self, page, site, audit_dir):
        super().__init__(page, site, audit_dir)
        # Per-session humanization state. CursorState carries cursor
        # position across calls so successive Bezier paths stitch
        # naturally; TypingProfile rotates daily so typing rhythm is
        # consistent within a day but drifts across days.
        self._cursor = CursorState()
        self._typing = TypingProfile.for_today()

    # ------------------------------------------------------------------
    # Static helpers — preserved from legacy unchanged (LOGIC.md hard rule)
    # ------------------------------------------------------------------

    @staticmethod
    def _alt_sibling_if_std_missing(accordion_name: str, visible_names):
        """Return the alt-merged sibling name if ``accordion_name`` is a
        " O/U"-suffixed std accordion AND its unsuffixed sibling is in
        ``visible_names``; else return None.

        Used to distinguish a structural-skip case (BetMGM doesn't ship
        the std O/U accordion on this event, but does ship the merged
        alt) from a genuine selector regression (YAML drift). See
        LOGIC.md.
        """
        if not accordion_name.endswith(" O/U"):
            return None
        sibling = accordion_name[: -len(" O/U")]
        target_norm = " ".join(sibling.lower().split())
        for name in visible_names:
            if " ".join((name or "").lower().split()) == target_norm:
                return sibling
        return None

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def navigate_and_expand_market(self, opportunity: Dict, market_config: Dict,
                                   direction: str = None) -> None:
        """Navigate BetMGM to the event and expand the market accordion.

        Humanized: every ``wait_for_timeout`` is replaced by a categorized
        ``settle()``; the team-name search uses ``humanized_type``; the
        event-page navigation goes through ``click_through`` so the bot
        clicks an anchor rather than goto-ing a constructed URL.
        """
        home_team = opportunity['home_team']
        away_team = opportunity['away_team']
        sport = (opportunity.get('sport_title') or '').upper()
        accordion_name = market_config.get('accordion_name', '')
        is_alternate = (
            market_config.get('is_alternate', False)
            or market_config.get('has_threshold_tabs', False)
        )

        # Start on the plain homepage so the slip pill at the bottom is
        # reachable. The betfinder popup overlays a search modal that
        # would race the subsequent slip-clear; clear first, then open
        # search. See legacy notes for the original observation.
        print(f"[BETMGM] Loading homepage... (sport: {sport})")
        self.page.goto(
            "https://www.mo.betmgm.com/en/sports",
            wait_until="domcontentloaded",
        )
        settle(self.page, "page_load", rng=self._typing.rng)

        # Auth probe — fail loud if the session expired.
        try:
            login_link = self.page.locator(
                'a[href*="/login"]:has-text("Log in")'
            )
            if login_link.count() > 0 and login_link.first.is_visible():
                self._screenshot("session_expired")
                raise BetPlacerError(
                    "BetMGM session expired (Log in link visible on homepage)"
                )
        except BetPlacerError:
            raise
        except Exception as e:
            print(f"[BETMGM] ⚠ Auth probe failed: {e} (continuing — assuming logged in)")

        # Clean any leftover bets BEFORE opening the betfinder popup.
        self.clear_betslip()

        # Now open the betfinder popup for the actual team lookup.
        print(f"[BETMGM] Opening betfinder search...")
        self.page.goto(
            "https://www.mo.betmgm.com/en/sports?popup=betfinder",
            wait_until="domcontentloaded",
        )
        settle(self.page, "page_load", rng=self._typing.rng)

        # Search — humanized typing, then Enter (with pre-submit dwell).
        try:
            search_input = self.page.locator(
                'div.cdk-overlay-container input, '
                'input[placeholder*="Search"], '
                'input[placeholder*="Find"]'
            ).first
            search_input.wait_for(state="visible", timeout=10000)
            print(f"[BETMGM] Searching for: {home_team}")
            humanized_type(self.page, search_input, home_team, profile=self._typing)

            if sport == 'MLB':
                # MLB: click autocomplete suggestion (BetMGM shows Futures
                # that interfere with Enter).
                settle(self.page, "search_results", rng=self._typing.rng)
                suggestion_clicked = False
                try:
                    suggestions = self.page.locator(
                        'ms-search-suggestions-list-item'
                    )
                    suggestions.first.wait_for(state="visible", timeout=5000)
                    for i in range(suggestions.count()):
                        item = suggestions.nth(i)
                        item_text = (item.text_content() or "").lower()
                        if (home_team.lower() in item_text
                                and "future" not in item_text):
                            print(
                                f"[BETMGM] Clicking suggestion: "
                                f"{item_text.strip()[:60]}"
                            )
                            mouse_click(self.page, item, state=self._cursor,
                                        rng=self._typing.rng)
                            suggestion_clicked = True
                            settle(self.page, "search_results",
                                   rng=self._typing.rng)
                            break
                except Exception:
                    pass

                if not suggestion_clicked:
                    print(f"[BETMGM] No suggestion found, pressing Enter...")
                    settle(self.page, "pre_submit_dwell", rng=self._typing.rng)
                    self.page.keyboard.press("Enter")
                    settle(self.page, "search_results", rng=self._typing.rng)
            else:
                # NBA/NHL/NFL: standard Enter-based search.
                settle(self.page, "pre_submit_dwell", rng=self._typing.rng)
                self.page.keyboard.press("Enter")
                settle(self.page, "search_results", rng=self._typing.rng)
        except Exception as e:
            raise BetPlacerError(f"Search failed: {e}")

        # Resolve the event link. BetMGM moved to whole-card anchors
        # mid-2026 — scan every `/sports/events/` anchor and pick the one
        # whose text + href slug covers both teams. Then navigate via
        # ``click_through`` so the bot scrolls and clicks rather than
        # goto-ing a constructed URL.
        try:
            current_url = self.page.url.lower()
            home_slug = home_team.lower().replace(" ", "-")
            away_slug = away_team.lower().replace(" ", "-")

            if "/events/" in current_url and (
                home_slug in current_url or away_slug in current_url
            ):
                print(f"[BETMGM] Already on event page: {current_url}")
            else:
                anchors = self.page.locator('a[href*="/sports/events/"]')
                n = anchors.count()
                print(
                    f"[BETMGM] Scanning {n} event anchor(s) for "
                    f"{away_team} @ {home_team}"
                )

                target_href = None
                best_score = 0
                home_l = home_team.lower()
                away_l = away_team.lower()

                for i in range(n):
                    a = anchors.nth(i)
                    try:
                        text = (a.text_content() or "").lower()
                        href = a.get_attribute("href") or ""
                    except Exception:
                        continue
                    if "future" in text:
                        continue
                    score = int(home_l in text) + int(away_l in text)
                    href_l = href.lower()
                    if home_slug in href_l:
                        score += 1
                    if away_slug in href_l:
                        score += 1
                    if score > best_score:
                        best_score = score
                        target_href = href

                if not target_href or best_score < 2:
                    self._screenshot("event_link_not_found")
                    raise BetPlacerError(
                        f"Could not find event link: {away_team} @ "
                        f"{home_team} (scanned {n} anchors, "
                        f"best score={best_score})"
                    )

                # Click-through navigation: scroll + click the anchor
                # rather than goto-ing the constructed URL. On miss,
                # falls back to a direct goto with a loud log.
                fallback_url = (
                    target_href
                    if target_href.startswith("http")
                    else "https://www.mo.betmgm.com" + target_href
                )
                click_through(
                    self.page,
                    start_url=self.page.url,
                    link_selector=f'a[href="{target_href}"]',
                    fallback_url=fallback_url,
                    state=self._cursor,
                    rng=self._typing.rng,
                )

            # Add ?market=PlayerProps to land on the full props view.
            current_url = self.page.url
            if "market=PlayerProps" not in current_url:
                new_url = current_url + (
                    "&" if "?" in current_url else "?"
                ) + "market=PlayerProps"
                print(f"[BETMGM] Navigating to player props: {new_url}")
                self.page.goto(new_url, wait_until="domcontentloaded")
                settle(self.page, "page_load", rng=self._typing.rng)
        except BetPlacerError:
            raise
        except Exception as e:
            self._screenshot("navigation_failed")
            raise BetPlacerError(f"Event navigation failed: {e}")

        # Sub-tab + accordion expansion.
        self._select_market_sub_tab_betmgm(market_config)
        self._expand_accordion_betmgm(accordion_name, is_alternate,
                                      opportunity, market_config, direction)

    def _select_market_sub_tab_betmgm(self, market_config: Dict) -> None:
        """Click a market sub-tab (e.g. 'Combo stats') if configured.

        No-op if the market has no ``sub_tab_label``.
        """
        sub_tab = market_config.get("sub_tab_label")
        if not sub_tab:
            return
        print(f"[BETMGM] Selecting market sub-tab: {sub_tab}")
        for selector in (
            f'div[role="tablist"] button:has-text("{sub_tab}")',
            f'[role="tab"]:has-text("{sub_tab}")',
            f'button:has-text("{sub_tab}")',
        ):
            try:
                loc = self.page.locator(selector)
                if loc.count() > 0 and loc.first.is_visible():
                    mouse_click(self.page, loc.first, state=self._cursor,
                                rng=self._typing.rng)
                    settle(self.page, "ui_expansion", rng=self._typing.rng)
                    print(f"[BETMGM] ✓ Sub-tab '{sub_tab}' selected")
                    self._screenshot("sub_tab_selected")
                    return
            except Exception as e:
                print(f"[BETMGM] Sub-tab selector failed ({selector}): {e}")
                continue
        self._screenshot("sub_tab_not_found")
        raise BetPlacerError(f"Could not find BetMGM sub-tab '{sub_tab}'")

    def _expand_accordion_betmgm(self, accordion_name: str, is_alternate: bool,
                                 opportunity: Dict, market_config: Dict,
                                 direction: str) -> None:
        """Locate the accordion by exact text, expand it, raise SkipError
        when only the merged-alt sibling is visible (LOGIC.md)."""
        try:
            print(f"[BETMGM] Expanding accordion: {accordion_name}")
            # Exact-text match — :has-text would match the alt sibling
            # by substring and silently expand the wrong accordion.
            exact_selector = (
                f'button[dsaccordiontoggle]:text-is("{accordion_name}")'
            )
            accordion = self.page.locator(exact_selector)

            target = None
            if accordion.count() > 0:
                target = accordion.first
            else:
                # No fuzzy fallback — iterate visible accordions with a
                # normalized exact match, then on miss either skip
                # (alt sibling present) or raise loud (real drift).
                need_norm = " ".join((accordion_name or "").lower().split())
                visible_names: list = []
                for btn in self.page.locator('button[dsaccordiontoggle]').all():
                    try:
                        btn_text = (btn.text_content() or "").strip()
                    except Exception:
                        continue
                    if not btn_text:
                        continue
                    visible_names.append(btn_text)
                    if " ".join(btn_text.lower().split()) == need_norm:
                        target = btn
                        break

                if target is None:
                    # Structural skip: std O/U missing, merged-alt sibling
                    # present → SkipError (worker classifies as SKIPPED).
                    alt_sibling = self._alt_sibling_if_std_missing(
                        accordion_name, visible_names
                    )
                    if alt_sibling is not None:
                        raise BetPlacerSkipError(
                            f"BetMGM std accordion {accordion_name!r} "
                            f"not on this event; merged-alt accordion "
                            f"{alt_sibling!r} is present but Yes-only, "
                            f"so a std×std arb is unbettable here. "
                            f"Skipping (see LOGIC.md)."
                        )
                    self._screenshot("accordion_not_found")
                    raise BetPlacerError(
                        f"BetMGM accordion not found: {accordion_name!r}. "
                        f"Visible ({len(visible_names)}): "
                        f"{sorted(set(visible_names))!r}. "
                        f"Update selectors/betmgm_markets.yaml to match one "
                        f"of these."
                    )

            # Accordion buttons toggle — skip the click if already expanded.
            try:
                already_expanded = (
                    target.get_attribute("aria-expanded") == "true"
                )
            except Exception:
                already_expanded = False
            if already_expanded:
                print(f"[BETMGM] Accordion already expanded; skipping click.")
            else:
                mouse_click(self.page, target, state=self._cursor,
                            rng=self._typing.rng)
                settle(self.page, "ui_expansion", rng=self._typing.rng)

            # Fast-fail: wait for at least one ms-event-pick row.
            try:
                self.page.wait_for_selector(
                    "ms-event-pick", timeout=5000, state="visible",
                )
            except PlaywrightTimeoutError as e:
                self._screenshot("betmgm_accordion_empty")
                raise BetPlacerError(
                    "BetMGM accordion expanded but no ms-event-pick rows "
                    "in 5s — likely wrong sub-tab, market not offered, or "
                    "stuck search overlay"
                ) from e

            # Click "Show More" until all players visible.
            show_more_selector = (
                'ms-option-panel-bottom-action:has-text("Show More")'
            )
            attempts = 0
            while attempts < 5:
                show_more = self.page.locator(show_more_selector)
                if show_more.count() == 0:
                    break
                mouse_click(self.page, show_more.first, state=self._cursor,
                            rng=self._typing.rng)
                settle(self.page, "ui_expansion", rng=self._typing.rng)
                attempts += 1

            print(f"[BETMGM] ✓ Market expanded")
            self._screenshot("market_expanded")

            # Alt markets: select the threshold tab.
            if is_alternate and direction:
                self._select_alternate_tab_betmgm(opportunity, market_config,
                                                  direction)
        except BetPlacerSkipError:
            raise
        except BetPlacerError:
            raise
        except Exception as e:
            self._screenshot("accordion_expansion_failed")
            raise BetPlacerError(f"Accordion expansion failed: {e}")

    def _select_alternate_tab_betmgm(self, opportunity: Dict,
                                     market_config: Dict,
                                     direction: str) -> None:
        """Select the alt threshold tab (e.g. ``5+``) matching the line.
        Best-effort — logs and continues on miss; the find/click path is
        what ultimately raises if the wrong picks rendered.
        """
        from selector_finder import calculate_alternate_tab_value
        line = (opportunity.get('over_line') if direction == 'over'
                else opportunity.get('under_line'))
        if line is None:
            print(f"[BETMGM] ⚠ No line for direction {direction}; skip tab")
            return
        tab_value = calculate_alternate_tab_value(line)
        tab_text = f"{tab_value}+"
        print(f"[BETMGM] Selecting alternate tab: {tab_text} (line: {line})")

        tab_selectors = [
            f'button:has-text("{tab_text}")',
            f'[role="tab"]:has-text("{tab_text}")',
            f'div[role="tablist"] button:has-text("{tab_text}")',
        ]
        pattern = market_config.get('tab_selector_pattern')
        if pattern:
            tab_selectors.append(pattern.format(threshold=tab_value))

        for selector in tab_selectors:
            try:
                tab = self.page.locator(selector)
                if tab.count() > 0 and tab.first.is_visible():
                    print(f"[BETMGM] Found tab using: {selector}")
                    mouse_click(self.page, tab.first, state=self._cursor,
                                rng=self._typing.rng)
                    settle(self.page, "ui_expansion", rng=self._typing.rng)
                    self._screenshot("alternate_tab_selected")
                    # Re-expand the player list after tab switch.
                    show_more_selector = (
                        'ms-option-panel-bottom-action:has-text("Show More")'
                    )
                    attempts = 0
                    while attempts < 5:
                        show_more = self.page.locator(show_more_selector)
                        if show_more.count() == 0:
                            break
                        try:
                            if not show_more.first.is_visible():
                                break
                            mouse_click(self.page, show_more.first,
                                        state=self._cursor,
                                        rng=self._typing.rng)
                            settle(self.page, "ui_expansion",
                                   rng=self._typing.rng)
                        except Exception:
                            break
                        attempts += 1
                    return
            except Exception as e:
                print(f"[BETMGM] Tab selector failed: {selector} - {e}")
                continue
        print(f"[BETMGM] ⚠ Could not find tab '{tab_text}'; continuing")
        self._screenshot("alternate_tab_not_found")

    # ------------------------------------------------------------------
    # Slip clearing — lazy fast-path
    # ------------------------------------------------------------------

    def clear_betslip(self) -> None:
        """Empty the BetMGM betslip; fail if it remains non-empty.

        **Lazy fast-path:** the very first action is the cheap pill read.
        If the pill reads ``(0)`` / ``0 Bet slip``, return immediately —
        no slip open, no sweep, no extra settle. Stale-bet cleanup only
        runs when the pill says we actually have items to clear.
        """
        # Fast-path: pill==0 → done.
        try:
            slip_pill = self.page.locator(_SLIP_PILL_SELECTOR)
            if slip_pill.count() > 0:
                pill_text = (slip_pill.first.text_content() or "").strip()
                count = _pill_count(pill_text)
                if count == 0:
                    print(f"[BETMGM] Slip already empty.")
                    return
        except Exception:
            # If the probe blew up, fall through to the full clear path.
            pass

        # Full clear dance — slip is (probably) non-empty.
        try:
            self._open_betmgm_slip()

            clicked_clear_all = False
            for sel in (
                'span:has-text("Clear All")',
                'button:has-text("Clear All")',
                'button:has-text("Remove all")',
                'button:has-text("Clear all")',
                'button[aria-label*="remove all" i]',
                'div[role="button"]:has-text("Clear All")',
                '[role="button"]:has-text("Clear All")',
            ):
                try:
                    loc = self.page.locator(sel)
                    if loc.count() > 0 and loc.first.is_visible():
                        print(f"[BETMGM] Clearing slip via {sel}")
                        mouse_click(self.page, loc.first, state=self._cursor,
                                    rng=self._typing.rng)
                        settle(self.page, "slip_update",
                               rng=self._typing.rng)
                        # Confirm-dialog dismissal — some BetMGM flows
                        # gate Clear All behind a Yes/Remove/Confirm.
                        for confirm_sel in (
                            'button:has-text("Yes")',
                            'button:has-text("Remove")',
                            'button:has-text("Confirm")',
                        ):
                            try:
                                cloc = self.page.locator(confirm_sel)
                                if (cloc.count() > 0
                                        and cloc.first.is_visible()):
                                    mouse_click(self.page, cloc.first,
                                                state=self._cursor,
                                                rng=self._typing.rng)
                                    settle(self.page, "modal_dismiss",
                                           rng=self._typing.rng)
                                    break
                            except Exception:
                                continue
                        clicked_clear_all = True
                        break
                except Exception:
                    continue

            # Per-bet remove sweep if Clear All wasn't available.
            if not clicked_clear_all:
                for _ in range(10):
                    removed = False
                    for sel in (
                        'bs-bet-slip-item button[aria-label*="remove" i]',
                        'button[aria-label*="remove" i]',
                        'button[aria-label*="delete" i]',
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
            print(f"[BETMGM] Slip cleared (best-effort).")
        except Exception as e:
            print(f"[BETMGM] ⚠ Slip clear failed: {e} (continuing).")

        # Post-clear verification: pill must read 0 (or be absent).
        try:
            slip_pill = self.page.locator(_SLIP_PILL_SELECTOR)
            if slip_pill.count() > 0:
                pill_text = (slip_pill.first.text_content() or "").strip()
                count = _pill_count(pill_text)
                if count is not None and count > 0:
                    raise BetPlacerError(
                        f"BetMGM slip-clear failed: pill still reads "
                        f"{pill_text!r}"
                    )
        except BetPlacerError:
            raise
        except Exception:
            pass

    def _open_betmgm_slip(self) -> None:
        """Click the bottom slip pill to expand the slip panel.

        Idempotent — if a stake input is already visible, return.
        """
        try:
            for probe in (
                'app-stake-input input',
                'bs-stake-input input',
                'input[inputmode="decimal"]',
            ):
                try:
                    loc = self.page.locator(probe)
                    if loc.count() > 0 and loc.first.is_visible():
                        return
                except Exception:
                    continue

            for sel in (
                'div:has-text("pays out")',
                'span:has-text("pays out")',
                '*[role="button"]:has-text("Bet slip")',
                'button:has-text("Bet slip")',
                'a:has-text("Bet slip")',
                'span:has-text("Bet slip")',
            ):
                try:
                    loc = self.page.locator(sel)
                    if loc.count() == 0:
                        continue
                    for i in range(min(loc.count(), 5)):
                        cand = loc.nth(i)
                        try:
                            if cand.is_visible():
                                print(f"[BETMGM] Opening slip via {sel}")
                                mouse_click(self.page, cand,
                                            state=self._cursor,
                                            rng=self._typing.rng)
                                settle(self.page, "ui_expansion",
                                       rng=self._typing.rng)
                                return
                        except Exception:
                            continue
                except Exception:
                    continue
            print("[BETMGM] ⚠ No slip-pill affordance found; continuing.")
        except Exception as e:
            print(f"[BETMGM] ⚠ Slip-open probe failed: {e} (continuing)")

    # ------------------------------------------------------------------
    # Slip assertions
    # ------------------------------------------------------------------

    def assert_betslip_has_bet(self) -> None:
        """Assert a selected bet actually reached the slip."""
        self._open_betmgm_slip()
        if not self._betmgm_slip_has_bet():
            self._screenshot("validation_slip_empty")
            raise BetPlacerError("BetMGM slip is empty after bet click")

    def assert_betslip_empty(self) -> None:
        """Assert the slip is empty after cleanup."""
        self._open_betmgm_slip()
        if self._betmgm_slip_has_bet():
            self._screenshot("validation_slip_not_empty")
            raise BetPlacerError("BetMGM slip still appears to contain a bet")

    def _betmgm_slip_has_bet(self) -> bool:
        """Return True if BetMGM's slip appears to contain at least one bet.
        Conservative on ambiguous states.
        """
        try:
            for text in ("No bet selections", "Betslip empty"):
                empty_marker = self.page.get_by_text(text, exact=False)
                if (empty_marker.count() > 0
                        and empty_marker.first.is_visible()):
                    return False
        except Exception:
            pass
        try:
            slip_pill = self.page.locator(_SLIP_PILL_SELECTOR)
            if slip_pill.count() > 0:
                pill_text = (slip_pill.first.text_content() or "").strip()
                count = _pill_count(pill_text)
                if count is not None:
                    return count > 0
        except Exception:
            pass
        try:
            for sel in (
                'bs-bet-slip-item',
                'bs-betslip-item',
                'button[aria-label*="remove" i]',
                'button[aria-label*="delete" i]',
                'span:has-text("Clear All")',
            ):
                loc = self.page.locator(sel)
                for i in range(min(loc.count(), 5)):
                    try:
                        if loc.nth(i).is_visible():
                            return True
                    except Exception:
                        continue
        except Exception:
            pass
        return False

    # ------------------------------------------------------------------
    # Task 13 surface — implemented in the next task
    # ------------------------------------------------------------------

    def find_and_click_bet(self, opportunity, direction, market_config):
        raise NotImplementedError("Task 13")

    def enter_wager(self, amount):
        raise NotImplementedError("Task 13")

    def place_bet(self):
        raise NotImplementedError("Task 13")

    def get_actual_odds(self):
        raise NotImplementedError("Task 13")

    def check_limit_alert(self):
        raise NotImplementedError("Task 13")
