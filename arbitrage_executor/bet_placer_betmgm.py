"""BetMGM-specific bet placement implementation."""

import os
import re
from datetime import datetime
from typing import Dict, Tuple, Optional

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from selector_finder import SelectorFinder, is_alternate_market, calculate_alternate_tab_value
from execution_logger import ExecutionLogger
from text_match import fuzzy_score, fuzzy_contains
from bet_placer import BetPlacer, BetPlacerError
from _bet_placer_helpers import _ACCORDION_FUZZY_THRESHOLD


class BetmgmBetPlacer(BetPlacer):
    """Handles bet placement on BetMGM."""

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        """Navigate BetMGM to event and expand the market accordion."""
        self._navigate_betmgm(opportunity, market_config, direction)

    def _navigate_betmgm(self, opportunity: Dict, market_config: Dict, direction: str = None):
        """Navigate BetMGM to event and expand market accordion."""
        home_team = opportunity['home_team']
        away_team = opportunity['away_team']
        sport = (opportunity.get('sport_title') or '').upper()
        accordion_name = market_config.get('accordion_name', '')
        is_alternate = market_config.get('is_alternate', False) or market_config.get('has_threshold_tabs', False)

        print(f"[BETMGM] Navigating to event... (sport: {sport})")
        self.page.goto("https://www.mo.betmgm.com/en/sports?popup=betfinder", wait_until="domcontentloaded")
        self.page.wait_for_timeout(2000)

        # Clear any leftover bets from a previous failed run. The slip
        # icon at the bottom shows "(N)" when items are queued — those
        # must come out before our click adds a new one cleanly.
        self._clear_betslip_betmgm_precheck()

        # Search for team — MLB needs autocomplete suggestion click, others use Enter
        try:
            search_input = self.page.locator(
                'div.cdk-overlay-container input, '
                'input[placeholder*="Search"], '
                'input[placeholder*="Find"]'
            ).first
            search_input.wait_for(state="visible", timeout=10000)
            print(f"[BETMGM] Searching for: {home_team}")
            search_input.fill(home_team)

            if sport == 'MLB':
                # MLB: click autocomplete suggestion (BetMGM shows Futures that interfere with Enter)
                self.page.wait_for_timeout(2000)
                suggestion_clicked = False
                try:
                    suggestions = self.page.locator('ms-search-suggestions-list-item')
                    suggestions.first.wait_for(state="visible", timeout=5000)
                    for i in range(suggestions.count()):
                        item = suggestions.nth(i)
                        item_text = (item.text_content() or "").lower()
                        if home_team.lower() in item_text and "future" not in item_text:
                            print(f"[BETMGM] Clicking search suggestion: {item_text.strip()[:60]}")
                            item.click()
                            suggestion_clicked = True
                            self.page.wait_for_timeout(3000)
                            break
                except Exception:
                    pass

                if not suggestion_clicked:
                    print(f"[BETMGM] No suggestion found, pressing Enter...")
                    search_input.press("Enter")
                    self.page.wait_for_timeout(3000)
            else:
                # NBA/NHL/NFL: standard Enter-based search
                self.page.keyboard.press("Enter")
                self.page.wait_for_timeout(3000)

        except Exception as e:
            raise BetPlacerError(f"Search failed: {e}")

        # Find and click "All Wagers" in the correct event card
        # Prefer cards with BOTH team names (skips Futures which only have one team)
        try:
            clicked = False

            # Check if we already landed on the event page (common for MLB suggestions)
            current_url = self.page.url.lower()
            home_slug = home_team.lower().replace(" ", "-")
            away_slug = away_team.lower().replace(" ", "-")
            if "/events/" in current_url and (home_slug in current_url or away_slug in current_url):
                print(f"[BETMGM] Already on event page: {current_url}")
                clicked = True

            if not clicked:
                # Try multiple selectors for result cards (search result card or standard event card)
                result_cards = self.page.locator('ms-grid-search-result-card, ms-event, ms-event-card, .event-card')
                card_count = result_cards.count()
                print(f"[BETMGM] Found {card_count} result card(s)")

                # Score cards: 2 = both teams, 1 = one team, 0 = neither
                # Also skip cards containing "futures" text
                scored_cards = []
                for i in range(card_count):
                    card = result_cards.nth(i)
                    card_text = (card.text_content() or "").lower()

                    if "future" in card_text:
                        print(f"[BETMGM] Skipping futures card #{i+1}")
                        continue

                    has_home = home_team.lower() in card_text
                    has_away = away_team.lower() in card_text
                    score = int(has_home) + int(has_away)
                    if score > 0:
                        scored_cards.append((score, i, card))

                # Sort by score descending — prefer cards with both teams
                scored_cards.sort(key=lambda x: x[0], reverse=True)

                for score, idx, card in scored_cards:
                    print(f"[BETMGM] Trying event card #{idx+1} (score={score})...")
                    # Try multiple selectors for "All Wagers" link
                    all_wagers_selectors = [
                        'span.title:has-text("All Wagers")',
                        'ms-event-footer span:has-text("All Wagers")',
                        'ms-event-footer > div > span',
                        'span:has-text("All Wagers")',
                        'ms-event-footer a', # Fallback to any link in footer
                    ]
                    for aw_selector in all_wagers_selectors:
                        all_wagers = card.locator(aw_selector)
                        if all_wagers.count() > 0:
                            print(f"[BETMGM] Found 'All Wagers' using: {aw_selector}")
                            all_wagers.first.click()
                            self.page.wait_for_timeout(2000)
                            clicked = True
                            break
                    if clicked:
                        break

            # Fallback: try clicking "All Wagers" directly in the overlay (betfinder modal)
            if not clicked:
                print(f"[BETMGM] Card-scoped search failed, trying overlay-scoped fallback...")
                overlay_selectors = [
                    'div.cdk-overlay-container ms-event-footer > div > span',
                    'div.cdk-overlay-container ms-event-footer span:has-text("All Wagers")',
                    'div.cdk-overlay-container span:has-text("All Wagers")',
                ]
                for ov_selector in overlay_selectors:
                    try:
                        ov_loc = self.page.locator(ov_selector)
                        if ov_loc.count() > 0 and ov_loc.first.is_visible():
                            print(f"[BETMGM] Found 'All Wagers' in overlay using: {ov_selector}")
                            ov_loc.first.click()
                            self.page.wait_for_timeout(2000)
                            clicked = True
                            break
                    except Exception:
                        continue

            if not clicked:
                self._screenshot("all_wagers_not_found")
                raise BetPlacerError(f"Could not find event: {away_team} @ {home_team}")

            # Navigate to full player props page
            current_url = self.page.url
            if "market=PlayerProps" not in current_url:
                new_url = current_url + ("&" if "?" in current_url else "?") + "market=PlayerProps"
                print(f"[BETMGM] Navigating to player props: {new_url}")
                self.page.goto(new_url, wait_until="domcontentloaded")
                self.page.wait_for_timeout(2000)

        except Exception as e:
            self._screenshot("navigation_failed")
            raise BetPlacerError(f"Event navigation failed: {e}")

        # Click market sub-tab first (e.g. "Combo stats" for player_points_rebounds)
        # — the sub-tab determines which accordion is visible at all.
        self._select_market_sub_tab_betmgm(market_config)

        # Expand accordion
        try:
            print(f"[BETMGM] Expanding accordion: {accordion_name}")
            # Use :text-is for an EXACT match — BetMGM ships sibling
            # accordions like "Player rebounds + assists" and "Player
            # rebounds + assists O/U" at the same level. :has-text would
            # match both via substring and pick whichever came first in
            # DOM order, often expanding the wrong market silently.
            exact_selector = (
                f'button[dsaccordiontoggle]:text-is("{accordion_name}")'
            )
            accordion = self.page.locator(exact_selector)

            target = None
            if accordion.count() > 0:
                target = accordion.first
            else:
                # Fuzzy fallback: score every accordion button and pick the
                # best match above threshold. partial_ratio returns 100 for
                # any substring containment, so e.g. "Player rebounds" and
                # "Player rebounds O/U" both score 100 against
                # "Player rebounds O/U". Break ties on a *match-quality*
                # rank that prefers (1) an exact normalized match, then
                # (2) the candidate that *contains* the needle (more
                # specific label), over (3) a candidate the needle
                # contains (less specific label). Old code preferred the
                # shorter text, which silently picked the alternate
                # "Player rebounds" accordion over the standard
                # "Player rebounds O/U" — landed on the wrong tab strip
                # and the bet was never findable.
                need_norm = " ".join((accordion_name or "").lower().split())
                best_btn = None
                best_text = None
                best_score = 0
                best_quality = -1
                all_texts = []
                for btn in self.page.locator('button[dsaccordiontoggle]').all():
                    try:
                        btn_text = (btn.text_content() or "").strip()
                    except Exception:
                        continue
                    if not btn_text:
                        continue
                    all_texts.append(btn_text)
                    score = fuzzy_score(btn_text, accordion_name)
                    btn_norm = " ".join(btn_text.lower().split())
                    if btn_norm == need_norm:
                        quality = 2  # exact match (post-normalize)
                    elif need_norm and need_norm in btn_norm:
                        quality = 1  # btn contains needle (more specific)
                    else:
                        quality = 0  # needle contains btn (less specific) / partial
                    if (score, quality) > (best_score, best_quality):
                        best_score = score
                        best_quality = quality
                        best_btn = btn
                        best_text = btn_text

                if best_btn is None or best_score < _ACCORDION_FUZZY_THRESHOLD:
                    print(f"[BETMGM] accordion '{accordion_name}' not found. "
                          f"Visible accordions ({len(all_texts)}): {all_texts!r}")
                    raise BetPlacerError(f"Accordion not found: {accordion_name}")

                print(f"[BETMGM] ⚠ Exact accordion miss; best fuzzy match "
                      f"'{best_text}' (score={best_score}, quality={best_quality}) "
                      f"for expected '{accordion_name}'")
                target = best_btn

            # Accordion buttons are toggles — if a prior session left it
            # expanded, clicking again COLLAPSES. Check aria-expanded and
            # skip the click in that case.
            try:
                already_expanded = (target.get_attribute("aria-expanded") == "true")
            except Exception:
                already_expanded = False
            if already_expanded:
                print(f"[BETMGM] Accordion already expanded; skipping click.")
            else:
                target.click()

            # Fast-fail: wait up to 5s for at least one ms-event-pick row.
            # Replaces a fixed 1.5s sleep that previously let stuck-search /
            # wrong-sub-tab cases stall ~67s downstream before surfacing.
            try:
                self.page.wait_for_selector("ms-event-pick", timeout=5000, state="visible")
            except PlaywrightTimeoutError as e:
                self._screenshot("betmgm_accordion_empty")
                raise BetPlacerError(
                    "BetMGM accordion expanded but no ms-event-pick rows in 5s "
                    "— likely wrong sub-tab, market not offered, or stuck search overlay"
                ) from e

            # Click "Show More" until all players visible
            show_more_selector = 'ms-option-panel-bottom-action:has-text("Show More")'
            attempts = 0
            while attempts < 5:
                show_more = self.page.locator(show_more_selector)
                if show_more.count() == 0:
                    break
                show_more.first.click()
                self.page.wait_for_timeout(1000)
                attempts += 1

            print(f"[BETMGM] ✓ Market expanded")
            self._screenshot("market_expanded")

            # For alternate markets, select the threshold tab
            if is_alternate and direction:
                self._select_alternate_tab_betmgm(opportunity, market_config, direction)

        except Exception as e:
            self._screenshot("accordion_expansion_failed")
            raise BetPlacerError(f"Accordion expansion failed: {e}")

    def _select_market_sub_tab_betmgm(self, market_config: Dict) -> None:
        """Click a market sub-tab (e.g. 'Combo stats') if configured.

        For non-default BetMGM markets like player_points_rebounds, the
        correct accordion lives under a sub-tab that isn't selected by
        default. No-op if the market has no `sub_tab_label`.
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
                    loc.first.click()
                    self.page.wait_for_timeout(800)
                    print(f"[BETMGM] ✓ Sub-tab '{sub_tab}' selected via {selector}")
                    self._screenshot("sub_tab_selected")
                    return
            except Exception as e:
                print(f"[BETMGM] Sub-tab selector failed ({selector}): {e}")
                continue

        self._screenshot("sub_tab_not_found")
        raise BetPlacerError(f"Could not find BetMGM sub-tab '{sub_tab}'")

    def _select_alternate_tab_betmgm(self, opportunity: Dict, market_config: Dict, direction: str):
        """Select threshold tab in BetMGM alternate accordion.

        For alternate markets, BetMGM shows tabs like "5+", "7+", "9+" for different thresholds.
        This method selects the correct tab based on the betting line.
        """
        line = opportunity.get('over_line') if direction == 'over' else opportunity.get('under_line')
        if line is None:
            print(f"[BETMGM] ⚠ No line found for direction {direction}, skipping tab selection")
            return

        tab_value = calculate_alternate_tab_value(line)
        tab_text = f"{tab_value}+"

        print(f"[BETMGM] Selecting alternate tab: {tab_text} (line: {line})")

        # Try multiple tab selector patterns
        tab_selectors = [
            f'button:has-text("{tab_text}")',
            f'[role="tab"]:has-text("{tab_text}")',
            f'div[role="tablist"] button:has-text("{tab_text}")',
            market_config.get('tab_selector_pattern', '').format(threshold=tab_value) if market_config.get('tab_selector_pattern') else None,
        ]

        for selector in tab_selectors:
            if not selector:
                continue
            try:
                tab = self.page.locator(selector)
                if tab.count() > 0 and tab.first.is_visible():
                    print(f"[BETMGM] Found tab using: {selector}")
                    tab.first.click()
                    self.page.wait_for_timeout(1000)
                    print(f"[BETMGM] ✓ Tab {tab_text} selected")
                    self._screenshot("alternate_tab_selected")
                    return
            except Exception as e:
                print(f"[BETMGM] Tab selector failed: {selector} - {e}")
                continue

        # If we couldn't find the tab, log warning but continue
        print(f"[BETMGM] ⚠ Could not find tab '{tab_text}', continuing without tab selection")
        self._screenshot("alternate_tab_not_found")

    def clear_betslip(self):
        raise NotImplementedError("migrated in Task C3")

    def assert_betslip_has_bet(self):
        raise NotImplementedError("migrated in Task C4")

    def assert_betslip_empty(self):
        raise NotImplementedError("migrated in Task C4")

    def find_and_click_bet(self, opportunity, direction, market_config):
        raise NotImplementedError("migrated in Task C5")

    def enter_wager(self, amount):
        raise NotImplementedError("migrated in Task C6")

    def place_bet(self):
        raise NotImplementedError("migrated in Task C7")

    def get_actual_odds(self):
        raise NotImplementedError("migrated in Task C8")

    def check_limit_alert(self):
        raise NotImplementedError("migrated in Task C7")
