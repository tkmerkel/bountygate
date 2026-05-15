"""
Bet Placement Logic
Site-specific logic for placing bets on FanDuel and BetMGM.
Uses selectors from mapped markets to find and click bet buttons.
"""

import os
import re
from datetime import datetime
from typing import Dict, Tuple, Optional
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from selector_finder import SelectorFinder, is_alternate_market, calculate_alternate_tab_value
from execution_logger import ExecutionLogger
from text_match import fuzzy_score, fuzzy_contains

# Accordion-header fuzzy threshold. Lower than the player-name threshold (90)
# because UI labels can be reworded more than player names ("Player points O/U"
# -> "Player Points Over/Under") without changing semantics.
_ACCORDION_FUZZY_THRESHOLD = 80

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


class BetPlacerError(Exception):
    """Raised when bet placement fails."""
    pass


class BetPlacer:
    """Handles bet placement on sportsbook sites."""

    def __init__(self, page: Page, site: str, audit_dir: str):
        """
        Args:
            page: Playwright page object
            site: Site name ('fanduel' or 'betmgm')
            audit_dir: Directory to save audit screenshots
        """
        self.page = page
        self.site = site
        self.audit_dir = audit_dir
        os.makedirs(audit_dir, exist_ok=True)

    def _screenshot(self, tag: str) -> str:
        """Save screenshot for audit trail."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.audit_dir, f"{self.site}_{tag}_{timestamp}.png")
        try:
            self.page.screenshot(path=filename, full_page=True)
        except Exception as e:
            print(f"⚠ Screenshot failed: {e}")
        return filename

    def navigate_and_expand_market(self, opportunity: Dict, market_config: Dict, direction: str = None):
        """
        Navigate to the event and expand market accordion (BetMGM) or search player (FanDuel).

        Args:
            opportunity: Opportunity dict with player/team info
            market_config: Market configuration from YAML
            direction: 'over' or 'under' (needed for alternate tab selection)

        Raises:
            BetPlacerError: If navigation fails
        """
        if self.site == "betmgm":
            self._navigate_betmgm(opportunity, market_config, direction)
        elif self.site == "fanduel":
            self._navigate_fanduel(opportunity)
        else:
            raise BetPlacerError(f"Unknown site: {self.site}")

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
                # best match above threshold. Tie-break on length: at equal
                # fuzzy score, prefer the SHORTER candidate. partial_ratio
                # returns 100 for any haystack that contains the needle as
                # a substring, so "Player rebounds + assists" and "Player
                # rebounds + assists O/U" both score 100 against either
                # input. Shortest-length tie-break gives us the most exact
                # textual match available.
                best_btn = None
                best_text = None
                best_score = 0
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
                    if score > best_score or (
                        score == best_score
                        and best_text is not None
                        and len(btn_text) < len(best_text)
                    ):
                        best_score = score
                        best_btn = btn
                        best_text = btn_text

                if best_btn is None or best_score < _ACCORDION_FUZZY_THRESHOLD:
                    print(f"[BETMGM] accordion '{accordion_name}' not found. "
                          f"Visible accordions ({len(all_texts)}): {all_texts!r}")
                    raise BetPlacerError(f"Accordion not found: {accordion_name}")

                print(f"[BETMGM] ⚠ Exact accordion miss; best fuzzy match "
                      f"'{best_text}' (score={best_score}) for expected "
                      f"'{accordion_name}'")
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

        Args:
            opportunity: Opportunity dict with line information
            market_config: Market configuration from YAML
            direction: 'over' or 'under'
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

    def find_and_click_bet(self, opportunity: Dict, direction: str, market_config: Dict) -> bool:
        """
        Find and click the bet button for the specified player/line/direction.

        Args:
            opportunity: Opportunity dict
            direction: 'over' or 'under'
            market_config: Market configuration from YAML

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

        print(f"[{self.site.upper()}] Finding bet: {player_name} {direction} {line}")

        # Check if this is an alternate market
        is_alternate = market_config.get('is_alternate', False) or is_alternate_market(market_key)

        # FanDuel MLB markets always use threshold format ("2+ Stolen Bases", "To Hit A Single")
        # even when the market_key doesn't have _alternate suffix (primary table rows)
        if self.site == "fanduel" and (market_key.startswith("batter_") or market_key.startswith("pitcher_")):
            is_alternate = True

        # For FanDuel alternate markets, use threshold-based search
        if self.site == "fanduel" and is_alternate:
            return self._find_and_click_alternate_bet_fanduel(opportunity, direction, market_config, player_name, line)

        # BetMGM: ms-event-pick elements don't contain the player name in
        # their own text — the name is in a sibling element. The generic
        # find_candidates_by_text fails because it requires both player
        # name and line in the same element. Use a structural finder that
        # walks DOM upward from each bet button to find the row.
        if self.site == "betmgm":
            if self._click_betmgm_pick_for_player(player_name, line, direction):
                return True
            # Fall through to diagnostic dump and raise below
            candidates = []
        else:
            # Use SelectorFinder to locate the bet
            display_names = market_config.get('display_names', [market_key])
            candidates = SelectorFinder.find_candidates_by_text(
                self.page, display_names, player_name, line
            )

        if not candidates:
            # Diagnostic: dump aria-labels and visible button text near the
            # player name so the next selector update has data. Cheap, runs
            # only on the failure path. Limited to keep console output sane.
            aria_dump = []
            try:
                aria_loc = self.page.locator(f'[aria-label*="{player_name}"]')
                for i in range(min(aria_loc.count(), 10)):
                    try:
                        aria_dump.append(aria_loc.nth(i).get_attribute("aria-label"))
                    except Exception:
                        continue
                print(f"[{self.site.upper()}] aria-labels mentioning {player_name!r} "
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
                print(f"[{self.site.upper()}] buttons mentioning {player_name!r} "
                      f"({len(btn_dump)}): {btn_dump!r}")
            except Exception:
                pass
            try:
                # BetMGM-specific: list ms-event-pick elements (the bet
                # element type from YAML) near the player row.
                if self.site == "betmgm":
                    pick_loc = self.page.locator("ms-event-pick")
                    pick_count = pick_loc.count()
                    pick_dump = []
                    for i in range(min(pick_count, 20)):
                        try:
                            txt = (pick_loc.nth(i).text_content() or "").strip()[:80]
                            if player_name.lower() in txt.lower():
                                pick_dump.append(txt)
                        except Exception:
                            continue
                    print(f"[BETMGM] ms-event-pick elements mentioning "
                          f"{player_name!r} ({len(pick_dump)}): {pick_dump!r}")
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
            # Fallback: use first candidate
            print(f"⚠ Could not filter by direction, using first candidate")
            selected = candidates[0]
        else:
            selected = direction_candidates[0]

        print(f"[{self.site.upper()}] Clicking bet: {selected.preview_text[:60]}")

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
            print(f"[{self.site.upper()}] Expanding viewport to 1920x945...")
            self.page.set_viewport_size({"width": 1920, "height": 945})
            self.page.wait_for_timeout(500)

            self._screenshot("bet_clicked")
            print(f"[{self.site.upper()}] ✓ Bet added to slip")
            return True
        except Exception as e:
            self._screenshot("click_failed")
            raise BetPlacerError(f"Failed to click bet: {e}")

    def _find_and_click_alternate_bet_fanduel(self, opportunity: Dict, direction: str,
                                              market_config: Dict, player_name: str, line: float) -> bool:
        """Find threshold-based bet on FanDuel for alternate markets.

        For alternate markets, FanDuel shows bets like "4+ Points" instead of "Over 4.5 Points".
        This method searches for the threshold format.

        Args:
            opportunity: Opportunity dict
            direction: 'over' or 'under'
            market_config: Market configuration from YAML
            player_name: Player name to search for
            line: Betting line

        Returns:
            True if bet successfully added to betslip
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

    def enter_wager(self, amount: float) -> bool:
        """
        Enter wager amount in betslip.

        Args:
            amount: Wager amount in dollars

        Returns:
            True if amount entered successfully

        Raises:
            BetPlacerError: If wager input cannot be found or filled
        """
        print(f"[{self.site.upper()}] Entering wager: ${amount:.2f}")

        if self.site == "fanduel":
            return self._enter_wager_fanduel(amount)
        elif self.site == "betmgm":
            return self._enter_wager_betmgm(amount)
        else:
            raise BetPlacerError(f"Unknown site: {self.site}")

    def _click_betmgm_pick_for_player(self, player_name: str, line: float, direction: str) -> bool:
        """Find and click the BetMGM ms-event-pick that matches this player+line+direction.

        BetMGM's player-prop rows lay out as:
            [avatar][player name][stat avg][chart icon][ms-event-pick: "O 11.5  2.00"]

        The bet button (``ms-event-pick``) text contains only the direction
        letter + line + odds (e.g. "O 11.5"). The player name lives in a
        sibling/ancestor element.

        Strategy: enumerate every ms-event-pick on the page, check its text
        matches ``"<O|U> <line>"``, then walk up the DOM and pull the
        innerText of each ancestor row container. The player-name match runs
        Python-side via ``fuzzy_contains`` so apostrophe variants
        (curly vs straight: "De'Aaron" vs "De'Aaron"), abbreviation forms,
        and case differences all resolve cleanly.

        Returns True on a successful click, False if no match found.
        """
        direction_letter = "O" if direction == "over" else "U"
        target_text = f"{direction_letter} {line}"
        target_text_alt = f"{direction_letter} {int(line)}" if float(line).is_integer() else None

        # Best-effort: scroll the page to its bottom to coax virtual-scroll
        # / lazy-load picks into the DOM. Some BetMGM pages keep below-fold
        # picks unrendered until they enter the viewport — we previously
        # missed Fox's Under-3.5 pick this way. Cheap, idempotent.
        try:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            self.page.wait_for_timeout(400)
            self.page.evaluate("window.scrollTo(0, 0)")
            self.page.wait_for_timeout(200)
        except Exception:
            pass

        all_picks = self.page.locator("ms-event-pick")
        pick_count = all_picks.count()
        print(f"[BETMGM] scanning {pick_count} ms-event-pick(s) for "
              f"{target_text!r} on player {player_name!r}")

        # Walk-up returns each ancestor's innerText (with a length cap so we
        # don't pay for serializing the whole document). Python-side, we
        # apply fuzzy_contains across the returned texts — the JS-side
        # `.includes(player)` is character-exact and silently misses curly-
        # vs-straight-apostrophe rows like "De'Aaron Fox".
        walkup_js = """
        (el, args) => {
            const max_depth = args.max_depth;
            const max_text_len = args.max_text_len;
            const out = [];
            let cur = el;
            for (let i = 0; i < max_depth && cur && cur.parentElement; i++) {
                cur = cur.parentElement;
                const text = (cur.innerText || cur.textContent || '').trim();
                if (text.length <= max_text_len) out.push(text);
            }
            return out;
        }
        """

        matched_option_id = None
        matched_handle = None
        matched_meta = None

        for i in range(pick_count):
            try:
                pick = all_picks.nth(i)
                # Deliberately NOT calling pick.is_visible() — the previous
                # impl skipped DOM-attached but not-yet-rendered picks (e.g.
                # below the fold), which is exactly how Fox's Under-3.5
                # disappeared. Playwright's click() will auto-scroll into
                # view, so unrendered-but-attached is fine here.
                txt = (pick.text_content() or "").strip()
                norm = " ".join(txt.split())
                if target_text not in norm and (
                    target_text_alt is None or target_text_alt not in norm
                ):
                    continue
                # Pull ancestor texts; fuzzy-match the player Python-side.
                ancestor_texts = pick.evaluate(
                    walkup_js,
                    {"max_depth": 15, "max_text_len": 600},
                )
                player_found = any(
                    fuzzy_contains(t, player_name, threshold=90)
                    for t in ancestor_texts
                )
                if not player_found:
                    continue
                option_id = pick.get_attribute("data-test-option-id")
                matched_option_id = option_id
                matched_handle = pick
                matched_meta = f"text={norm!r} option_id={option_id!r}"
                break
            except Exception as e:
                print(f"[BETMGM] pick #{i} scan error: {e}")
                continue

        if matched_handle is None:
            print(f"[BETMGM] no ms-event-pick matched {target_text!r} for "
                  f"{player_name!r} (scanned {pick_count})")
            return False

        print(f"[BETMGM] matched bet: {matched_meta}")
        # Prefer clicking via the stable data-test-option-id selector when
        # one is present — the locator handle can go stale across the click's
        # auto-scroll if Angular re-renders the row.
        try:
            if matched_option_id:
                target = self.page.locator(
                    f'ms-event-pick[data-test-option-id="{matched_option_id}"]'
                )
                target.first.click(timeout=10000)
            else:
                matched_handle.click(timeout=10000)
            self.page.wait_for_timeout(1500)
            self._screenshot("bet_clicked")
            print(f"[BETMGM] ✓ Bet added to slip")
            return True
        except Exception as e:
            self._screenshot("click_failed")
            raise BetPlacerError(f"Failed to click BetMGM bet: {e}")

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
                        return
                except Exception:
                    continue

            # 2. Otherwise, click individual remove buttons until empty.
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

    def _clear_betslip_betmgm_precheck(self) -> None:
        """Empty the BetMGM betslip if it isn't already.

        Same rationale as the FanDuel version: stale items leave the
        accordion-click path in an ambiguous state and cause toggle-off
        side-effects. Best-effort, never raises.
        """
        try:
            # BetMGM shows a "Bet slip (N)" pill at the bottom. If N == 0
            # there's nothing to clear and the icon is just "Bet slip".
            try:
                slip_pill = self.page.locator(
                    'text=/^\\s*Bet slip\\s*\\(?\\s*\\d+\\s*\\)?\\s*$/'
                )
                text = ""
                if slip_pill.count() > 0:
                    text = (slip_pill.first.text_content() or "").strip()
                # Match "(0)" -> empty; "(1)" -> has bets.
                m = re.search(r"\((\d+)\)", text)
                if m and int(m.group(1)) == 0:
                    print(f"[BETMGM] Slip already empty.")
                    return
            except Exception:
                pass

            # Open the slip to access remove controls. The pill at the
            # bottom is a non-button element ("0 Bet slip" / "1 Bet slip"
            # in a span/div). Earlier `button:has-text("Bet slip")`
            # selectors matched nothing and the subsequent "Clear all"
            # click hit a wrong button on the page (or no-op'd) while
            # bets accumulated across iterations.
            self._open_betmgm_slip()

            # "Remove all" sweep. In desktop right-rail layout the actual
            # clickable Clear All element is a <span> (not a <button>).
            # Mobile-layout variants render it as a button or div[role=button].
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
                        loc.first.click()
                        self.page.wait_for_timeout(800)
                        # Some BetMGM flows show a confirmation dialog.
                        for confirm_sel in (
                            'button:has-text("Yes")',
                            'button:has-text("Remove")',
                            'button:has-text("Confirm")',
                        ):
                            try:
                                cloc = self.page.locator(confirm_sel)
                                if cloc.count() > 0 and cloc.first.is_visible():
                                    cloc.first.click()
                                    self.page.wait_for_timeout(500)
                                    break
                            except Exception:
                                continue
                        return
                except Exception:
                    continue

            # Otherwise individual remove icons.
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
            print(f"[BETMGM] Slip cleared (best-effort).")
        except Exception as e:
            print(f"[BETMGM] ⚠ Slip clear failed: {e} (continuing).")

        # Post-clear verification: re-read the slip pill. If "(N)" with N > 0
        # remains, the clear didn't take and we should halt before placing.
        try:
            slip_pill = self.page.locator(
                'text=/^\\s*Bet slip\\s*\\(?\\s*\\d+\\s*\\)?\\s*$/'
            )
            if slip_pill.count() > 0:
                text = (slip_pill.first.text_content() or "").strip()
                m = re.search(r"\((\d+)\)", text)
                if m and int(m.group(1)) > 0:
                    raise BetPlacerError(
                        f"BetMGM slip-clear failed: pill still reads {text!r}"
                    )
        except BetPlacerError:
            raise
        except Exception:
            pass

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

    def _enter_wager_betmgm(self, amount: float) -> bool:
        """Enter wager on BetMGM."""
        try:
            # BetMGM docks the slip at the bottom of the screen, collapsed
            # to a "1 Bet slip — $X.XX pays out $Y.YY" pill. The wager
            # input only mounts when the slip is expanded. Click the pill
            # (or any visible "pays out"/"Bet slip" affordance) to expand.
            self._open_betmgm_slip()

            # BetMGM stake input — historically `app-stake-input input`,
            # but the prefix and component name drift. Broaden the cascade
            # to durable signals: inputmode=decimal, aria-label / placeholder
            # patterns, and the data-testid families we've seen.
            wager_selectors = [
                'app-stake-input input',
                'bs-stake-input input',
                'input[inputmode="decimal"]',
                'input[type="number"]',
                'input[aria-label*="stake" i]',
                'input[aria-label*="wager" i]',
                'input[aria-label*="amount" i]',
                'input[placeholder*="stake" i]',
                'input[placeholder*="enter amount" i]',
                '[data-testid*="stake" i] input',
                'input[type="text"]',  # last-resort fallback
            ]

            # When slip-clear fails and multiple bets accumulate, the slip
            # has multiple stake inputs and we must enter the wager into
            # the one for the just-added bet. Heuristic: prefer the LAST
            # visible EMPTY stake input (just-added bets have no value;
            # previously-filled bets retain their value across iterations).
            wager_input = None
            for selector in wager_selectors:
                try:
                    locator = self.page.locator(selector)
                    if locator.count() == 0:
                        continue
                    visible_inputs = []
                    for i in range(locator.count()):
                        elem = locator.nth(i)
                        try:
                            if not elem.is_visible():
                                continue
                            try:
                                value = elem.input_value() or ""
                            except Exception:
                                value = ""
                            visible_inputs.append((elem, value))
                        except Exception:
                            continue
                    if not visible_inputs:
                        continue
                    empty_inputs = [el for el, v in visible_inputs if not v.strip()]
                    if empty_inputs:
                        wager_input = empty_inputs[-1]  # last empty = just-added bet
                        print(f"[BETMGM] Found wager input via {selector} "
                              f"(picked last empty of {len(visible_inputs)})")
                    else:
                        wager_input = visible_inputs[-1][0]  # last filled — best guess
                        print(f"[BETMGM] Found wager input via {selector} "
                              f"(all {len(visible_inputs)} filled; picked last)")
                    break
                except Exception:
                    continue

            if wager_input is None:
                # Diagnostic dump on miss — what visible inputs DO exist?
                try:
                    inputs = self.page.locator("input")
                    dump = []
                    for i in range(min(inputs.count(), 12)):
                        el = inputs.nth(i)
                        try:
                            if not el.is_visible():
                                continue
                            dump.append({
                                "type": el.get_attribute("type"),
                                "inputmode": el.get_attribute("inputmode"),
                                "aria-label": el.get_attribute("aria-label"),
                                "placeholder": el.get_attribute("placeholder"),
                                "data-testid": el.get_attribute("data-testid"),
                            })
                        except Exception:
                            continue
                    print(f"[BETMGM] visible inputs ({len(dump)}): {dump!r}")
                except Exception:
                    pass
                self._screenshot("wager_input_not_found")
                raise BetPlacerError("Could not find BetMGM wager input")

            # Enter the amount via individual keystrokes. BetMGM uses a
            # custom Angular numpad widget; .fill() sets the input value
            # in DOM but doesn't fire the keydown events the widget's
            # form-state machine listens for, so the Place Bet button
            # stays aria-disabled='true'. .pw_type() generates real
            # keystroke events that the widget accepts.
            wager_input.click()
            self.page.wait_for_timeout(200)
            # Clear any existing content first (Ctrl+A, Delete) so the
            # new digits don't get appended.
            self.page.keyboard.press("Control+A")
            self.page.keyboard.press("Delete")
            self.page.wait_for_timeout(200)
            amount_str = f"{amount:.2f}"
            # Use keyboard.type with a small per-char delay so each
            # digit triggers a clean keypress that the numpad widget
            # processes individually.
            self.page.keyboard.type(amount_str, delay=80)
            self.page.wait_for_timeout(500)

            # Press Tab/ArrowDown to blur the input and trigger validation
            # — needed so the Place Bet button transitions from disabled
            # to enabled.
            self.page.keyboard.press("Tab")
            self.page.wait_for_timeout(1000)

            self._screenshot("wager_entered")
            print(f"[BETMGM] ✓ Wager entered: ${amount:.2f}")
            return True

        except Exception as e:
            self._screenshot("wager_entry_failed")
            raise BetPlacerError(f"Failed to enter wager: {e}")

    def _open_betmgm_slip(self) -> None:
        """Click the bottom-docked Bet slip pill to expand the slip panel.

        BetMGM's slip is collapsed by default after a bet is added —
        the wager input doesn't exist in the DOM until the slip
        expands. Tries multiple selectors (text variants seen in
        production: 'pays out', 'to win', plain 'Bet slip').
        Idempotent — if the slip is already open, returns silently.
        """
        try:
            # If a stake input is already visible, slip is already expanded.
            for probe in ('app-stake-input input', 'bs-stake-input input',
                          'input[inputmode="decimal"]'):
                try:
                    loc = self.page.locator(probe)
                    if loc.count() > 0 and loc.first.is_visible():
                        return
                except Exception:
                    continue

            # Click any visible slip-pill affordance.
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
                    # Pick the FIRST visible candidate; bottom-pill is
                    # usually the only visible match.
                    for i in range(min(loc.count(), 5)):
                        cand = loc.nth(i)
                        try:
                            if cand.is_visible():
                                print(f"[BETMGM] Opening slip via {sel}")
                                cand.click()
                                self.page.wait_for_timeout(1500)
                                return
                        except Exception:
                            continue
                except Exception:
                    continue
            print("[BETMGM] ⚠ No slip-pill affordance found; continuing.")
        except Exception as e:
            print(f"[BETMGM] ⚠ Slip-open probe failed: {e} (continuing)")

    def discover_max_wager_fanduel(self) -> Tuple[float, str]:
        """
        Enter large amount on FanDuel to discover max wager limit.

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

    def place_bet(self) -> Tuple[str, str]:
        """
        Click the "Place Bet" button and check for success/failure.

        Returns:
            (status, message) where status is 'ACCEPTED', 'REJECTED', or 'UNKNOWN'

        Raises:
            BetPlacerError: If place bet button cannot be found
        """
        print(f"[{self.site.upper()}] Placing bet...")

        if self.site == "fanduel":
            return self._place_bet_fanduel()
        elif self.site == "betmgm":
            return self._place_bet_betmgm()
        else:
            raise BetPlacerError(f"Unknown site: {self.site}")

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

    def _place_bet_betmgm(self) -> Tuple[str, str]:
        """Place bet on BetMGM."""
        try:
            # BetMGM: button with "Place Bet" text
            place_btn = self.page.get_by_role("button", name=re.compile(r"Place\s+Bet", re.I))

            if place_btn.count() == 0:
                self._screenshot("place_bet_not_found")
                raise BetPlacerError("Place Bet button not found")

            print(f"[BETMGM] Clicking Place Bet...")
            place_btn.first.click()
            self.page.wait_for_timeout(2000)

            # Check for success/failure - poll for result
            for _ in range(10):  # 5 seconds max
                # Check for success: "Your bet has been accepted" in pc-richtext section
                accepted_msg = self.page.get_by_text("Your bet has been accepted")
                if accepted_msg.count() > 0 and accepted_msg.first.is_visible():
                    self._screenshot("bet_placed_success")
                    print(f"[BETMGM] ✓ Bet ACCEPTED")
                    self._close_betslip_betmgm()
                    return "ACCEPTED", "Your bet has been accepted"

                # Alternative success messages
                alt_success = self.page.get_by_text(re.compile(r"Bet Placed|Wager Accepted", re.I))
                if alt_success.count() > 0 and alt_success.first.is_visible():
                    self._screenshot("bet_placed_success")
                    print(f"[BETMGM] ✓ Bet ACCEPTED")
                    self._close_betslip_betmgm()
                    return "ACCEPTED", "Bet placed successfully"

                # Check for error messages
                error_msg = self.page.get_by_text(re.compile(r"limit exceeded|Error|rejected", re.I))
                if error_msg.count() > 0 and error_msg.first.is_visible():
                    msg = error_msg.first.text_content() or "Unknown error"
                    self._screenshot("bet_rejected")
                    print(f"[BETMGM] ✗ Bet REJECTED: {msg}")
                    return "REJECTED", msg

                self.page.wait_for_timeout(500)

            # Unknown state
            self._screenshot("bet_status_unknown")
            print(f"[BETMGM] ? Bet status UNKNOWN")
            return "UNKNOWN", "Could not determine bet status"

        except Exception as e:
            self._screenshot("place_bet_failed")
            raise BetPlacerError(f"Place bet failed: {e}")

    def _close_betslip_betmgm(self):
        """Close the betslip after a successful bet on BetMGM."""
        try:
            # From recording: aria/Close or bs-linear-result-summary button
            close_selectors = [
                'bs-linear-result-summary button',
                '[aria-label="Close"]',
            ]

            for selector in close_selectors:
                try:
                    close_btn = self.page.locator(selector)
                    if close_btn.count() > 0 and close_btn.first.is_visible():
                        print(f"[BETMGM] Closing betslip...")
                        close_btn.first.click()
                        self.page.wait_for_timeout(500)
                        print(f"[BETMGM] ✓ Betslip closed")
                        return
                except Exception:
                    continue

            print(f"[BETMGM] ⚠ Could not find close button, continuing anyway...")
        except Exception as e:
            print(f"[BETMGM] ⚠ Error closing betslip: {e}")

    def get_actual_odds_fanduel(self) -> Optional[float]:
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

    def get_actual_odds_betmgm(self) -> Optional[float]:
        """Extract actual odds from BetMGM betslip.

        BetMGM displays decimal odds in: span.odds-indicator__lite--default

        Returns:
            Decimal odds as float, or None if not found
        """
        try:
            # Primary selector for BetMGM odds
            odds_selectors = [
                'span.odds-indicator__lite--default',
                'span[class*="odds-indicator"]',
                '.odds-indicator',
            ]

            for selector in odds_selectors:
                try:
                    odds_elem = self.page.locator(selector)
                    if odds_elem.count() > 0:
                        text = odds_elem.first.text_content() or ""
                        text = text.strip()

                        # Parse decimal odds (e.g., "1.75")
                        decimal_match = re.search(r'(\d+\.?\d*)', text)
                        if decimal_match:
                            decimal_odds = float(decimal_match.group(1))
                            print(f"[BETMGM] Extracted odds: {decimal_odds:.3f}")
                            return decimal_odds
                except Exception:
                    continue

            print(f"[BETMGM] ⚠ Could not extract odds from betslip")
            return None

        except Exception as e:
            print(f"[BETMGM] ⚠ Error extracting odds: {e}")
            return None

    def check_betmgm_limit_alert(self) -> Tuple[bool, Optional[float]]:
        """Check if BetMGM shows the max limit alert and get adjusted stake.

        When the requested bet exceeds BetMGM's limit, they show an alert:
        "Your requested bet is over the allowed limit. The maximum stake has been adjusted..."

        Returns:
            (limit_hit: bool, adjusted_stake: float or None)
        """
        try:
            # Check for the limit alert message
            alert_selectors = [
                'p.alert-content__message',
                '.alert-content__message',
                'p:has-text("over the allowed limit")',
            ]

            for selector in alert_selectors:
                try:
                    alert_elem = self.page.locator(selector)
                    if alert_elem.count() > 0:
                        alert_text = alert_elem.first.text_content() or ""
                        if "over the allowed limit" in alert_text.lower():
                            print(f"[BETMGM] ⚠ Max limit alert detected!")

                            # Extract the adjusted stake from betslip summary
                            stake_selectors = [
                                'span.betslip-summary-value',
                                '.betslip-summary-value',
                            ]

                            for stake_selector in stake_selectors:
                                stake_elem = self.page.locator(stake_selector).first
                                if stake_elem.count() > 0:
                                    stake_text = stake_elem.text_content() or ""
                                    # Parse "$6.76" format
                                    stake_match = re.search(r'\$?([\d,]+\.?\d*)', stake_text)
                                    if stake_match:
                                        adjusted_stake = float(stake_match.group(1).replace(',', ''))
                                        print(f"[BETMGM] Adjusted stake: ${adjusted_stake:.2f}")
                                        self._screenshot("limit_alert_detected")
                                        return True, adjusted_stake

                            # Alert found but couldn't parse stake
                            print(f"[BETMGM] ⚠ Could not parse adjusted stake")
                            self._screenshot("limit_alert_no_stake")
                            return True, None
                except Exception:
                    continue

            return False, None

        except Exception as e:
            print(f"[BETMGM] ⚠ Error checking limit alert: {e}")
            return False, None

    def _american_to_decimal(self, american_odds: int) -> float:
        """Convert American odds to decimal odds.

        Args:
            american_odds: American odds (e.g., -110, +150)

        Returns:
            Decimal odds (e.g., 1.909, 2.50)
        """
        if american_odds > 0:
            return (american_odds / 100) + 1
        else:
            return (100 / abs(american_odds)) + 1
