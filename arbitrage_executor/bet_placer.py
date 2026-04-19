"""
Bet Placement Logic
Site-specific logic for placing bets on FanDuel and BetMGM.
Uses selectors from mapped markets to find and click bet buttons.
"""

import os
import re
from datetime import datetime
from typing import Dict, Tuple, Optional
from playwright.sync_api import Page

from selector_finder import SelectorFinder, is_alternate_market, calculate_alternate_tab_value
from execution_logger import ExecutionLogger

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

        # Find search input
        try:
            search_input = self.page.locator('input[placeholder="Search"], div.aq input').first
            search_input.wait_for(state="visible", timeout=15000)
        except Exception:
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

        # Expand accordion
        try:
            print(f"[BETMGM] Expanding accordion: {accordion_name}")
            accordion_selector = f'button[dsaccordiontoggle]:has-text("{accordion_name}")'
            accordion = self.page.locator(accordion_selector)

            if accordion.count() == 0:
                raise BetPlacerError(f"Accordion not found: {accordion_name}")

            accordion.first.click()
            self.page.wait_for_timeout(1500)

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

        # Use SelectorFinder to locate the bet
        display_names = market_config.get('display_names', [market_key])
        candidates = SelectorFinder.find_candidates_by_text(
            self.page, display_names, player_name, line
        )

        if not candidates:
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
            locator = self.page.locator(selected.selector).first
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

        # Standard N+ patterns (always included as fallback, or primary for threshold >= 2)
        # IMPORTANT: Include market display name (base_display) to avoid matching wrong market type
        selector_patterns.extend([
            # Most specific: player + threshold + market type (e.g., "6+ Assists")
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
                            elem.click(timeout=10000)
                            self.page.wait_for_timeout(1500)

                            # Expand viewport for betslip interaction
                            print(f"[FANDUEL] Expanding viewport to 1920x945...")
                            self.page.set_viewport_size({"width": 1920, "height": 945})
                            self.page.wait_for_timeout(500)

                            self._screenshot("alternate_bet_clicked")
                            print(f"[FANDUEL] ✓ Alternate bet added to slip")
                            return True
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

    def _enter_wager_fanduel(self, amount: float) -> bool:
        """Enter wager on FanDuel."""
        try:
            # First, click on betslip to open/focus it (from recording step)
            # This is the "$10 wins $14.80" area that opens the betslip panel
            # The class changes between recordings (was jo, now hk) so we try multiple
            betslip_panel_selectors = [
                'div.ay > div > div > div > div.hk > div',  # Latest from recording
                'div.ay > div > div > div > div.jo > div',  # Older recording
            ]

            betslip_opened = False
            for selector in betslip_panel_selectors:
                try:
                    panel = self.page.locator(selector).first
                    if panel.count() > 0 and panel.is_visible():
                        print(f"[FANDUEL] Clicking betslip to open using: {selector}")
                        panel.click()
                        self.page.wait_for_timeout(1000)
                        betslip_opened = True
                        break
                except Exception:
                    continue

            # Try aria-based selector if CSS selectors failed (matches "$X wins $Y" pattern)
            if not betslip_opened:
                try:
                    wins_pattern = self.page.get_by_text(re.compile(r'\$[\d.]+ wins \$[\d.]+', re.I))
                    if wins_pattern.count() > 0:
                        print(f"[FANDUEL] Clicking betslip using wins pattern...")
                        wins_pattern.first.click()
                        self.page.wait_for_timeout(1000)
                        betslip_opened = True
                except Exception:
                    pass

            if not betslip_opened:
                print(f"[FANDUEL] ⚠ Could not click betslip panel, continuing anyway...")

            # Take screenshot after betslip interaction
            self._screenshot("betslip_opened")

            # Now find wager input
            # From DevTools recording: aria/WAGER $ is the accessible name
            wager_input = None

            # Method 1: Use Playwright's get_by_label (matches accessible name)
            try:
                aria_input = self.page.get_by_label("WAGER $")
                if aria_input.count() > 0 and aria_input.first.is_visible():
                    wager_input = aria_input.first
                    print(f"[FANDUEL] Found wager input using get_by_label")
            except Exception:
                pass

            # Method 2: Try partial label match
            if wager_input is None:
                try:
                    aria_input = self.page.get_by_label(re.compile(r"WAGER", re.I))
                    if aria_input.count() > 0 and aria_input.first.is_visible():
                        wager_input = aria_input.first
                        print(f"[FANDUEL] Found wager input using partial label")
                except Exception:
                    pass

            # Method 3: CSS selector inside #main (betslip area, not header search)
            if wager_input is None:
                wager_selectors = [
                    '#main div > div > div.bt input',  # Latest from recording, scoped to #main
                    '#main div > div > div.cg input',  # Older fallback
                    '#main input[type="text"]',  # Generic input in main
                ]

                for selector in wager_selectors:
                    try:
                        locator = self.page.locator(selector)
                        if locator.count() > 0:
                            # Find visible input that's NOT the search input
                            for i in range(locator.count()):
                                elem = locator.nth(i)
                                try:
                                    if not elem.is_visible():
                                        continue

                                    # Make sure it's not the search input
                                    placeholder = elem.get_attribute("placeholder") or ""
                                    if "search" in placeholder.lower():
                                        continue

                                    wager_input = elem
                                    print(f"[FANDUEL] Found wager input using: {selector}")
                                    break
                                except Exception:
                                    continue
                            if wager_input:
                                break
                    except Exception:
                        continue

            if wager_input is None:
                self._screenshot("wager_input_not_found")
                raise BetPlacerError("Could not find FanDuel wager input")

            # Clear and enter amount
            wager_input.click()
            wager_input.fill("")  # Clear existing value
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
            # BetMGM: app-stake-input input or similar
            wager_selectors = [
                'app-stake-input input',
                'input[type="text"]',
            ]

            wager_input = None
            for selector in wager_selectors:
                try:
                    locator = self.page.locator(selector)
                    if locator.count() > 0:
                        for i in range(locator.count()):
                            elem = locator.nth(i)
                            if elem.is_visible():
                                wager_input = elem
                                break
                        if wager_input:
                            break
                except Exception:
                    continue

            if wager_input is None:
                self._screenshot("wager_input_not_found")
                raise BetPlacerError("Could not find BetMGM wager input")

            # Clear and enter amount
            wager_input.click()
            wager_input.fill(f"{amount:.2f}")
            self.page.wait_for_timeout(500)

            # Press down arrow to trigger validation and enable Place Bet button
            self.page.keyboard.press("ArrowDown")
            self.page.wait_for_timeout(1000)

            self._screenshot("wager_entered")
            print(f"[BETMGM] ✓ Wager entered: ${amount:.2f}")
            return True

        except Exception as e:
            self._screenshot("wager_entry_failed")
            raise BetPlacerError(f"Failed to enter wager: {e}")

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
        """Place bet on FanDuel."""
        try:
            # From DevTools recording: [data-testid='place-bet-button']
            place_btn = self.page.locator('[data-testid="place-bet-button"]')

            if place_btn.count() == 0:
                self._screenshot("place_bet_not_found")
                raise BetPlacerError("Place Bet button not found")

            print(f"[FANDUEL] Clicking Place Bet...")
            place_btn.first.click()
            self.page.wait_for_timeout(2000)

            # Check for success: bet receipt or confirmation
            receipt = self.page.locator('[data-testid="bet-receipt-done-btn"]')
            if receipt.count() > 0:
                self._screenshot("bet_placed_success")
                print(f"[FANDUEL] ✓ Bet ACCEPTED")
                return "ACCEPTED", "Bet placed successfully"

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

            # Unknown state
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
