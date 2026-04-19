"""
Selector Finder Utilities
Helper functions for discovering, validating, and managing element selectors.
"""

import yaml
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError


# ============================================================================
# Alternate Market Utilities
# ============================================================================

def is_alternate_market(market_key: str) -> bool:
    """Check if a market key represents an alternate market.

    Args:
        market_key: Market key to check (e.g., 'player_points_alternate')

    Returns:
        True if the market key ends with '_alternate'
    """
    return market_key.endswith('_alternate')


def get_base_market_key(market_key: str) -> str:
    """Get the base market key by stripping the _alternate suffix.

    Args:
        market_key: Market key that may have _alternate suffix

    Returns:
        Base market key without _alternate suffix

    Example:
        get_base_market_key('player_points_alternate') -> 'player_points'
        get_base_market_key('player_points') -> 'player_points'
    """
    if market_key.endswith('_alternate'):
        return market_key[:-10]  # Remove '_alternate' (10 chars)
    return market_key


def calculate_alternate_tab_value(line: float) -> int:
    """Calculate BetMGM tab value for alternate markets.

    BetMGM alternate markets have tabs like "5+", "7+", "9+" for different thresholds.
    The tab value is calculated as int(line + 0.5).

    Args:
        line: The betting line (e.g., 4.5, 6.5)

    Returns:
        Tab value as integer (e.g., 4.5 -> 5, 6.5 -> 7)

    Example:
        calculate_alternate_tab_value(4.5) -> 5
        calculate_alternate_tab_value(6.5) -> 7
        calculate_alternate_tab_value(14.5) -> 15
    """
    return int(line + 0.5)


class SelectorCandidate:
    """Represents a potential selector for a market."""

    def __init__(self, selector: str, selector_type: str, preview_text: str, confidence: int):
        self.selector = selector
        self.selector_type = selector_type  # aria_label, button, div, xpath
        self.preview_text = preview_text
        self.confidence = confidence  # 0-100, how likely this is correct

    def __repr__(self):
        return f"[{self.confidence}%] {self.selector_type}: {self.selector}\n  Preview: {self.preview_text[:80]}"


class SelectorFinder:
    """Utilities for finding and validating element selectors."""

    @staticmethod
    def find_candidates_by_text(page: Page, search_terms: List[str], player_name: str,
                                line: Optional[float] = None) -> List[SelectorCandidate]:
        """
        Scan page for elements containing search terms.

        Args:
            page: Playwright page object
            search_terms: List of text to search for (e.g., ["Points", "Total Points"])
            player_name: Player name to match
            line: Optional betting line to match (e.g., 25.5)

        Returns:
            List of SelectorCandidate objects ranked by confidence
        """
        candidates = []

        # Strategy 1: aria-label matching (FanDuel style)
        for term in search_terms:
            try:
                # Find all elements with aria-label containing the term
                elements = page.locator(f'[aria-label*="{term}"]').all()

                for elem in elements[:10]:  # Limit to first 10 to avoid spam
                    try:
                        aria_label = elem.get_attribute("aria-label") or ""

                        # Check if it contains player name and term
                        if player_name.lower() in aria_label.lower() and term.lower() in aria_label.lower():
                            confidence = 80

                            # Boost confidence if line matches
                            if line and str(line) in aria_label:
                                confidence = 95

                            selector = f'[aria-label*="{player_name}"][aria-label*="{term}"]'
                            if line:
                                selector += f'[aria-label*="{line}"]'

                            candidates.append(SelectorCandidate(
                                selector=selector,
                                selector_type="aria_label",
                                preview_text=aria_label,
                                confidence=confidence
                            ))
                    except Exception:
                        continue
            except Exception:
                continue

        # Strategy 2: button with text content (BetMGM style)
        for term in search_terms:
            try:
                # Find buttons containing the term
                buttons = page.locator(f'button:has-text("{term}")').all()

                for btn in buttons[:10]:
                    try:
                        text_content = btn.text_content() or ""

                        # Check if it contains player name
                        if player_name.lower() in text_content.lower():
                            confidence = 70

                            # Try to extract data attributes for better selector
                            data_attrs = {}
                            for attr in ["data-test", "data-testid", "data-market", "data-selection"]:
                                val = btn.get_attribute(attr)
                                if val:
                                    data_attrs[attr] = val

                            if data_attrs:
                                # Prefer data attributes
                                attr_name = list(data_attrs.keys())[0]
                                attr_val = data_attrs[attr_name]
                                selector = f'button[{attr_name}="{attr_val}"]'
                                confidence = 85
                            else:
                                # Fall back to text-based selector
                                selector = f'button:has-text("{player_name}"):has-text("{term}")'

                            candidates.append(SelectorCandidate(
                                selector=selector,
                                selector_type="button",
                                preview_text=text_content[:100],
                                confidence=confidence
                            ))
                    except Exception:
                        continue
            except Exception:
                continue

        # Strategy 3: BetMGM-style ms-event-pick elements (player-aware search)
        try:
            # BetMGM structure (from DevTools recording):
            # ms-split-header contains all player rows
            # Each row is a div that contains player name + ms-option elements
            # ms-option[1] = over, ms-option[2] = under (typically)
            # ms-event-pick is the clickable button inside ms-option

            # Find the ms-split-header container
            split_headers = page.locator('ms-split-header').all()

            for split_header in split_headers:
                try:
                    # Get all player row containers within this split header
                    # Each player row is a div child of ms-split-header/div
                    player_rows = split_header.locator('> div > div').all()

                    for row in player_rows:
                        try:
                            row_text = row.text_content() or ""

                            # Check if this row contains our player name
                            if player_name.lower() not in row_text.lower():
                                continue

                            # This is the right player's row!
                            # Now find ms-option elements within this row
                            ms_options = row.locator('ms-option').all()

                            for option in ms_options:
                                try:
                                    # Find the ms-event-pick within this option
                                    event_pick = option.locator('ms-event-pick').first

                                    if event_pick.count() == 0:
                                        continue

                                    pick_text = event_pick.text_content() or ""

                                    # Check if contains our line
                                    if line and str(line) not in pick_text:
                                        continue

                                    # Get the data-test-option-id for a unique selector
                                    option_id = event_pick.get_attribute("data-test-option-id")

                                    if option_id:
                                        selector = f'ms-event-pick[data-test-option-id="{option_id}"]'
                                        confidence = 98
                                    else:
                                        # Fallback: build a specific selector path
                                        # Find player row, then ms-option containing the line
                                        selector = f'ms-split-header div:has-text("{player_name}") ms-option:has-text("{line}") ms-event-pick'
                                        confidence = 90

                                    # Determine direction
                                    direction = "over" if " O " in pick_text or pick_text.strip().startswith("O ") else "under"

                                    preview = f"[{direction}] {player_name} {line} - {pick_text.strip()}"

                                    candidates.append(SelectorCandidate(
                                        selector=selector,
                                        selector_type="ms_event_pick",
                                        preview_text=preview[:100],
                                        confidence=confidence
                                    ))

                                except Exception:
                                    continue

                            # Found the right player, no need to check other rows
                            if len(candidates) > 0:
                                break

                        except Exception:
                            continue

                except Exception:
                    continue

        except Exception:
            pass

        # Strategy 4: Generic clickable elements with role="button"
        for term in search_terms:
            try:
                elements = page.locator(f'[role="button"]:has-text("{term}")').all()

                for elem in elements[:10]:
                    try:
                        text_content = elem.text_content() or ""

                        if player_name.lower() in text_content.lower():
                            # Check for unique attributes
                            elem_id = elem.get_attribute("id")
                            elem_class = elem.get_attribute("class")

                            if elem_id:
                                selector = f'#{elem_id}'
                                confidence = 90
                            elif elem_class:
                                # Use first class
                                first_class = elem_class.split()[0]
                                selector = f'[role="button"].{first_class}:has-text("{term}")'
                                confidence = 75
                            else:
                                selector = f'[role="button"]:has-text("{player_name}"):has-text("{term}")'
                                confidence = 60

                            candidates.append(SelectorCandidate(
                                selector=selector,
                                selector_type="role_button",
                                preview_text=text_content[:100],
                                confidence=confidence
                            ))
                    except Exception:
                        continue
            except Exception:
                continue

        # Sort by confidence descending
        candidates.sort(key=lambda c: c.confidence, reverse=True)

        # Remove duplicates based on preview text
        seen_previews = set()
        unique_candidates = []
        for cand in candidates:
            preview_key = cand.preview_text[:50].strip()
            if preview_key not in seen_previews:
                seen_previews.add(preview_key)
                unique_candidates.append(cand)

        return unique_candidates[:5]  # Return top 5

    @staticmethod
    def validate_selector(page: Page, selector: str, expected_player: str,
                         expected_market: str) -> Tuple[bool, str]:
        """
        Validate a selector by attempting to locate and inspect the element.

        Returns:
            (success: bool, message: str)
        """
        try:
            locator = page.locator(selector)
            count = locator.count()

            if count == 0:
                return False, "Selector matched 0 elements"

            if count > 5:
                return False, f"Selector matched {count} elements (too many, need more specific)"

            # Get first match
            elem = locator.first

            # Check if element is visible
            if not elem.is_visible():
                return False, "Element not visible"

            # Try to get text content
            text = elem.text_content() or elem.get_attribute("aria-label") or ""

            # Verify it contains expected player and market terms
            if expected_player.lower() not in text.lower():
                return False, f"Element doesn't contain player name: {text[:50]}"

            if expected_market.lower() not in text.lower():
                return False, f"Element doesn't contain market term: {text[:50]}"

            return True, f"Valid! Matched {count} element(s)"

        except PlaywrightTimeoutError:
            return False, "Timeout waiting for element"
        except Exception as e:
            return False, f"Error: {str(e)}"

    @staticmethod
    def test_click_selector(page: Page, selector: str) -> Tuple[bool, str]:
        """
        Test clicking a selector and verify betslip updates.

        Returns:
            (success: bool, message: str)
        """
        try:
            # Take screenshot before
            initial_html = page.content()

            # Click the element
            locator = page.locator(selector).first
            locator.click(timeout=5000)

            # Wait briefly for betslip to update
            page.wait_for_timeout(1000)

            # Check for common betslip indicators
            betslip_indicators = [
                'aria-selected="true"',  # FanDuel style
                'text=Remove',  # Common remove bet button
                'text=Bet Slip',  # Betslip panel
                '[class*="betslip"]',  # Class containing betslip
                '[data-test*="betslip"]',  # Data attribute
            ]

            for indicator in betslip_indicators:
                try:
                    if page.locator(indicator).count() > 0:
                        return True, "Click successful, betslip updated"
                except Exception:
                    continue

            # Check if page content changed
            new_html = page.content()
            if new_html != initial_html:
                return True, "Click successful, page updated"

            return False, "Click executed but betslip didn't update"

        except PlaywrightTimeoutError:
            return False, "Timeout clicking element"
        except Exception as e:
            return False, f"Error clicking: {str(e)}"


class SelectorManager:
    """Manages loading and saving selector configurations."""

    @staticmethod
    def load_market_config(site: str) -> Dict:
        """Load market configuration from YAML file."""
        file_path = f"selectors/{site}_markets.yaml"
        try:
            with open(file_path, 'r') as f:
                config = yaml.safe_load(f) or {}
                # Filter out comments/none values
                return {k: v for k, v in config.items() if v is not None and isinstance(v, dict)}
        except FileNotFoundError:
            return {}
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return {}

    @staticmethod
    def save_market_config(site: str, market_key: str, config: Dict) -> bool:
        """Save a market configuration to YAML file."""
        file_path = f"selectors/{site}_markets.yaml"

        try:
            # Load existing config
            existing = SelectorManager.load_market_config(site)

            # Add timestamp
            config['validated_at'] = datetime.now().isoformat()

            # Update with new market
            existing[market_key] = config

            # Write back to file
            with open(file_path, 'w') as f:
                # Write header comment
                f.write(f"# {site.upper()} Market Selector Configuration\n")
                f.write(f"# Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                yaml.dump(existing, f, default_flow_style=False, sort_keys=False)

            return True
        except Exception as e:
            print(f"Error saving to {file_path}: {e}")
            return False

    @staticmethod
    def has_market(site: str, market_key: str) -> bool:
        """Check if a market is already mapped."""
        config = SelectorManager.load_market_config(site)
        return market_key in config

    @staticmethod
    def get_market(site: str, market_key: str) -> Optional[Dict]:
        """Get configuration for a specific market."""
        config = SelectorManager.load_market_config(site)
        return config.get(market_key)
