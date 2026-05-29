"""
Selector Finder Utilities
Helper functions for discovering, validating, and managing element selectors.
"""

import os
import yaml
from datetime import datetime
from typing import Dict, Optional, Tuple
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


class SelectorFinder:
    """Utilities for finding and validating element selectors."""

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
    def _selectors_dir() -> str:
        """Return the selectors directory independent of caller CWD."""
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "selectors")

    @staticmethod
    def _market_config_path(site: str) -> str:
        return os.path.join(SelectorManager._selectors_dir(), f"{site}_markets.yaml")

    @staticmethod
    def load_market_config(site: str) -> Dict:
        """Load market configuration from YAML file."""
        file_path = SelectorManager._market_config_path(site)
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
        file_path = SelectorManager._market_config_path(site)

        try:
            # Load existing config
            existing = SelectorManager.load_market_config(site)

            # Add timestamp for legacy mapping writes, but preserve the exact
            # timestamp set by executable validation metadata.
            config.setdefault('validated_at', datetime.now().isoformat())

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
    def update_validation_status(
        site: str,
        market_key: str,
        *,
        status: str,
        details: Optional[Dict] = None,
    ) -> bool:
        """Update validation metadata on an existing market config.

        `status` is intentionally small and stringly for YAML readability:
        "passed", "failed", or "unknown".
        """
        config = SelectorManager.get_market(site, market_key)
        if not config:
            print(f"Cannot update validation: {site}/{market_key} is not mapped")
            return False

        details = details or {}
        config["validation_status"] = status
        config["validated_at"] = datetime.now().isoformat()
        config["validation"] = {
            **details,
            "status": status,
            "validated_at": config["validated_at"],
        }
        return SelectorManager.save_market_config(site, market_key, config)

    @staticmethod
    def has_market(site: str, market_key: str) -> bool:
        """Check if a market is already mapped."""
        config = SelectorManager.load_market_config(site)
        return market_key in config

    @staticmethod
    def is_market_executable(site: str, market_key: str) -> bool:
        """Return True only for mappings proven by the validation harness."""
        config = SelectorManager.get_market(site, market_key)
        if not config:
            return False
        return config.get("validation_status") == "passed"

    @staticmethod
    def get_market(site: str, market_key: str) -> Optional[Dict]:
        """Get configuration for a specific market."""
        config = SelectorManager.load_market_config(site)
        return config.get(market_key)
