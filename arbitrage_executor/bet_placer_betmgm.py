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
        raise NotImplementedError("migrated in Task C2")

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
