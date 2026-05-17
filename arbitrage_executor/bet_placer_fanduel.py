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
        raise NotImplementedError("migrated in Task B2")

    def clear_betslip(self):
        raise NotImplementedError("migrated in Task B3")

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
