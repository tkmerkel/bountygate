"""BetMGM humanized bet placer.

Rewrite of the legacy bet_placer_betmgm.py against the new
``arbitrage_executor/human/`` primitives. Public interface preserved
(see ``bet_placer.BetPlacer`` ABC).
"""

from typing import Dict, Optional, Tuple

from bet_placer import BetPlacer, BetPlacerError, BetPlacerSkipError


class BetmgmBetPlacer(BetPlacer):
    """Handles bet placement on BetMGM."""

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        raise NotImplementedError("Task 12")

    def clear_betslip(self):
        raise NotImplementedError("Task 12")

    def assert_betslip_has_bet(self):
        raise NotImplementedError("Task 12")

    def assert_betslip_empty(self):
        raise NotImplementedError("Task 12")

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
