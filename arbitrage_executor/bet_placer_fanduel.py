"""FanDuel humanized bet placer.

Rewrite of the legacy bet_placer_fanduel.py against the new
``arbitrage_executor/human/`` primitives. Public interface preserved
(see ``bet_placer.BetPlacer`` ABC).
"""

from typing import Dict, Optional, Tuple

from bet_placer import BetPlacer, BetPlacerError


class FanduelBetPlacer(BetPlacer):
    """Handles bet placement on FanDuel."""

    def navigate_and_expand_market(self, opportunity, market_config, direction=None):
        raise NotImplementedError("Task 14")

    def clear_betslip(self):
        raise NotImplementedError("Task 14")

    def assert_betslip_has_bet(self):
        raise NotImplementedError("Task 14")

    def assert_betslip_empty(self):
        raise NotImplementedError("Task 14")

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
