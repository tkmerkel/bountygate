"""
Bet Placement Logic
Site-specific logic for placing bets on FanDuel and BetMGM lives in
bet_placer_fanduel.py and bet_placer_betmgm.py. This module exposes
the public surface (BetPlacerError + the BetPlacer abstract base + a
factory that dispatches by site string).
"""

import os
from abc import ABC, abstractmethod
from typing import Dict, Tuple, Optional


class BetPlacerError(Exception):
    """Raised when bet placement fails."""
    pass


class BetPlacer(ABC):
    """Abstract base for site-specific bet placers.

    Construct via ``BetPlacer(page, site, audit_dir)`` — the ``__new__``
    factory dispatches by site string to the concrete subclass.
    """

    def __new__(cls, page, site, audit_dir):
        # Factory dispatch: BetPlacer(page, "fanduel", ...) returns a
        # FanduelBetPlacer instance. Subclasses constructed directly
        # (FanduelBetPlacer(page, ...)) bypass this branch.
        #
        # The subclass imports happen INSIDE __new__ to avoid the
        # circular import that would otherwise hit at module-load time:
        # bet_placer_fanduel and bet_placer_betmgm both inherit from
        # BetPlacer defined here.
        #
        # Subclasses must NOT override __init__ to add new positional
        # arguments — Python's __new__/__init__ contract requires the
        # same call signature reach both.
        if cls is BetPlacer:
            if site == "fanduel":
                from bet_placer_fanduel import FanduelBetPlacer
                return object.__new__(FanduelBetPlacer)
            if site == "betmgm":
                from bet_placer_betmgm import BetmgmBetPlacer
                return object.__new__(BetmgmBetPlacer)
            raise BetPlacerError(f"Unknown site: {site}")
        return object.__new__(cls)

    def __init__(self, page, site, audit_dir):
        self.page = page
        self.site = site
        self.audit_dir = audit_dir
        os.makedirs(audit_dir, exist_ok=True)

    def _screenshot(self, tag: str) -> str:
        """Thin proxy to the helper, preserved as an instance method so
        per-method bodies migrate from the legacy class with zero changes
        to screenshot call sites."""
        from _bet_placer_helpers import screenshot
        return screenshot(self.page, self.audit_dir, self.site, tag)

    @abstractmethod
    def navigate_and_expand_market(self, opportunity: Dict, market_config: Dict, direction: str = None) -> None: ...
    @abstractmethod
    def clear_betslip(self) -> None: ...
    @abstractmethod
    def assert_betslip_has_bet(self) -> None: ...
    @abstractmethod
    def assert_betslip_empty(self) -> None: ...
    @abstractmethod
    def find_and_click_bet(self, opportunity: Dict, direction: str, market_config: Dict) -> bool: ...
    @abstractmethod
    def enter_wager(self, amount: float) -> bool: ...
    @abstractmethod
    def place_bet(self) -> Tuple[str, str]: ...
    @abstractmethod
    def get_actual_odds(self) -> Optional[float]: ...

    # Optional capabilities — base raises NotImplementedError
    def discover_max_wager(self) -> Tuple[float, str]:
        raise NotImplementedError(
            f"{self.site} does not support max-wager discovery"
        )

    def check_limit_alert(self) -> Tuple[bool, Optional[float]]:
        raise NotImplementedError(
            f"{self.site} does not support limit-alert check"
        )
