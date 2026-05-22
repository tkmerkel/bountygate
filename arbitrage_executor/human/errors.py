"""Typed errors raised during the humanization flow.

Both errors subclass ``BetPlacerSkipError`` so the orchestrator's
existing ``except BetPlacerSkipError`` branches classify them as
benign skips. They can only fire during ``intra_book_idle`` (between
Phase 1 and Phase 2) — by hard rule there is no idle between Phase 2
and Phase 3, so neither can fire inside the orphan window.

Worker classification path (verified Task 19):
  raise SlipDrainedDuringIdleError / FdOddsDriftedDuringIdleError
    → execute()'s ``except BetPlacerSkipError: raise`` re-raises
    → execute_arb.main()'s per-opp ``except BetPlacerSkipError`` advances
      to next candidate WITHOUT setting attempted_any
    → main() returns (False, False) when nothing else attempted
    → task_worker.py's "No viable opportunity" branch marks SKIPPED
      (does NOT increment the circuit-breaker counter).
"""

from bet_placer import BetPlacerSkipError


class SlipDrainedDuringIdleError(BetPlacerSkipError):
    """The FanDuel betslip lost its Phase 1 selection while the bot was
    idling. The Phase 2 placement cannot proceed without a hedge target,
    so we skip to the next opportunity instead of placing a bare MGM
    leg.
    """


class FdOddsDriftedDuringIdleError(BetPlacerSkipError):
    """FanDuel odds moved by more than ``IDLE_DRIFT_EPSILON`` (decimal
    units) between the Phase 1 tease-discovery and the post-idle
    re-check. ROI may have flipped negative; skip rather than place.
    """

    def __init__(self, *, old_odds: float, new_odds: float, epsilon: float):
        self.old_odds = old_odds
        self.new_odds = new_odds
        self.epsilon = epsilon
        super().__init__(
            f"FanDuel odds drifted during idle: {old_odds:.2f} → "
            f"{new_odds:.2f} (epsilon={epsilon:.2f})"
        )
