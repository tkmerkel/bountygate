from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)
from bet_placer import BetPlacerSkipError


def test_idle_errors_subclass_skip_error():
    """Idle-window errors must be benign skips, not real failures.

    The orchestrator catches BetPlacerSkipError as a non-counting skip;
    these errors only fire BEFORE the Phase 2 placement (idle is
    explicitly forbidden between Phase 2 and Phase 3), so they are
    benign by construction.
    """
    assert issubclass(SlipDrainedDuringIdleError, BetPlacerSkipError)
    assert issubclass(FdOddsDriftedDuringIdleError, BetPlacerSkipError)


def test_drift_error_carries_old_and_new_odds():
    err = FdOddsDriftedDuringIdleError(old_odds=2.10, new_odds=2.05, epsilon=0.05)
    assert err.old_odds == 2.10
    assert err.new_odds == 2.05
    assert err.epsilon == 0.05
    assert "2.10" in str(err) and "2.05" in str(err)
