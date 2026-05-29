"""
devig.py — Remove-the-vig (devigging) functions for two-way markets.

All functions are pure (no I/O) and operate on *decimal* odds (e.g. 1.909
for American -110).  Two-way markets are assumed: Over side and Under side.

Supported methods
-----------------
* Multiplicative  — divide each raw implied prob by the sum (proportional).
* Power           — solve exponent k such that p_o^k + p_u^k = 1 (bisection).
* Shin            — Shin (1992/1993) insider-trading model (bisection on z).

References
----------
* Shin, H. S. (1992). Prices of state contingent claims with insider traders.
  The Economic Journal, 102(411), 426-435.
* Joseph Buchdahl — "Wisdom of the Crowd" odds comparison methodology.
"""

from __future__ import annotations

import math
from typing import Optional


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

def implied_prob(decimal_odds: float) -> float:
    """Return the raw (vig-inclusive) implied probability for *decimal_odds*.

    Formula:  p = 1 / decimal_odds

    Parameters
    ----------
    decimal_odds : float
        Decimal odds >= 1.0 (e.g. 1.909 for −110 American).

    Returns
    -------
    float
        Implied probability in [0, 1].
    """
    if decimal_odds <= 0:
        raise ValueError(f"decimal_odds must be positive; got {decimal_odds}")
    return 1.0 / decimal_odds


# ---------------------------------------------------------------------------
# Multiplicative devig
# ---------------------------------------------------------------------------

def multiplicative_devig(
    p_over: float, p_under: float
) -> tuple[float, float]:
    """Remove the overround using the multiplicative (proportional) method.

    Each raw implied probability is scaled by the inverse of the overround so
    the pair sums to 1:

        fair_p_i = p_i / (p_over + p_under)

    Parameters
    ----------
    p_over : float
        Raw implied probability for the Over side.
    p_under : float
        Raw implied probability for the Under side.

    Returns
    -------
    (fair_prob_over, fair_prob_under) : tuple[float, float]
    """
    total = p_over + p_under
    if total <= 0:
        raise ValueError("Sum of implied probabilities must be positive.")
    return p_over / total, p_under / total


# ---------------------------------------------------------------------------
# Power devig
# ---------------------------------------------------------------------------

def power_devig(
    p_over: float,
    p_under: float,
    *,
    tol: float = 1e-9,
    max_iter: int = 100,
) -> tuple[float, float]:
    """Remove the overround using the power (Shin-inspired) method.

    Finds exponent k > 0 such that:

        p_over^k + p_under^k = 1

    via bisection on k in (0.0001, 50).  When vig is present k < 1 (flattens
    the distribution); when no vig k = 1.

    Parameters
    ----------
    p_over : float
        Raw implied probability for the Over side (1 / decimal_odds).
    p_under : float
        Raw implied probability for the Under side.
    tol : float
        Bisection convergence tolerance on k.
    max_iter : int
        Maximum bisection iterations before falling back to multiplicative.

    Returns
    -------
    (fair_prob_over, fair_prob_under) : tuple[float, float]
        Each is p_side^k; they sum to 1 by construction.

    Notes
    -----
    Falls back to :func:`multiplicative_devig` on non-convergence or
    degenerate input.
    """
    # Degenerate guards
    if p_over <= 0 or p_under <= 0:
        return multiplicative_devig(max(p_over, 1e-12), max(p_under, 1e-12))

    # If overround is already essentially 1, no vig present.
    total = p_over + p_under
    if abs(total - 1.0) < 1e-12:
        return float(p_over), float(p_under)

    def _f(k: float) -> float:
        return p_over ** k + p_under ** k - 1.0

    lo, hi = 0.0001, 50.0

    # Sanity: f(lo) and f(hi) should have opposite signs when vig is present.
    f_lo = _f(lo)
    f_hi = _f(hi)
    if f_lo * f_hi > 0:
        # Bisection cannot work — fall back.
        return multiplicative_devig(p_over, p_under)

    k = 1.0
    for _ in range(max_iter):
        k = (lo + hi) / 2.0
        fk = _f(k)
        if abs(fk) < tol or (hi - lo) / 2.0 < tol:
            break
        if f_lo * fk < 0:
            hi = k
            f_hi = fk
        else:
            lo = k
            f_lo = fk
    else:
        # Non-convergence: fall back to multiplicative.
        return multiplicative_devig(p_over, p_under)

    fair_over = p_over ** k
    fair_under = p_under ** k
    # Normalise for floating-point residual.
    s = fair_over + fair_under
    return fair_over / s, fair_under / s


# ---------------------------------------------------------------------------
# Shin devig
# ---------------------------------------------------------------------------

def shin_devig(
    p_over: float,
    p_under: float,
    *,
    tol: float = 1e-9,
    max_iter: int = 200,
) -> tuple[float, float]:
    """Remove the overround using Shin's (1992) insider-trading model.

    Shin's model posits that a fraction *z* of bettors are insiders with
    perfect information.  The fair probability for each outcome i is:

        pi_i = (sqrt(z^2 + 4*(1-z) * r_i^2 / sum_r) - z) / (2*(1-z))

    where r_i = p_i (raw implied prob) and sum_r = p_over + p_under.

    We find z ∈ [0, 1) by bisection such that pi_over + pi_under = 1.

    Parameters
    ----------
    p_over : float
        Raw implied probability for the Over side.
    p_under : float
        Raw implied probability for the Under side.
    tol : float
        Bisection convergence tolerance on z.
    max_iter : int
        Maximum bisection iterations before falling back to multiplicative.

    Returns
    -------
    (fair_prob_over, fair_prob_under) : tuple[float, float]

    Notes
    -----
    Falls back to :func:`multiplicative_devig` on degenerate input or
    non-convergence.
    """
    if p_over <= 0 or p_under <= 0:
        return multiplicative_devig(max(p_over, 1e-12), max(p_under, 1e-12))

    sum_r = p_over + p_under
    if sum_r <= 0:
        return multiplicative_devig(p_over, p_under)

    # Normalise r values (Shin paper uses normalised probabilities).
    r_o = p_over / sum_r
    r_u = p_under / sum_r

    def _shin_pi(r_i: float, z: float) -> float:
        """Single-outcome Shin fair probability."""
        if abs(1 - z) < 1e-14:
            return r_i  # z→1 degenerate: return raw
        discriminant = z ** 2 + 4.0 * (1.0 - z) * r_i ** 2
        if discriminant < 0:
            discriminant = 0.0
        return (math.sqrt(discriminant) - z) / (2.0 * (1.0 - z))

    def _f(z: float) -> float:
        return _shin_pi(r_o, z) + _shin_pi(r_u, z) - 1.0

    # At z=0: pi_i = r_i (normalised), sum = 1.0 → f(0) ≈ 0 when no vig.
    # When vig present sum_r > 1 → z > 0.
    lo, hi = 0.0, 1.0 - 1e-10

    f_lo = _f(lo)
    f_hi = _f(hi)

    if abs(f_lo) < tol:
        # No vig / already fair.
        return _shin_pi(r_o, 0.0), _shin_pi(r_u, 0.0)

    if f_lo * f_hi > 0:
        return multiplicative_devig(p_over, p_under)

    z = 0.0
    for _ in range(max_iter):
        z = (lo + hi) / 2.0
        fz = _f(z)
        if abs(fz) < tol or (hi - lo) / 2.0 < tol:
            break
        if f_lo * fz < 0:
            hi = z
            f_hi = fz
        else:
            lo = z
            f_lo = fz
    else:
        return multiplicative_devig(p_over, p_under)

    fair_over = _shin_pi(r_o, z)
    fair_under = _shin_pi(r_u, z)
    s = fair_over + fair_under
    if s <= 0:
        return multiplicative_devig(p_over, p_under)
    return fair_over / s, fair_under / s


# ---------------------------------------------------------------------------
# method_disagreement helper
# ---------------------------------------------------------------------------

def method_disagreement(over_probs: list[float]) -> float:
    """Return the maximum absolute spread across multiple Over-side estimates.

    Parameters
    ----------
    over_probs : list[float]
        Fair Over-side probability from each devig method (e.g. mult, power,
        Shin).

    Returns
    -------
    float
        max(over_probs) - min(over_probs), or 0.0 for < 2 estimates.
    """
    valid = [p for p in over_probs if p is not None and math.isfinite(p)]
    if len(valid) < 2:
        return 0.0
    return max(valid) - min(valid)


# ---------------------------------------------------------------------------
# devig_all — convenience aggregator
# ---------------------------------------------------------------------------

def devig_all(
    over_odds: float | None,
    under_odds: float | None,
) -> Optional[dict]:
    """Compute all three devig methods for a two-way market and return a dict.

    Parameters
    ----------
    over_odds : float
        Decimal odds for the Over side (e.g. 1.909 for −110).
    under_odds : float
        Decimal odds for the Under side.

    Returns
    -------
    dict or None
        None if either odds is missing or <= 1.0 (one-sided / invalid quote).

        Otherwise returns:
        {
          'overround':               float,   # sum of raw implied probs - 1
          'fair_prob_over_mult':     float,
          'fair_prob_under_mult':    float,
          'fair_prob_over_power':    float,
          'fair_prob_under_power':   float,
          'fair_prob_over_shin':     float,
          'fair_prob_under_shin':    float,
          'method_disagreement':     float,   # max spread of Over across methods
          'disagreement_flag':       bool,    # True if spread > 0.02
        }
    """
    # Guard: missing or invalid odds.
    if over_odds is None or under_odds is None:
        return None
    try:
        over_odds = float(over_odds)
        under_odds = float(under_odds)
    except (TypeError, ValueError):
        return None
    if over_odds <= 1.0 or under_odds <= 1.0:
        return None

    r_o = implied_prob(over_odds)
    r_u = implied_prob(under_odds)

    overround = r_o + r_u - 1.0

    mult_o, mult_u = multiplicative_devig(r_o, r_u)
    pow_o, pow_u = power_devig(r_o, r_u)
    shin_o, shin_u = shin_devig(r_o, r_u)

    disagree = method_disagreement([mult_o, pow_o, shin_o])

    return {
        "overround": overround,
        "fair_prob_over_mult": mult_o,
        "fair_prob_under_mult": mult_u,
        "fair_prob_over_power": pow_o,
        "fair_prob_under_power": pow_u,
        "fair_prob_over_shin": shin_o,
        "fair_prob_under_shin": shin_u,
        "method_disagreement": disagree,
        "disagreement_flag": disagree > 0.02,
    }
