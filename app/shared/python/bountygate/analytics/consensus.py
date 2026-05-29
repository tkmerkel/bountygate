"""
consensus.py — Wide multi-book no-vig consensus fair probability.

Used as the fair-price anchor when Pinnacle has not yet posted for an event.
The approach is intentionally simple and dependency-light so it can run in any
Python environment (standard library ``statistics`` module is used when
available; falls back to a manual mean otherwise).

Algorithm
---------
For each bookmaker that has *both* an Over and an Under price:

1.  Compute the raw implied probabilities:
        r_over  = 1 / decimal_over
        r_under = 1 / decimal_under

2.  Apply multiplicative devigging (proportional normalisation):
        fair_over  = r_over  / (r_over + r_under)
        fair_under = r_under / (r_over + r_under)

3.  Average the per-book fair probabilities across all usable books:
        consensus_over  = mean(fair_over_i  for i in usable_books)
        consensus_under = mean(fair_under_i for i in usable_books)

This is the "wisdom of the crowd" approach described in:
    Buchdahl, J. (2016). "Closing Line Value and the Assessment of Betting
    Systems", https://www.football-data.co.uk/

No IO; no external dependencies beyond the standard library.
"""

from __future__ import annotations

from typing import Optional

from .devig import implied_prob, multiplicative_devig


def no_vig_consensus(
    over_odds_list: list[float],
    under_odds_list: list[float],
) -> Optional[tuple[float, float]]:
    """Compute the wide multi-book no-vig consensus fair probability.

    For each book that provides both an Over and an Under price, the
    multiplicative devig is applied.  The resulting fair probabilities are
    averaged across all usable books.

    Parameters
    ----------
    over_odds_list : list[float]
        Decimal Over odds from each bookmaker, in the same order as
        *under_odds_list*.  Use ``None`` or ``0`` / ``<= 1.0`` to indicate a
        missing quote for that bookmaker.
    under_odds_list : list[float]
        Decimal Under odds from each bookmaker, parallel to *over_odds_list*.

    Returns
    -------
    (consensus_fair_over, consensus_fair_under) : tuple[float, float] or None
        Each value is in [0, 1] and the pair sums to 1.0.
        Returns ``None`` if no bookmaker has a valid two-way quote.

    Examples
    --------
    >>> # Two symmetric books both at −110 / −110 (decimal 1.9091)
    >>> o = no_vig_consensus([1.9091, 1.9091], [1.9091, 1.9091])
    >>> round(o[0], 4), round(o[1], 4)
    (0.5, 0.5)

    >>> # Mixed: one book missing Under
    >>> o = no_vig_consensus([1.80, None], [2.10, 1.95])
    >>> round(o[0] + o[1], 8)
    1.0
    """
    fair_overs: list[float] = []
    fair_unders: list[float] = []

    for o_odds, u_odds in zip(over_odds_list, under_odds_list):
        # Skip missing or invalid (one-sided / placeholder) quotes.
        try:
            o = float(o_odds)
            u = float(u_odds)
        except (TypeError, ValueError):
            continue

        if o <= 1.0 or u <= 1.0:
            continue

        r_o = implied_prob(o)
        r_u = implied_prob(u)

        fair_o, fair_u = multiplicative_devig(r_o, r_u)
        fair_overs.append(fair_o)
        fair_unders.append(fair_u)

    if not fair_overs:
        return None

    n = len(fair_overs)
    mean_over = sum(fair_overs) / n
    mean_under = sum(fair_unders) / n

    # Renormalise to absorb any floating-point residual.
    total = mean_over + mean_under
    return mean_over / total, mean_under / total
