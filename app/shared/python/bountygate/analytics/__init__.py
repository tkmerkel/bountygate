"""
bountygate.analytics
====================
Pure-Python quantitative betting analytics: devigging, consensus fair price,
EV, Kelly sizing, CLV, and line-movement signals.

No I/O.  All functions operate on Python scalars / plain lists so they can
be unit-tested without a database or Airflow context.
"""

from .devig import (
    implied_prob,
    multiplicative_devig,
    power_devig,
    shin_devig,
    devig_all,
    method_disagreement,
)
from .consensus import no_vig_consensus

__all__ = [
    "implied_prob",
    "multiplicative_devig",
    "power_devig",
    "shin_devig",
    "devig_all",
    "method_disagreement",
    "no_vig_consensus",
]
