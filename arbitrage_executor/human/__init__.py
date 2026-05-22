"""Humanized Playwright primitives.

Public API surface kept small so callers (placers, orchestrator,
validator) import from ``human`` directly without reaching into
submodules.
"""

from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)
from human.waiting import settle, WAIT_CATEGORIES

__all__ = [
    "SlipDrainedDuringIdleError",
    "FdOddsDriftedDuringIdleError",
    "settle",
    "WAIT_CATEGORIES",
]
