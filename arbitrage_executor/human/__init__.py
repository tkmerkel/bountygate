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
from human.typing import TypingProfile, humanized_type

__all__ = [
    "SlipDrainedDuringIdleError",
    "FdOddsDriftedDuringIdleError",
    "settle",
    "WAIT_CATEGORIES",
    "TypingProfile",
    "humanized_type",
]
