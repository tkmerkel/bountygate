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
from human.mouse import CursorState, move_to

__all__ = [
    "SlipDrainedDuringIdleError",
    "FdOddsDriftedDuringIdleError",
    "settle",
    "WAIT_CATEGORIES",
    "TypingProfile",
    "humanized_type",
    "CursorState",
    "move_to",
]
