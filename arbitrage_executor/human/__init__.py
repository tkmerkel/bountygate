"""Humanized Playwright primitives."""

from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)
from human.waiting import settle, WAIT_CATEGORIES
from human.typing import TypingProfile, humanized_type
from human.mouse import CursorState, move_to, click, idle_jitter
from human.navigation import click_through
from human.modals import ModalWatcher

__all__ = [
    "SlipDrainedDuringIdleError",
    "FdOddsDriftedDuringIdleError",
    "settle",
    "WAIT_CATEGORIES",
    "TypingProfile",
    "humanized_type",
    "CursorState",
    "move_to",
    "click",
    "idle_jitter",
    "click_through",
    "ModalWatcher",
]
