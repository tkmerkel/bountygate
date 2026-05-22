"""Humanized Playwright primitives.

Public API surface kept small so callers (placers, orchestrator,
validator) import from ``human`` directly without reaching into
submodules.
"""

from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)

__all__ = [
    "SlipDrainedDuringIdleError",
    "FdOddsDriftedDuringIdleError",
]
