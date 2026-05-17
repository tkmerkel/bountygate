"""Tests for BetmgmBetPlacer migrated methods."""
import pytest

from _fakes import FakeElement, FakeLocator, FakePage
from bet_placer import BetPlacerError
from bet_placer_betmgm import BetmgmBetPlacer

AUDIT_DIR = "audit_logs/test_bet_placer_betmgm"


def test_skeleton_imports_cleanly():
    page = FakePage()
    placer = BetmgmBetPlacer(page, "betmgm", AUDIT_DIR)
    assert placer.site == "betmgm"
