"""Tests for FanduelBetPlacer migrated methods."""
import pytest

from _fakes import FakeElement, FakeLocator, FakePage
from bet_placer import BetPlacerError
from bet_placer_fanduel import FanduelBetPlacer, FANDUEL_THRESHOLD_ONE_LABELS

AUDIT_DIR = "audit_logs/test_bet_placer_fanduel"


def test_threshold_one_labels_has_22_entries():
    assert len(FANDUEL_THRESHOLD_ONE_LABELS) == 22


def test_threshold_one_labels_singular_and_plural_agree():
    assert FANDUEL_THRESHOLD_ONE_LABELS["Single"] == FANDUEL_THRESHOLD_ONE_LABELS["Singles"]
    assert FANDUEL_THRESHOLD_ONE_LABELS["RBI"] == FANDUEL_THRESHOLD_ONE_LABELS["RBIs"]
    assert FANDUEL_THRESHOLD_ONE_LABELS["Stolen Base"] == FANDUEL_THRESHOLD_ONE_LABELS["Stolen Bases"]


def test_rbi_uses_an_article():
    _, article, _ = FANDUEL_THRESHOLD_ONE_LABELS["RBI"]
    assert article == "An"
