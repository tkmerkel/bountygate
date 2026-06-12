import pytest

from bountygate.models.fair import book_fair_probs, weighted_consensus

EVEN = 1.9091  # -110 American


def test_book_fair_probs_symmetric_book():
    out = book_fair_probs({"fanduel": {"Over": EVEN, "Under": EVEN}}, ["Over", "Under"])
    assert set(out) == {"fanduel"}
    for method in ("mult", "power", "shin"):
        assert out["fanduel"][method]["Over"] == pytest.approx(0.5, abs=1e-6)
        assert out["fanduel"][method]["Under"] == pytest.approx(0.5, abs=1e-6)


def test_book_fair_probs_skips_one_sided_books():
    out = book_fair_probs(
        {"fanduel": {"Over": EVEN, "Under": EVEN}, "draftkings": {"Over": 1.87}},
        ["Over", "Under"],
    )
    assert "draftkings" not in out and "fanduel" in out


def test_weighted_consensus_equal_weights_is_mean():
    probs = {"a": {"X": 0.6, "Y": 0.4}, "b": {"X": 0.5, "Y": 0.5}}
    cons = weighted_consensus(probs)
    assert cons["X"] == pytest.approx(0.55)
    assert cons["X"] + cons["Y"] == pytest.approx(1.0)


def test_weighted_consensus_respects_weights():
    probs = {"a": {"X": 0.6, "Y": 0.4}, "b": {"X": 0.5, "Y": 0.5}}
    cons = weighted_consensus(probs, {"a": 1.0, "b": 0.0})
    assert cons["X"] == pytest.approx(0.6)


def test_weighted_consensus_unknown_book_gets_default_half_weight():
    probs = {"a": {"X": 0.6, "Y": 0.4}, "b": {"X": 0.5, "Y": 0.5}}
    # weights dict missing 'b' -> b gets 0.5: X = (1*0.6 + 0.5*0.5) / 1.5
    cons = weighted_consensus(probs, {"a": 1.0})
    assert cons["X"] == pytest.approx((0.6 + 0.25) / 1.5)


def test_weighted_consensus_empty_returns_none():
    assert weighted_consensus({}) is None
    assert weighted_consensus({"a": {"X": 0.6}}, {"a": 0.0}) is None
