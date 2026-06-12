import pytest

from bountygate.models.weights import sharpness_weights


def test_prior_when_no_venue_qualifies():
    stats = {"pinnacle": {"brier": 0.20, "n_games": 10},
             "fanduel": {"brier": 0.26, "n_games": 10}}
    w = sharpness_weights(stats, min_games=200)
    assert w == {"pinnacle": 1.0, "fanduel": 0.5}


def test_qualified_venues_get_inverse_brier_weights():
    stats = {"pinnacle": {"brier": 0.24, "n_games": 300},
             "fanduel": {"brier": 0.25, "n_games": 300},
             "newbook": {"brier": 0.20, "n_games": 10}}
    w = sharpness_weights(stats, min_games=200)
    assert w["pinnacle"] == pytest.approx(1.0)           # sharpest qualified
    assert w["fanduel"] == pytest.approx(0.24 / 0.25)
    assert w["newbook"] == 0.5                            # unqualified -> prior


def test_empty_stats_returns_empty_dict():
    assert sharpness_weights({}, min_games=200) == {}


def test_zero_brier_is_clamped_not_divided():
    stats = {"a": {"brier": 0.0, "n_games": 300}, "b": {"brier": 0.25, "n_games": 300}}
    w = sharpness_weights(stats, min_games=200)
    assert w["a"] == pytest.approx(1.0)
    assert 0 < w["b"] < 1e-3      # 1e-6 / 0.25
