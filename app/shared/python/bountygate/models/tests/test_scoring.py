import math

import pytest

from bountygate.models.scoring import brier_score, calibration_buckets, log_loss_score


def test_brier_hand_computed():
    # ((0.8-1)^2 + (0.4-0)^2) / 2 = (0.04 + 0.16) / 2
    assert brier_score([(0.8, 1), (0.4, 0)]) == pytest.approx(0.10)


def test_brier_empty_is_none():
    assert brier_score([]) is None


def test_log_loss_hand_computed():
    expected = -(math.log(0.8) + math.log(0.6)) / 2
    assert log_loss_score([(0.8, 1), (0.4, 0)]) == pytest.approx(expected)


def test_log_loss_clamps_extremes():
    assert math.isfinite(log_loss_score([(1.0, 0), (0.0, 1)]))


def test_calibration_buckets():
    pairs = [(0.75, 1), (0.78, 1), (0.72, 0), (0.05, 0), (1.0, 1)]
    buckets = {b["prob_bucket"]: b for b in calibration_buckets(pairs)}
    b7 = buckets[0.7]
    assert b7["n"] == 3
    assert b7["predicted_mean"] == pytest.approx((0.75 + 0.78 + 0.72) / 3)
    assert b7["realized_rate"] == pytest.approx(2 / 3)
    assert buckets[0.0]["n"] == 1
    assert buckets[0.9]["n"] == 1     # p=1.0 lands in the top bucket
