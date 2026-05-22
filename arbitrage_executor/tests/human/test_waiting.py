import random

import pytest

from human.waiting import settle, WAIT_CATEGORIES


class FakePage:
    def __init__(self):
        self.waited_ms: list[int] = []

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))


def test_settle_each_category_falls_in_documented_band():
    """settle(category) must sample within the documented band for that category."""
    rng = random.Random(42)
    for category, (lo_ms, hi_ms) in WAIT_CATEGORIES.items():
        page = FakePage()
        # Sample many times to confirm we stay in band even at the tails.
        for _ in range(200):
            settle(page, category, rng=rng)
        assert all(lo_ms <= w <= hi_ms for w in page.waited_ms), (
            f"{category}: out of band, got {sorted(set(page.waited_ms))[:5]}..."
        )


def test_settle_rejects_unknown_category():
    page = FakePage()
    with pytest.raises(KeyError):
        settle(page, "made_up_category", rng=random.Random(0))


def test_settle_jittered_false_uses_band_midpoint():
    """jittered=False is the escape hatch for tests that want deterministic timing."""
    page = FakePage()
    settle(page, "page_load", jittered=False)
    lo, hi = WAIT_CATEGORIES["page_load"]
    assert page.waited_ms == [(lo + hi) // 2]
