import random

import pytest

from human.session import warmup_browse, SITE_HOMEPAGES


class FakeMouse:
    def __init__(self):
        self.moves = []

    def move(self, x, y, **kw):
        self.moves.append((x, y))

    def down(self):
        pass

    def up(self):
        pass


class FakeLocator:
    def __init__(self, count=0):
        self._count = count

    def count(self):
        return self._count

    @property
    def first(self):
        return self

    def is_visible(self):
        return True

    def bounding_box(self):
        return {"x": 100, "y": 100, "width": 50, "height": 20}


class FakePage:
    def __init__(self):
        self.url = "about:blank"
        self.gotos = []
        self.scrolled = []
        self.mouse = FakeMouse()
        self.waited_ms = []

    def goto(self, url, **kw):
        self.url = url
        self.gotos.append(url)

    def evaluate(self, expr, *args, **kw):
        if "scrollBy" in expr:
            self.scrolled.append(expr)
        return None

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))

    def go_back(self, **kw):
        pass

    def locator(self, sel):
        return FakeLocator(count=0)


def test_warmup_loads_the_sites_homepage():
    page = FakePage()
    warmup_browse(page, site="fanduel", rng=random.Random(0))
    assert SITE_HOMEPAGES["fanduel"] in page.gotos


def test_warmup_includes_at_least_one_scroll():
    page = FakePage()
    warmup_browse(page, site="betmgm", rng=random.Random(0))
    assert len(page.scrolled) >= 1


def test_warmup_total_wait_is_in_band_12_to_35_seconds():
    """Aggregate wait_for_timeout calls should land between 12s and 35s."""
    page = FakePage()
    warmup_browse(page, site="fanduel", rng=random.Random(0))
    total_ms = sum(page.waited_ms)
    assert 12000 <= total_ms <= 35000, f"warmup ran for {total_ms}ms"


def test_warmup_rejects_unknown_site():
    page = FakePage()
    with pytest.raises(KeyError):
        warmup_browse(page, site="unknown", rng=random.Random(0))


from human.errors import (
    SlipDrainedDuringIdleError,
    FdOddsDriftedDuringIdleError,
)
from human.session import intra_book_idle


def _ok_check_slip_has_bet():
    return True


def _slip_drained_check():
    return False


def test_intra_book_idle_runs_only_for_fanduel():
    page = FakePage()
    # Should raise ValueError if called for non-fanduel.
    with pytest.raises(ValueError):
        intra_book_idle(
            page,
            site="betmgm",
            check_slip_has_bet=_ok_check_slip_has_bet,
            current_fd_odds=2.0,
            read_fd_odds=lambda: 2.0,
            rng=random.Random(0),
        )


def test_intra_book_idle_total_duration_is_in_band():
    """Idle window should be 8-25s total (a bit shorter than warmup)."""
    page = FakePage()
    intra_book_idle(
        page,
        site="fanduel",
        check_slip_has_bet=_ok_check_slip_has_bet,
        current_fd_odds=2.10,
        read_fd_odds=lambda: 2.10,
        rng=random.Random(0),
    )
    total_ms = sum(page.waited_ms)
    assert 8000 <= total_ms <= 25000


def test_intra_book_idle_raises_when_slip_drained():
    page = FakePage()
    with pytest.raises(SlipDrainedDuringIdleError):
        intra_book_idle(
            page,
            site="fanduel",
            check_slip_has_bet=_slip_drained_check,
            current_fd_odds=2.10,
            read_fd_odds=lambda: 2.10,
            rng=random.Random(0),
        )


def test_intra_book_idle_raises_when_odds_drifted_beyond_epsilon():
    page = FakePage()
    with pytest.raises(FdOddsDriftedDuringIdleError) as exc:
        intra_book_idle(
            page,
            site="fanduel",
            check_slip_has_bet=_ok_check_slip_has_bet,
            current_fd_odds=2.10,
            read_fd_odds=lambda: 1.99,  # |Δ| = 0.11 > default 0.05
            rng=random.Random(0),
            epsilon=0.05,
        )
    assert exc.value.old_odds == 2.10
    assert exc.value.new_odds == 1.99


def test_intra_book_idle_tolerates_small_drift():
    page = FakePage()
    intra_book_idle(
        page,
        site="fanduel",
        check_slip_has_bet=_ok_check_slip_has_bet,
        current_fd_odds=2.10,
        read_fd_odds=lambda: 2.13,  # |Δ| = 0.03 < default 0.05
        rng=random.Random(0),
        epsilon=0.05,
    )  # no raise
