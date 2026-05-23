"""Unit tests for _bet_placer_helpers."""
import os
import tempfile

import pytest

from _fakes import FakeElement, FakeLocator, FakePage
from _bet_placer_helpers import (
    screenshot,
    first_visible,
    dump_miss_context,
    poll_until,
    with_screenshot_on_error,
)
from bet_placer import BetPlacerError, BetPlacerSkipError


def test_first_visible_returns_first_matching_visible_locator():
    page = FakePage(locators={
        "a": FakeLocator([]),                                  # zero count, skip
        "b": FakeLocator([FakeElement(visible=False)]),        # invisible, skip
        "c": FakeLocator([FakeElement(visible=True)]),         # MATCH
        "d": FakeLocator([FakeElement(visible=True)]),         # not reached
    })
    result = first_visible(page, ["a", "b", "c", "d"])
    assert result is not None
    assert result is page.locators["c"].first


def test_first_visible_returns_none_when_no_match():
    page = FakePage(locators={
        "a": FakeLocator([]),
        "b": FakeLocator([FakeElement(visible=False)]),
    })
    assert first_visible(page, ["a", "b"]) is None


def test_first_visible_skips_selectors_that_raise():
    class RaisingPage(FakePage):
        def locator(self, selector):
            if selector == "boom":
                raise RuntimeError("simulated playwright error")
            return super().locator(selector)

    page = RaisingPage(locators={
        "ok": FakeLocator([FakeElement(visible=True)]),
    })
    result = first_visible(page, ["boom", "ok"])
    assert result is page.locators["ok"].first


def test_first_visible_logs_when_label_provided(capsys):
    page = FakePage(locators={
        "x": FakeLocator([FakeElement(visible=True)]),
    })
    first_visible(page, ["x"], label="Found thing", site="fanduel")
    captured = capsys.readouterr()
    assert "[FANDUEL] Found thing via x" in captured.out


def test_screenshot_returns_expected_filename_format():
    page = FakePage()
    with tempfile.TemporaryDirectory() as tmp:
        path = screenshot(page, tmp, "fanduel", "search_results")
        assert path.startswith(os.path.join(tmp, "fanduel_search_results_"))
        assert path.endswith(".png")


def test_screenshot_does_not_raise_when_page_screenshot_fails(capsys):
    class FailingPage(FakePage):
        def screenshot(self, *args, **kwargs):
            raise RuntimeError("disk full")

    with tempfile.TemporaryDirectory() as tmp:
        path = screenshot(FailingPage(), tmp, "betmgm", "tag")
        assert "betmgm_tag_" in path
    captured = capsys.readouterr()
    assert "Screenshot failed" in captured.out


# ---------------------------------------------------------------------------
# dump_miss_context
# ---------------------------------------------------------------------------


def _make_named_element(name: str) -> FakeElement:
    """FakeElement that returns ``name`` from both aria-label and text."""
    return FakeElement(
        visible=True,
        text=name,
        attributes={"aria-label": name},
    )


def test_dump_miss_context_dumps_aria_and_button_matches(capsys):
    aria_el = _make_named_element("Devin Vassell - Points Over 14.5")
    btn_el = _make_named_element("Devin Vassell")
    page = FakePage(locators={
        '[aria-label*="Devin Vassell"]': FakeLocator([aria_el]),
        'button:has-text("Devin Vassell")': FakeLocator([btn_el]),
    })
    dump_miss_context(page, site="betmgm", player_name="Devin Vassell")
    out = capsys.readouterr().out
    assert "[BETMGM] aria-labels mentioning 'Devin Vassell' (1):" in out
    assert "Devin Vassell - Points Over 14.5" in out
    assert "[BETMGM] buttons mentioning 'Devin Vassell' (1):" in out


def test_dump_miss_context_prints_zero_count_when_no_matches(capsys):
    page = FakePage()
    dump_miss_context(page, site="fanduel", player_name="Unknown Player")
    out = capsys.readouterr().out
    assert "aria-labels mentioning 'Unknown Player' (0): []" in out
    assert "buttons mentioning 'Unknown Player' (0): []" in out


def test_dump_miss_context_includes_extra_locators(capsys):
    pick = _make_named_element("Devin Vassell yes 15+")
    page = FakePage(locators={
        'ms-event-pick:has-text("Devin Vassell")': FakeLocator([pick]),
    })
    dump_miss_context(
        page,
        site="betmgm",
        player_name="Devin Vassell",
        extra_locators=[
            ("ms-event-pick elements", 'ms-event-pick:has-text("Devin Vassell")'),
        ],
    )
    out = capsys.readouterr().out
    assert "[BETMGM] ms-event-pick elements mentioning 'Devin Vassell' (1):" in out
    assert "yes 15+" in out


def test_dump_miss_context_never_raises_on_page_explosion():
    class BoomPage:
        def locator(self, sel):
            raise RuntimeError("CDP disconnected")
    # Must NOT raise.
    dump_miss_context(BoomPage(), site="betmgm", player_name="x")


# ---------------------------------------------------------------------------
# poll_until
# ---------------------------------------------------------------------------


def test_poll_until_returns_when_condition_becomes_true():
    calls = {"count": 0}
    def cond():
        calls["count"] += 1
        return calls["count"] >= 3

    settles: list[tuple[str, str]] = []
    def settle_fake(page, category, *, rng=None):
        settles.append((id(page), category))
        return 100

    poll_until(
        page=FakePage(),
        settle_fn=settle_fake,
        settle_category="slip_update",
        rng=None,
        condition=cond,
        attempts=5,
    )
    assert calls["count"] == 3
    assert len(settles) == 3
    assert all(s[1] == "slip_update" for s in settles)


def test_poll_until_raises_BetPlacerError_on_timeout():
    settles: list[int] = []
    def settle_fake(page, category, *, rng=None):
        settles.append(1)
        return 100

    with pytest.raises(BetPlacerError, match="slip never showed"):
        poll_until(
            page=FakePage(),
            settle_fn=settle_fake,
            settle_category="slip_update",
            rng=None,
            condition=lambda: False,
            attempts=3,
            on_timeout_message="slip never showed",
        )
    assert len(settles) == 3


def test_poll_until_fires_screenshot_on_timeout():
    fired = []
    def settle_fake(page, category, *, rng=None):
        return 0

    with pytest.raises(BetPlacerError):
        poll_until(
            page=FakePage(),
            settle_fn=settle_fake,
            settle_category="slip_update",
            rng=None,
            condition=lambda: False,
            attempts=2,
            on_timeout_screenshot=lambda: fired.append(True),
        )
    assert fired == [True]


def test_poll_until_does_not_fire_screenshot_on_success():
    fired = []
    def settle_fake(page, category, *, rng=None):
        return 0

    poll_until(
        page=FakePage(),
        settle_fn=settle_fake,
        settle_category="slip_update",
        rng=None,
        condition=lambda: True,
        attempts=2,
        on_timeout_screenshot=lambda: fired.append(True),
    )
    assert fired == []


# ---------------------------------------------------------------------------
# with_screenshot_on_error
# ---------------------------------------------------------------------------


class _ScreenshotSpy:
    """Stand-in for a BetPlacer; records ``_screenshot`` calls."""

    def __init__(self):
        self.shots: list[str] = []

    def _screenshot(self, tag: str) -> str:
        self.shots.append(tag)
        return f"/tmp/{tag}.png"


def test_with_screenshot_on_error_wraps_generic_exception_as_BetPlacerError():
    placer = _ScreenshotSpy()
    with pytest.raises(BetPlacerError, match="wager entry failed: boom"):
        with with_screenshot_on_error(placer, "wager_failed", "wager entry failed"):
            raise RuntimeError("boom")
    assert placer.shots == ["wager_failed"]


def test_with_screenshot_on_error_preserves_BetPlacerError():
    placer = _ScreenshotSpy()
    with pytest.raises(BetPlacerError, match="^original$"):
        with with_screenshot_on_error(placer, "tag", "outer msg"):
            raise BetPlacerError("original")
    # Existing BetPlacerError doesn't get re-wrapped, so no screenshot.
    assert placer.shots == []


def test_with_screenshot_on_error_preserves_BetPlacerSkipError():
    placer = _ScreenshotSpy()
    with pytest.raises(BetPlacerSkipError, match="^structural$"):
        with with_screenshot_on_error(placer, "tag", "outer msg"):
            raise BetPlacerSkipError("structural")
    assert placer.shots == []


def test_with_screenshot_on_error_swallows_screenshot_failure():
    class ExplodingPlacer:
        def _screenshot(self, tag: str) -> str:
            raise RuntimeError("disk full")
    with pytest.raises(BetPlacerError, match="ctx failed"):
        with with_screenshot_on_error(ExplodingPlacer(), "tag", "ctx failed"):
            raise RuntimeError("inner")


def test_with_screenshot_on_error_no_exception_passes_through():
    placer = _ScreenshotSpy()
    with with_screenshot_on_error(placer, "tag", "would not fire"):
        x = 1 + 1
    assert placer.shots == []
    assert x == 2
