"""Unit tests for _bet_placer_helpers."""
import os
import tempfile

import pytest

from _fakes import FakeElement, FakeLocator, FakePage
from _bet_placer_helpers import (
    _ACCORDION_FUZZY_THRESHOLD,
    screenshot,
    first_visible,
)


def test_accordion_threshold_constant():
    assert _ACCORDION_FUZZY_THRESHOLD == 80


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
