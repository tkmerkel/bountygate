import random

import pytest

from human.mouse import CursorState
from human.navigation import click_through


class FakeLocator:
    def __init__(self, count=0, visible=True, box=None):
        self._count = count
        self._visible = visible
        self._box = box or {"x": 100, "y": 200, "width": 60, "height": 20}
        self.clicked = False

    def count(self):
        return self._count

    def is_visible(self):
        return self._visible

    def bounding_box(self):
        return self._box

    @property
    def first(self):
        return self

    def click(self):
        self.clicked = True


class FakeMouse:
    def __init__(self):
        self.moves = []

    def move(self, x, y, **kw):
        self.moves.append((x, y))

    def down(self):
        pass

    def up(self):
        pass


class FakePage:
    def __init__(self, locator_by_selector=None):
        self.url = "about:blank"
        self.gotos: list[str] = []
        self.mouse = FakeMouse()
        self.waited_ms: list[int] = []
        self._locators = locator_by_selector or {}

    def goto(self, url, **kw):
        self.url = url
        self.gotos.append(url)

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))

    def evaluate(self, *args, **kw):
        return None  # scroll calls

    def locator(self, sel):
        return self._locators.get(sel, FakeLocator(count=0))


def test_click_through_navigates_to_start_url_first():
    """Even when the link selector matches, we still load start_url first."""
    page = FakePage(locator_by_selector={"a.event": FakeLocator(count=1, visible=True)})
    click_through(
        page,
        start_url="https://example.com/sports",
        link_selector="a.event",
        fallback_url="https://example.com/sports/events/123",
        state=CursorState(),
        rng=random.Random(0),
    )
    # First nav must be the start_url.
    assert page.gotos[0] == "https://example.com/sports"


def test_click_through_clicks_link_when_found():
    locator = FakeLocator(count=1, visible=True)
    page = FakePage(locator_by_selector={"a.event": locator})
    click_through(
        page,
        start_url="https://example.com/sports",
        link_selector="a.event",
        fallback_url="https://example.com/sports/events/123",
        state=CursorState(),
        rng=random.Random(0),
    )
    # The mouse should have moved to the link's bounding box at some point.
    assert any(100 <= x <= 160 for (x, _) in page.mouse.moves)


def test_click_through_falls_back_to_direct_goto_when_link_missing(capsys):
    page = FakePage(locator_by_selector={"a.event": FakeLocator(count=0)})
    click_through(
        page,
        start_url="https://example.com/sports",
        link_selector="a.event",
        fallback_url="https://example.com/sports/events/123",
        state=CursorState(),
        rng=random.Random(0),
    )
    # Loud log on fallback.
    captured = capsys.readouterr()
    assert "fallback" in captured.out.lower()
    # Final navigation is the fallback URL.
    assert page.gotos[-1] == "https://example.com/sports/events/123"
