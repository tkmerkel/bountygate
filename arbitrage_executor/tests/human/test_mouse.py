import math
import random

import pytest

from human.mouse import CursorState, _bezier_path, _step_count, move_to
from human.mouse import click


def test_step_count_scales_with_distance():
    # Bounds lowered to [8, 16] on 2026-05-29 to cut CDP round-trips on
    # renderer-saturated slips (see human.mouse._MIN_STEPS/_MAX_STEPS).
    assert _step_count(distance_px=20) == 8    # clamped at min
    assert _step_count(distance_px=800) == 16  # clamped at max
    assert 8 < _step_count(distance_px=120) < 16


def test_bezier_path_starts_and_ends_on_endpoints():
    rng = random.Random(0)
    pts = _bezier_path((0.0, 0.0), (100.0, 100.0), steps=20, rng=rng, overshoot=False)
    assert pts[0] == pytest.approx((0.0, 0.0), abs=0.5)
    assert pts[-1] == pytest.approx((100.0, 100.0), abs=0.5)
    assert len(pts) == 20


def test_bezier_path_is_not_a_straight_line():
    """The curve should deviate from the straight-line midpoint by a few pixels."""
    rng = random.Random(0)
    pts = _bezier_path((0.0, 0.0), (200.0, 0.0), steps=30, rng=rng, overshoot=False)
    midpoint = pts[len(pts) // 2]
    # Straight line midpoint y would be 0; the Bezier should pull off-axis.
    assert abs(midpoint[1]) > 2.0, f"midpoint {midpoint} is too straight"


def test_bezier_path_with_overshoot_passes_target():
    """Overshoot path has a point whose distance from target exceeds the endpoint distance."""
    rng = random.Random(0)
    pts = _bezier_path((0.0, 0.0), (100.0, 0.0), steps=30, rng=rng, overshoot=True)
    distances = [math.dist(p, (100.0, 0.0)) for p in pts]
    # Some intermediate point must be farther from target than start/end.
    assert max(distances[5:-5]) > 5.0


def test_cursor_state_tracks_position_across_moves():
    """A CursorState carries position between moves so the next path starts there."""
    state = CursorState()
    assert state.position == (0.0, 0.0)
    state.position = (100.0, 50.0)
    assert state.position == (100.0, 50.0)


class FakeMouse:
    def __init__(self):
        self.moves: list[tuple[float, float]] = []

    def move(self, x, y, *, steps=None):
        self.moves.append((x, y))


class FakePage:
    def __init__(self):
        self.mouse = FakeMouse()
        self.waited_ms: list[int] = []

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))


class FakeLocator:
    def __init__(self, box):
        self._box = box
        self.click_calls: list[dict] = []

    def bounding_box(self):
        return self._box

    def click(self, **kwargs):
        self.click_calls.append(kwargs)


def test_move_to_traces_path_and_updates_cursor_state():
    page = FakePage()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    move_to(page, locator, state=state, rng=random.Random(0))

    # Should have called mouse.move many times — at least 12 (min steps).
    assert len(page.mouse.moves) >= 12
    # Final position should be inside the target bbox.
    final_x, final_y = page.mouse.moves[-1]
    assert 200 <= final_x <= 280
    assert 100 <= final_y <= 130
    # Cursor state mirrors the final mouse position.
    assert state.position == (final_x, final_y)


def test_bezier_path_handles_zero_length_segment():
    """When start ≈ end, return a flat list of N copies of end."""
    pts = _bezier_path(
        (100.0, 100.0),
        (100.3, 100.4),
        steps=15,
        rng=random.Random(0),
        overshoot=False,
    )
    assert len(pts) == 15
    assert all(p == (100.3, 100.4) for p in pts)


def test_move_to_raises_when_locator_has_no_box():
    """A None bounding_box (off-screen / detached) raises ValueError."""
    page = FakePage()
    state = CursorState()
    with pytest.raises(ValueError, match="bounding box"):
        move_to(page, FakeLocator(None), state=state, rng=random.Random(0))


def test_move_to_raises_when_locator_has_zero_area_box():
    """A zero-area box (mid-animation hidden row) raises ValueError."""
    page = FakePage()
    state = CursorState()
    box = {"x": 0, "y": 0, "width": 0, "height": 0}
    with pytest.raises(ValueError, match="bounding box"):
        move_to(page, FakeLocator(box), state=state, rng=random.Random(0))


def test_overshoot_path_has_corrective_tail():
    """overshoot=True appends 5 extra corrective steps after the main path."""
    rng = random.Random(0)
    pts = _bezier_path(
        (0.0, 0.0),
        (100.0, 0.0),
        steps=20,
        rng=rng,
        overshoot=True,
    )
    assert len(pts) == 25, f"expected 20 main + 5 corrective steps, got {len(pts)}"


def test_click_moves_then_invokes_locator_click():
    """click() must run the humanized movement first, then dispatch via
    locator.click() so React/synthetic-event handlers actually fire."""
    page = FakePage()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    click(page, locator, state=state, rng=random.Random(0))

    # Humanized cursor movement happened first.
    assert len(page.mouse.moves) > 0, "no cursor movement recorded before click"
    # Exactly one locator.click dispatch — the final event handoff.
    assert len(locator.click_calls) == 1


def test_click_passes_a_lognormal_hold_delay_to_locator_click():
    """The mousedown→mouseup hold time is preserved as locator.click's
    ``delay`` so the event sequence still has lognormal jitter."""
    page = FakePage()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    click(page, locator, state=state, rng=random.Random(0))

    kwargs = locator.click_calls[0]
    assert "delay" in kwargs
    # Lognormal hold should never collapse to 0; floor is 15ms.
    assert kwargs["delay"] >= 15
    # And shouldn't be wildly long — p95 of the lognormal is ~180ms.
    assert kwargs["delay"] < 1000


def test_click_includes_a_dwell_before_dispatch():
    page = FakePage()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    click(page, locator, state=state, rng=random.Random(0))

    # Some recorded wait must exceed the inter-step move floor (8-18ms)
    # — that's the pre-click dwell.
    big_waits = [w for w in page.waited_ms if w > 25]
    assert len(big_waits) >= 1, "no dwell-shaped wait recorded"


