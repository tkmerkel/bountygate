import math
import random

import pytest

from human.mouse import CursorState, _bezier_path, _step_count, move_to


def test_step_count_scales_with_distance():
    assert _step_count(distance_px=20) == 12  # clamped at min
    assert _step_count(distance_px=800) == 40  # clamped at max
    assert 12 < _step_count(distance_px=200) < 40


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

    def bounding_box(self):
        return self._box


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
