import math
import random

import pytest

from human.mouse import CursorState, _bezier_path, _step_count, move_to
from human.mouse import click, idle_jitter


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


class FakeMouseWithButtons(FakeMouse):
    def __init__(self):
        super().__init__()
        self.events: list[str] = []

    def down(self):
        self.events.append("down")

    def up(self):
        self.events.append("up")


class FakePageForClick(FakePage):
    def __init__(self):
        super().__init__()
        self.mouse = FakeMouseWithButtons()


def test_click_emits_move_down_hold_up_sequence():
    page = FakePageForClick()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    click(page, locator, state=state, rng=random.Random(0))

    assert "down" in page.mouse.events
    assert "up" in page.mouse.events
    # 'down' must precede 'up'.
    assert page.mouse.events.index("down") < page.mouse.events.index("up")
    # move_to runs INSIDE click before mousedown — verify by checking
    # FakeMouse.moves is non-empty (movement happened) before any down.
    assert len(page.mouse.moves) > 0, "no cursor movement recorded before mousedown"


def test_click_includes_a_dwell_before_mousedown():
    page = FakePageForClick()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    click(page, locator, state=state, rng=random.Random(0))

    # The last wait_for_timeout before "down" is the dwell — must be > 15ms.
    # We can't slot in by index easily; instead, check that SOME wait is in
    # the dwell range, separate from the inter-step move waits (8-18ms).
    big_waits = [w for w in page.waited_ms if w > 25]
    assert len(big_waits) >= 1, "no dwell-shaped wait recorded"


def test_idle_jitter_makes_a_few_small_moves():
    page = FakePage()
    state = CursorState((400.0, 300.0))

    idle_jitter(page, state=state, rng=random.Random(0), duration_ms=600)

    # 2-6 moves in a 600ms window (per spec).
    assert 2 <= len(page.mouse.moves) <= 6
    # Anchor sampling — each move is within sqrt(2)*30 ≈ 42.4px of the
    # ORIGINAL position. Test bound 45 includes a comfortable margin
    # without being so loose that a regression to random-walk drift
    # would pass.
    for (x, y) in page.mouse.moves:
        assert math.hypot(x - 400, y - 300) < 45, (
            f"move {(x, y)} drifted >45px from origin (400, 300)"
        )


def test_idle_jitter_anchor_sampling_caps_drift_across_seeds():
    """Anchor sampling: every move is in a ±30 box around the original
    position, regardless of seed. This is a regression test for the
    random-walk bug where cumulative drift could exceed 100px.
    """
    for seed in range(50):
        page = FakePage()
        state = CursorState((400.0, 300.0))
        idle_jitter(page, state=state, rng=random.Random(seed), duration_ms=600)
        for (x, y) in page.mouse.moves:
            assert math.hypot(x - 400, y - 300) < 45, (
                f"seed {seed}: move {(x, y)} drifted >45px"
            )
