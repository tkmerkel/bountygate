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
        # Records the ``steps`` kwarg per call so fast-mode tests can sum
        # the TOTAL dispatched mousemove events (the H2 event budget),
        # which differs from len(moves) (the call count) once we use
        # native interpolated moves.
        self.steps: list[int] = []

    def move(self, x, y, *, steps=None):
        self.moves.append((x, y))
        self.steps.append(steps or 1)


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


# ── Fast (low-event) move mode ────────────────────────────────────────
# fast=True replaces the dense 8-16 single-point mouse.move loop with
# ≤3 native interpolated page.mouse.move(steps=k) calls totalling ≤4
# dispatched events, for renderer-saturated betslips. These tests lock
# both budgets (call count AND event count) and confirm the trace stays
# off-axis and ends on the TRUE endpoint (never the overshoot point).


def test_fast_move_emits_few_native_calls_and_events(monkeypatch):
    # Pin no-overshoot so the budget is the deterministic 2-call / 4-event
    # case; the overshoot variant is asserted separately below.
    monkeypatch.setattr("human.mouse._OVERSHOOT_PROB", 0.0)
    page = FakePage()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    move_to(page, locator, state=state, rng=random.Random(0), fast=True)

    # Call budget: at most 3 native moves (here 2, no overshoot).
    assert len(page.mouse.moves) <= 3
    # Event budget: total dispatched mousemove events ≤ 4 (the H2 lever).
    assert sum(page.mouse.steps) <= 4
    # Final landing point is inside the target bbox.
    final_x, final_y = page.mouse.moves[-1]
    assert 200 <= final_x <= 280
    assert 100 <= final_y <= 130
    # Cursor state mirrors the final mouse position.
    assert state.position == (final_x, final_y)


def test_fast_move_is_off_axis(monkeypatch):
    """The intermediate (midpoint) move must deviate from the straight
    start→end line — a coarse polyline, not a teleporting straight shot."""
    monkeypatch.setattr("human.mouse._OVERSHOOT_PROB", 0.0)
    page = FakePage()
    state = CursorState()  # starts at (0, 0)
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    move_to(page, locator, state=state, rng=random.Random(0), fast=True)

    # No-overshoot fast path: moves[0] = midpoint, moves[-1] = end.
    midpoint = page.mouse.moves[0]
    start = (0.0, 0.0)
    end = page.mouse.moves[-1]
    # Perpendicular distance of the midpoint from the start→end line.
    ax, ay = end[0] - start[0], end[1] - start[1]
    seg_len = math.hypot(ax, ay)
    perp = abs(ax * (start[1] - midpoint[1]) - (start[0] - midpoint[0]) * ay) / seg_len
    assert perp > 2.0, f"midpoint {midpoint} too close to straight line (perp={perp:.2f})"


def test_fast_move_overshoot_budget(monkeypatch):
    """With overshoot forced, fast mode uses ≤3 calls / ≤4 events and the
    cursor still settles on the TRUE endpoint, not the overshoot point."""
    monkeypatch.setattr("human.mouse._OVERSHOOT_PROB", 1.0)
    page = FakePage()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    move_to(page, locator, state=state, rng=random.Random(0), fast=True)

    assert len(page.mouse.moves) <= 3
    assert sum(page.mouse.steps) <= 4
    # Last move is the true endpoint; the overshoot hop (moves[1]) is
    # a distinct, farther point we must NOT end on.
    assert state.position == page.mouse.moves[-1]
    assert state.position != page.mouse.moves[1]


def test_click_fast_threads_through():
    """click(fast=True) still dispatches exactly one locator.click with a
    lognormal hold delay and a pre-click dwell — the click micro-timing is
    identical across modes; only the approach-path density changes."""
    page = FakePage()
    state = CursorState()
    locator = FakeLocator({"x": 200, "y": 100, "width": 80, "height": 30})

    click(page, locator, state=state, rng=random.Random(0), fast=True)

    # Low-event approach (≤3 native moves) still happened before the click.
    assert 0 < len(page.mouse.moves) <= 3
    assert sum(page.mouse.steps) <= 4
    # Exactly one click dispatch, with a lognormal hold delay preserved.
    assert len(locator.click_calls) == 1
    kwargs = locator.click_calls[0]
    assert kwargs.get("delay", 0) >= 15
    # A dwell-shaped wait was still recorded.
    big_waits = [w for w in page.waited_ms if w > 25]
    assert len(big_waits) >= 1, "no dwell-shaped wait recorded in fast mode"


