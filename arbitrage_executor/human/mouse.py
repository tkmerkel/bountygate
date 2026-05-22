"""Humanized mouse movement and clicks.

Two big tells we're trying to fight:
1. Instant teleport to click target (no movement events).
2. Identical hover→click micro-timing on every interaction.

Strategy: a quadratic Bezier path through a randomly-offset midpoint,
sampled with ease-out timing (slow start, fast middle, slow end —
matches human muscle dynamics). 10% of moves overshoot the target by
8–22px and correct back. Click dwell + press hold drawn from
lognormal — short, but not constant.

Cursor state is carried across calls so the next move starts where
the last one ended; otherwise every Bezier would start from (0, 0)
and the cross-phase moves would be obviously stitched.
"""

import math
import random
from dataclasses import dataclass


# Min / max number of intermediate mouse-move events along a path.
_MIN_STEPS = 12
_MAX_STEPS = 40
# Pixels per step — driving target density. 1 step per ~10px so that
# a 200px move lands well above the floor (~20 steps) and an 800px
# move saturates at the ceiling (40).
_PX_PER_STEP = 10

# Probability of an overshoot.
_OVERSHOOT_PROB = 0.10
# Overshoot distance (pixels past the target).
_OVERSHOOT_MIN = 8
_OVERSHOOT_MAX = 22

# Bezier midpoint offset — pulls the path off the straight line.
_MID_OFFSET_MIN = 0.05  # 5% of segment length
_MID_OFFSET_MAX = 0.25  # 25% of segment length

# Click dwell (between move-end and mousedown) and press hold
# (between mousedown and mouseup). Lognormal, in milliseconds.
# Median dwell ~60ms, median hold ~75ms; both p95 ~180ms.
_DWELL_MU = math.log(60.0)
_DWELL_SIGMA = 0.45
_HOLD_MU = math.log(75.0)
_HOLD_SIGMA = 0.45


@dataclass
class CursorState:
    """Tracks the cursor's last known position so successive moves start
    where the last one ended. Constructed once per session and threaded
    through ``move_to`` / ``click`` calls.
    """
    position: tuple[float, float] = (0.0, 0.0)


def _step_count(*, distance_px: float) -> int:
    """1 step per ~10px, clamped to [12, 40]."""
    return max(_MIN_STEPS, min(_MAX_STEPS, int(distance_px / _PX_PER_STEP)))


def _ease_out_t(i: int, steps: int) -> float:
    """Ease-out (cubic) timing parameter for step ``i`` of ``steps``.

    Returns a value in [0, 1] that grows fast at the start and slows
    at the end — matching how the cursor decelerates onto the target.
    """
    # steps==1 is unreachable via _step_count (clamped to _MIN_STEPS=12)
    # but kept defensive for direct callers / unit tests.
    raw = i / (steps - 1) if steps > 1 else 1.0
    return 1.0 - (1.0 - raw) ** 3


def _bezier_path(
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    steps: int,
    rng: random.Random,
    overshoot: bool,
) -> list[tuple[float, float]]:
    """Sample a quadratic Bezier from ``start`` to ``end`` with one
    random-offset midpoint, plus optional overshoot.

    The "two control points" in the design spec collapse to one
    midpoint here — a quadratic Bezier has degree 2 (one control
    point); adding a second would make it cubic, which doesn't buy
    us additional realism for the distances involved.
    """
    sx, sy = start
    ex, ey = end
    dx, dy = ex - sx, ey - sy
    length = math.hypot(dx, dy)
    if length < 1.0:
        return [end] * steps

    # Perpendicular offset for the midpoint — gives the path its arc.
    offset_frac = rng.uniform(_MID_OFFSET_MIN, _MID_OFFSET_MAX)
    offset_dist = offset_frac * length * rng.choice([-1.0, 1.0])
    # Unit normal to (dx, dy).
    nx, ny = -dy / length, dx / length
    midpoint = (
        (sx + ex) / 2 + nx * offset_dist,
        (sy + ey) / 2 + ny * offset_dist,
    )

    # Target endpoint — push past if overshooting.
    if overshoot:
        over_dist = rng.uniform(_OVERSHOOT_MIN, _OVERSHOOT_MAX)
        ux, uy = dx / length, dy / length
        target = (ex + ux * over_dist, ey + uy * over_dist)
    else:
        target = end

    pts: list[tuple[float, float]] = []
    for i in range(steps):
        t = _ease_out_t(i, steps)
        # Quadratic Bezier: (1-t)^2 * P0 + 2(1-t)t * P1 + t^2 * P2
        omt = 1.0 - t
        x = omt * omt * sx + 2 * omt * t * midpoint[0] + t * t * target[0]
        y = omt * omt * sy + 2 * omt * t * midpoint[1] + t * t * target[1]
        pts.append((x, y))

    # If we overshot, append a corrective tail back to the true endpoint.
    if overshoot:
        # 5 extra steps to come back.
        correct_steps = 5
        for i in range(1, correct_steps + 1):
            t = i / correct_steps
            x = target[0] * (1 - t) + ex * t
            y = target[1] * (1 - t) + ey * t
            pts.append((x, y))

    return pts


# Used by click() and idle_jitter() in Task 6 — kept here so the
# dwell/hold constants (_DWELL_*, _HOLD_*) live next to their consumer.
def _sample_lognormal_ms(mu: float, sigma: float, rng: random.Random) -> int:
    return max(15, int(math.exp(rng.normalvariate(mu, sigma))))


def move_to(
    page,
    locator,
    *,
    state: CursorState,
    rng: random.Random | None = None,
) -> tuple[float, float]:
    """Move the cursor along a humanized path to a random point inside
    the ``locator``'s bounding box. Updates ``state.position`` to the
    final coordinates.

    Returns the final (x, y) coordinates.

    If the locator has no visible bounding box (off-screen, not yet
    rendered, or zero-area), raises ``ValueError``.

    Note:
        Emits ``_step_count(distance)`` mouse-move events on a clean
        path, plus 5 corrective steps when the path overshoots
        (probability ``_OVERSHOOT_PROB``). Callers that need
        deterministic event counts should be aware of the +5 tail.
    """
    rng = rng or random.Random()
    box = locator.bounding_box()
    if not box or box["width"] <= 0 or box["height"] <= 0:
        raise ValueError("move_to: locator has no visible bounding box")

    # Random landing point inside the box (avoid the literal centre —
    # too constant across runs). Inset 20% on each axis to stay clear
    # of the edge but spread coverage.
    inset_x = box["width"] * 0.2
    inset_y = box["height"] * 0.2
    end_x = box["x"] + inset_x + rng.uniform(0, box["width"] - 2 * inset_x)
    end_y = box["y"] + inset_y + rng.uniform(0, box["height"] - 2 * inset_y)
    end = (end_x, end_y)

    start = state.position
    distance = math.hypot(end[0] - start[0], end[1] - start[1])
    steps = _step_count(distance_px=distance)
    overshoot = rng.random() < _OVERSHOOT_PROB

    pts = _bezier_path(start, end, steps=steps, rng=rng, overshoot=overshoot)
    for (x, y) in pts:
        page.mouse.move(x, y)
        # Tiny inter-step pause — Playwright's mouse.move(steps=N) does
        # this internally for browser-native moves but we want explicit
        # control over the cadence. Uniform (not lognormal) — sub-frame
        # jitter; the macro shape comes from ease-out, not the per-step
        # delay.
        page.wait_for_timeout(rng.randint(8, 18))

    state.position = pts[-1]
    return state.position


def click(
    page,
    locator,
    *,
    state: CursorState,
    rng: random.Random | None = None,
) -> None:
    """Move to ``locator``, dwell, mousedown, hold, mouseup.

    The dwell-and-hold timing is sampled from lognormal so it varies
    across clicks without being suspiciously constant.

    Raises:
        ValueError: propagated from ``move_to`` when the locator has
            no visible bounding box.
    """
    rng = rng or random.Random()
    move_to(page, locator, state=state, rng=rng)

    dwell_ms = _sample_lognormal_ms(_DWELL_MU, _DWELL_SIGMA, rng)
    page.wait_for_timeout(dwell_ms)
    page.mouse.down()
    hold_ms = _sample_lognormal_ms(_HOLD_MU, _HOLD_SIGMA, rng)
    page.wait_for_timeout(hold_ms)
    page.mouse.up()


def idle_jitter(
    page,
    *,
    state: CursorState,
    rng: random.Random | None = None,
    duration_ms: int = 600,
) -> None:
    """Drift the cursor by a few pixels over ``duration_ms``.

    Mimics the small involuntary movements humans make while a page is
    loading or while reading. Intended to be wired into long-wait
    paths in later tasks (``intra_book_idle`` in Task 10; long
    ``settle`` calls in Tasks 17-18).

    Anchor sampling caps drift at ~42px (sqrt(2)*30) from the
    starting position so the cursor never accidentally hovers an
    adjacent button and triggers a tooltip.

    Note: each move has a 50ms minimum; ``duration_ms`` values
    below ~150ms will overshoot the requested duration because the
    floor dominates.
    """
    rng = rng or random.Random()
    # 2-6 small moves over the duration.
    n_moves = rng.randint(2, 6)
    per_move_ms = max(50, duration_ms // n_moves)
    # Anchor sampling: every move is drawn from a box centered on the
    # ORIGINAL position, not the previous sample. This caps cumulative
    # drift at ~42px (the diagonal of the ±30 box) instead of letting
    # a random walk wander far enough to hover over an adjacent button.
    ax, ay = state.position
    nx, ny = ax, ay
    for _ in range(n_moves):
        nx = ax + rng.uniform(-30, 30)
        ny = ay + rng.uniform(-30, 30)
        page.mouse.move(nx, ny)
        page.wait_for_timeout(per_move_ms)
    state.position = (nx, ny)
