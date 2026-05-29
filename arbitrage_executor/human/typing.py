"""Per-session typing profile.

Goal: stop typing instantly into search boxes. Real users have:
    - variable inter-key delays drawn from a fat-tailed distribution
      (lognormal works well; median ~120ms, p95 ~220ms)
    - faster transitions for common bigrams (th, he, in, er, an, ...)
    - slower transitions for same-finger bigrams (rt, hj, oo, rr, ...)
    - occasional typos with a corrective backspace

The profile seed rotates daily so the same machine has consistent
rhythm within a day (matches real users — your typing today is more
like your typing yesterday than someone else's typing yesterday) but
drifts across days (rotation makes per-session fingerprinting harder).
"""

import math
import random
from dataclasses import dataclass, field
from datetime import date

from human.waiting import settle


# Mu and sigma for the lognormal inter-key delay.
# Target: median 120ms, p95 ~220ms.
#   median = exp(mu)               → mu = ln(120) ≈ 4.787
#   p95    = exp(mu + 1.645*sigma) → choose sigma so p95 ≈ 220
# Solving: sigma = (ln(220) - mu) / 1.645 ≈ (5.394 - 4.787) / 1.645 ≈ 0.369
# Tuning headroom: sigma is fragile; if p95 creeps above 280 in
# integration tests, drop sigma to 0.34.
_MU = math.log(120.0)
_SIGMA = 0.369

# Bound delays — never less than 35ms (human floor), never more than
# 1500ms (a pause that long looks like distraction, which we DO want
# occasionally, but not for every keystroke).
_MIN_DELAY_MS = 35
_MAX_DELAY_MS = 1500


# Common English bigrams (top ~15 by frequency). Hit with 0.85x speed multiplier.
COMMON_BIGRAMS: set[tuple[str, str]] = {
    ("t", "h"), ("h", "e"), ("i", "n"), ("e", "r"), ("a", "n"),
    ("r", "e"), ("o", "n"), ("a", "t"), ("e", "n"), ("n", "d"),
    ("t", "i"), ("e", "s"), ("o", "r"), ("t", "e"), ("o", "f"),
}

# Same-finger bigrams on QWERTY (chosen subset — the ones that show up
# in player names: e.g. "wood" has "oo", "barrett" has "rr"). Hit with
# 1.3x speed multiplier (slow). Source: standard QWERTY finger map.
SAME_FINGER_PAIRS: set[tuple[str, str]] = {
    # Left index: r, t, f, g, v, b
    ("r", "t"), ("t", "r"), ("f", "g"), ("g", "f"), ("v", "b"), ("b", "v"),
    ("r", "f"), ("f", "r"), ("t", "g"), ("g", "t"),
    # Right index: y, u, h, j, n, m
    ("y", "u"), ("u", "y"), ("h", "j"), ("j", "h"), ("n", "m"), ("m", "n"),
    # Doubled letters — same finger by definition.
    ("o", "o"), ("e", "e"), ("l", "l"), ("r", "r"), ("t", "t"), ("s", "s"),
    ("n", "n"), ("p", "p"), ("d", "d"), ("f", "f"), ("g", "g"), ("a", "a"),
}


_TYPO_RATE_PER_CHAR = 0.03
_MIN_TEXT_LEN_FOR_TYPOS = 6


@dataclass
class TypingProfile:
    """Per-session typing rhythm.

    Use ``TypingProfile.for_date(date.today())`` in the executor;
    inject a custom ``rng`` for tests.
    """

    rng: random.Random = field(default_factory=random.Random)

    @classmethod
    def for_date(cls, d: date) -> "TypingProfile":
        """Build a profile seeded by the given calendar date.

        Same machine, same date → same sequence of delays. Different
        dates → different rhythm.
        """
        seed = d.toordinal()
        return cls(rng=random.Random(seed))

    @classmethod
    def for_today(cls) -> "TypingProfile":
        return cls.for_date(date.today())

    def _base_delay_ms(self) -> float:
        """Sample one lognormal inter-key delay, bounded."""
        ms = math.exp(self.rng.normalvariate(_MU, _SIGMA))
        return max(_MIN_DELAY_MS, min(_MAX_DELAY_MS, ms))

    def next_delay_ms(self, prev_char: str, next_char: str) -> int:
        """Return the delay (ms) to wait BEFORE typing ``next_char``,
        given ``prev_char`` (or empty string for the first character).
        """
        base = self._base_delay_ms()
        if not prev_char:
            return int(base)
        pair = (prev_char.lower(), next_char.lower())
        if pair in COMMON_BIGRAMS:
            base *= 0.85
        elif pair in SAME_FINGER_PAIRS:
            base *= 1.30
        return max(_MIN_DELAY_MS, int(base))

    def should_typo(self, *, text_length: int) -> bool:
        """Roll the 3% typo die. Returns False for text_length < 6
        regardless of the roll — typos on short text are too noticeable
        and there's no meaningful cover.
        """
        if text_length < _MIN_TEXT_LEN_FOR_TYPOS:
            return False
        return self.rng.random() < _TYPO_RATE_PER_CHAR

    def adjacent_typo_char(self, intended: str) -> str:
        """For a typo, pick a plausible neighbouring key on QWERTY.

        Falls back to a random lowercase letter when intended isn't
        a key with a known neighbour (e.g. digits, hyphens).
        """
        neighbours = _QWERTY_NEIGHBOURS.get(intended.lower())
        if not neighbours:
            return self.rng.choice("abcdefghijklmnopqrstuvwxyz")
        return self.rng.choice(neighbours)


# Minimal QWERTY neighbour map — enough for player-name typos to look
# like fat-finger errors rather than random gibberish.
_QWERTY_NEIGHBOURS: dict[str, str] = {
    "q": "wa", "w": "qeas", "e": "wrsd", "r": "etdf", "t": "ryfg",
    "y": "tugh", "u": "yihj", "i": "uojk", "o": "ipkl", "p": "ol",
    "a": "qwsz", "s": "awedxz", "d": "serfcx", "f": "drtgvc",
    "g": "ftyhbv", "h": "gyujnb", "j": "huikmn", "k": "jiolm",
    "l": "kop", "z": "asx", "x": "zsdc", "c": "xdfv", "v": "cfgb",
    "b": "vghn", "n": "bhjm", "m": "njk",
}


# Presence (any non-empty value) signals React linkage to a popup/listbox.
_REACT_HEURISTIC_ATTRS = ("aria-controls",)
# Exact role match (ARIA combobox pattern).
_REACT_HEURISTIC_ROLES = ("combobox",)
# Case-insensitive substring match on data-testid.
_REACT_HEURISTIC_TESTIDS = ("search",)


def _looks_react_controlled(locator) -> bool:
    """Heuristic: does this locator look like a React-controlled input?

    React-controlled inputs sometimes ignore character-by-character
    keypress events and only update their internal state on a fill()
    or input event with the whole value. We can't tell with certainty
    without instrumentation, so we use signals that are common in the
    FanDuel and BetMGM search-input components.
    """
    try:
        for attr in _REACT_HEURISTIC_ATTRS:
            if locator.get_attribute(attr):
                return True
        role = locator.get_attribute("role")
        if role and role in _REACT_HEURISTIC_ROLES:
            return True
        testid = locator.get_attribute("data-testid") or ""
        if any(marker in testid.lower() for marker in _REACT_HEURISTIC_TESTIDS):
            return True
    except Exception:
        return False
    return False


def humanized_type(
    page,
    locator,
    text: str,
    *,
    profile: TypingProfile | None = None,
) -> None:
    """Type ``text`` into the input ``locator`` one character at a time
    with humanized delays, optional typo-and-correction, and a
    pre-submit dwell.

    Caller is responsible for pressing Enter / submitting; this
    function intentionally stops after the last character + dwell.

    Args:
        page: Playwright Page (or test fake) — needs ``keyboard.press``,
            ``keyboard.type``, ``wait_for_timeout``.
        locator: Playwright Locator (or test fake) for the target
            input. Used both as the React-heuristic target and as the
            ``fill()`` fallback receiver.
        text: the text to type.
        profile: optional TypingProfile. Defaults to today's profile.

    Preconditions:
        locator must already be focused. ``humanized_type`` does NOT
        click or focus the locator — it only uses ``locator`` as the
        React-heuristic target and the ``.fill()`` fallback receiver.
        Caller is responsible for focusing first (typically via
        ``human.mouse.click()`` from Task 5).
    """
    profile = profile or TypingProfile.for_today()
    text_len = len(text)
    prev = ""
    for ch in text:
        page.wait_for_timeout(profile.next_delay_ms(prev, ch))
        if profile.should_typo(text_length=text_len):
            stray = profile.adjacent_typo_char(ch)
            page.keyboard.type(stray)
            settle(page, "micro_pause", rng=profile.rng)
            page.keyboard.press("Backspace")
            settle(page, "micro_pause", rng=profile.rng)
        page.keyboard.type(ch)
        prev = ch

    # Pre-submit dwell — the human-paced "did I type this right?" beat.
    settle(page, "pre_submit_dwell", rng=profile.rng)

    # React fallback: if the locator looks like a React-controlled
    # input, also call .fill() to ensure the framework state is set.
    # This is belt-and-suspenders — many React inputs DO accept the
    # keypress events, but some only commit state on input events.
    if _looks_react_controlled(locator):
        try:
            # Short timeout: the per-keystroke typing above already set the
            # value. This .fill() only commits framework state for inputs that
            # ignore keypress events. On a constantly-re-rendering slip the
            # element-stability check would otherwise burn the full 30s default
            # before this belt-and-suspenders call gives up.
            locator.fill(text, timeout=2000)
        except Exception as e:
            print(
                f"[human.typing] React fill fallback failed (keystroke typing "
                f"already completed): {e}; continuing"
            )
