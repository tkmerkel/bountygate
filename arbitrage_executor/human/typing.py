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
