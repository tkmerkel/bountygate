import random
import statistics
from datetime import date

import pytest

from human.typing import TypingProfile, SAME_FINGER_PAIRS


def test_profile_seed_is_daily_stable():
    """Two profiles built on the same date produce the same first 100 delays."""
    p1 = TypingProfile.for_date(date(2026, 5, 21))
    p2 = TypingProfile.for_date(date(2026, 5, 21))
    seq1 = [p1.next_delay_ms("a", "b") for _ in range(100)]
    seq2 = [p2.next_delay_ms("a", "b") for _ in range(100)]
    assert seq1 == seq2


def test_profile_changes_across_days():
    """Different dates → different rhythm (cheap sanity, not stat-significant)."""
    p1 = TypingProfile.for_date(date(2026, 5, 21))
    p2 = TypingProfile.for_date(date(2026, 5, 22))
    seq1 = [p1.next_delay_ms("a", "b") for _ in range(50)]
    seq2 = [p2.next_delay_ms("a", "b") for _ in range(50)]
    assert seq1 != seq2


def test_delay_distribution_hits_target_median_and_p95():
    """Median ~120ms (range 100-140), p95 ~220ms (range 180-280) for neutral bigrams."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    samples = [p.next_delay_ms("a", "b") for _ in range(5000)]
    med = statistics.median(samples)
    p95 = sorted(samples)[int(0.95 * len(samples))]
    assert 100 <= med <= 140, f"median {med} out of band"
    assert 180 <= p95 <= 280, f"p95 {p95} out of band"


def test_same_finger_pairs_are_slower():
    """Same-finger bigrams (e.g. 'rt' on QWERTY left index) → ~1.3x median delay."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    same_finger = [p.next_delay_ms("r", "t") for _ in range(2000)]
    neutral = [p.next_delay_ms("a", "l") for _ in range(2000)]
    assert statistics.median(same_finger) > statistics.median(neutral) * 1.15
    assert ("r", "t") in SAME_FINGER_PAIRS


def test_common_bigram_is_faster():
    """Common English bigrams (e.g. 'th') → ~0.85x median delay."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    common = [p.next_delay_ms("t", "h") for _ in range(2000)]
    neutral = [p.next_delay_ms("a", "l") for _ in range(2000)]
    assert statistics.median(common) < statistics.median(neutral) * 0.95


def test_typo_rate_is_three_percent_on_long_text():
    """3% chance per character to fire a typo-and-correction on text length >= 6."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    typo_count = sum(1 for _ in range(10000) if p.should_typo(text_length=10))
    assert 200 <= typo_count <= 450, f"typo rate looks off: {typo_count / 10000}"


def test_typo_disabled_on_short_text():
    """No typos on text length < 6 — too small a sample to be plausible."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    typo_count = sum(1 for _ in range(1000) if p.should_typo(text_length=5))
    assert typo_count == 0


def test_adjacent_typo_char_uppercase_uses_lowercase_neighbours():
    """A typo on 'A' looks up 'a' and returns a lowercase neighbour."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    result = p.adjacent_typo_char("A")
    assert result in "qwsz", f"expected a neighbour of 'a', got {result!r}"


def test_adjacent_typo_char_non_letter_falls_back_to_random_letter():
    """A typo on a digit or hyphen returns SOME lowercase a-z without raising."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    for ch in ("1", "-", "."):
        result = p.adjacent_typo_char(ch)
        assert result in "abcdefghijklmnopqrstuvwxyz", (
            f"non-letter {ch!r} → {result!r} not in a-z"
        )


def test_adjacent_typo_char_empty_string_does_not_raise():
    """Empty intended char hits the random-letter fallback without exception."""
    p = TypingProfile.for_date(date(2026, 5, 21))
    result = p.adjacent_typo_char("")
    assert result in "abcdefghijklmnopqrstuvwxyz"


from human.typing import TypingProfile, humanized_type


class FakeKeyboard:
    def __init__(self):
        self.keys: list[str] = []
        self.types: list[str] = []

    def type(self, value, **kwargs):
        self.types.append(value)
        self.keys.append(value)

    def press(self, key, **kwargs):
        self.keys.append(f"<{key}>")


class FakeLocator:
    def __init__(self, attrs=None):
        self.fills: list[str] = []
        self._attrs = attrs or {}

    def fill(self, value, **kwargs):
        self.fills.append(value)

    def get_attribute(self, name):
        return self._attrs.get(name)


class FakePage:
    def __init__(self, locator_attrs=None):
        self.keyboard = FakeKeyboard()
        self.locator_obj = FakeLocator(attrs=locator_attrs)
        self.waited_ms: list[int] = []

    def wait_for_timeout(self, ms):
        self.waited_ms.append(int(ms))


def test_humanized_type_writes_each_character():
    """Each intended character ends up in the keyboard log."""
    page = FakePage()
    profile = TypingProfile(rng=random.Random(0))
    # Force should_typo to always return False by patching profile.
    profile.should_typo = lambda **kw: False  # type: ignore

    humanized_type(page, page.locator_obj, "Anthony Edwards", profile=profile)

    # Compose the typed text from non-bracket entries.
    typed = "".join(k for k in page.keyboard.keys if not k.startswith("<"))
    assert typed == "Anthony Edwards"


def test_humanized_type_emits_typo_and_correction():
    """When typo fires, a stray char + backspace appear before the intended char."""
    page = FakePage()
    profile = TypingProfile(rng=random.Random(0))
    # Force one typo on the 5th character (text length is 15 so >= 6).
    call_count = {"n": 0}
    def stub_typo(**kw):
        call_count["n"] += 1
        return call_count["n"] == 5
    profile.should_typo = stub_typo  # type: ignore
    profile.adjacent_typo_char = lambda intended: "x"  # type: ignore

    humanized_type(page, page.locator_obj, "Anthony Edwards", profile=profile)

    # Expect to see <Backspace> in the key log AND an "x" before that.
    keys = page.keyboard.keys
    backspace_idx = keys.index("<Backspace>")
    # The character just before Backspace must be the stray 'x'.
    assert keys[backspace_idx - 1] == "x"

    # Order: type(stray) → settle(micro_pause) → press(Backspace) →
    # settle(micro_pause) → type(intended). The micro_pause band is
    # 180-420ms; two extra waits in that band must fall AROUND the
    # backspace event.
    micro_pause_band = (180, 420)
    waits_in_band = [w for w in page.waited_ms if micro_pause_band[0] <= w <= micro_pause_band[1]]
    assert len(waits_in_band) >= 2, (
        f"expected ≥2 micro_pause waits during typo+correction, got {waits_in_band}"
    )


def test_humanized_type_falls_back_to_fill_for_react_combobox():
    """When the locator has role='combobox', the helper also calls .fill() at end."""
    page = FakePage(locator_attrs={"role": "combobox"})
    profile = TypingProfile(rng=random.Random(0))
    profile.should_typo = lambda **kw: False  # type: ignore

    humanized_type(page, page.locator_obj, "Anthony", profile=profile)

    assert page.locator_obj.fills == ["Anthony"]


def test_humanized_type_no_fill_when_locator_is_plain_input():
    """No fill fallback when the locator has no React heuristic markers."""
    page = FakePage(locator_attrs={"type": "text"})
    profile = TypingProfile(rng=random.Random(0))
    profile.should_typo = lambda **kw: False  # type: ignore

    humanized_type(page, page.locator_obj, "Anthony", profile=profile)

    assert page.locator_obj.fills == []


def test_humanized_type_swallows_fill_fallback_errors(capsys):
    """A locator.fill() exception must not abort the bet flow.

    Documented contract: keystroke typing already succeeded by this
    point, so a React-fill failure is recoverable. Pins the fail-soft
    behavior in case someone later "cleans up" the bare except.
    """
    class FailingLocator(FakeLocator):
        def fill(self, value, **kwargs):
            raise RuntimeError("simulated React detachment")

    page = FakePage()
    page.locator_obj = FailingLocator(attrs={"role": "combobox"})
    profile = TypingProfile(rng=random.Random(0))
    profile.should_typo = lambda **kw: False  # type: ignore

    # Must not raise.
    humanized_type(page, page.locator_obj, "Anthony", profile=profile)

    captured = capsys.readouterr()
    assert "React fill fallback failed" in captured.out


def test_humanized_type_emits_pre_submit_dwell():
    """The function must end with a pre_submit_dwell (450-950ms band)
    so the caller doesn't need to add their own dwell before Enter."""
    page = FakePage()
    profile = TypingProfile(rng=random.Random(0))
    profile.should_typo = lambda **kw: False  # type: ignore
    humanized_type(page, page.locator_obj, "hi", profile=profile)
    # The LAST wait_for_timeout entry must fall in the pre_submit_dwell band.
    assert 450 <= page.waited_ms[-1] <= 950, page.waited_ms
