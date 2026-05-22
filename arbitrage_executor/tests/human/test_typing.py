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
