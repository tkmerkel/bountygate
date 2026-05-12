"""Fuzzy text-matching helpers for DOM selectors.

Thin wrapper over ``rapidfuzz`` for the narrow cases where the bot needs to
tolerate minor text variation in the sportsbook DOM (curly vs. straight
apostrophes, period-free name abbreviations, UI-label rewording).

Not for use on CSS selectors, XPath, or attribute identifiers — those need
exact matches.
"""

from __future__ import annotations

import re

from rapidfuzz import fuzz

# Strips punctuation that varies between how players/labels are written in
# different DOM contexts ("A.J." vs "AJ", "Jr." vs "Jr"). Keeps apostrophes
# and hyphens since those don't vary purely as stylistic noise.
_PUNCT_TO_STRIP = re.compile(r"[.,]")


def _normalize(s: str) -> str:
    s = _PUNCT_TO_STRIP.sub("", s)
    s = " ".join(s.split())
    return s.lower()


def fuzzy_score(haystack: str, needle: str) -> int:
    """Return the partial-ratio match score (0-100) of ``needle`` against
    ``haystack``. Useful when the caller wants to rank candidates rather
    than threshold-test them.

    Returns 0 if either input is empty.
    """
    if not haystack or not needle:
        return 0
    return int(fuzz.partial_ratio(_normalize(needle), _normalize(haystack)))


def fuzzy_contains(haystack: str, needle: str, threshold: int = 85) -> bool:
    """Return True if ``needle`` fuzzy-matches any subsequence of ``haystack``.

    Uses ``rapidfuzz.fuzz.partial_ratio`` which finds the best-matching
    substring of ``haystack`` of length ``len(needle)`` and scores it 0-100.

    Both strings are lowercased before comparison. Empty inputs always return
    False — an empty needle must not "match" everything, which would cause
    catastrophic mis-bets.

    The default threshold of 85 is tuned for player-name matching: it absorbs
    curly/straight apostrophe differences and period-free abbreviations
    ("A.J. Brown" -> "AJ Brown") while rejecting distinct names ("Anthony
    Davis" vs. "Anthony Edwards"). Callers can raise the threshold for
    high-stakes paths or lower it for noisy UI labels.

    Args:
        haystack: The string to search within (e.g. DOM row text).
        needle: The string to look for (e.g. expected player name).
        threshold: Integer 0-100. Minimum match score to return True.

    Returns:
        True if the best partial match scores at or above ``threshold``.
    """
    if not haystack or not needle:
        return False
    score = fuzz.partial_ratio(_normalize(needle), _normalize(haystack))
    return score >= threshold
