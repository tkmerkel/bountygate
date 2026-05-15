"""Tests for text_match.fuzzy_contains.

Run: python -m pytest arbitrage_executor/tests/test_text_match.py -v
"""

import pytest

from text_match import fuzzy_contains


class TestFuzzyContainsExactMatches:
    def test_exact_substring_matches(self):
        assert fuzzy_contains("Anthony Edwards scored 25 points", "Anthony Edwards")

    def test_case_insensitive(self):
        assert fuzzy_contains("ANTHONY EDWARDS", "anthony edwards")

    def test_no_match_returns_false(self):
        assert not fuzzy_contains("LeBron James", "Stephen Curry")


class TestFuzzyContainsRealWorldVariations:
    def test_curly_apostrophe_matches_straight(self):
        # The DOM sometimes renders with a unicode curly apostrophe
        assert fuzzy_contains("De’Andre Hunter made 3s", "De'Andre Hunter")

    def test_straight_apostrophe_matches_curly(self):
        assert fuzzy_contains("De'Andre Hunter", "De’Andre Hunter")

    def test_period_free_abbreviation_matches(self):
        # "A.J. Brown" vs "AJ Brown" is a real case in NFL player names
        assert fuzzy_contains("Player row: AJ Brown total receiving", "A.J. Brown")

    def test_jr_suffix_with_without_period(self):
        assert fuzzy_contains("Tim Hardaway Jr scored 15", "Tim Hardaway Jr.")

    def test_extra_whitespace_tolerated(self):
        assert fuzzy_contains("Player:  Rudy  Gobert  ", "Rudy Gobert")


class TestFuzzyContainsRejectsFalseMatches:
    def test_similar_but_different_players_rejected(self):
        # Two distinct players — should NOT match. Anthony Davis vs Anthony Edwards
        assert not fuzzy_contains("Anthony Davis", "Anthony Edwards", threshold=90)

    def test_empty_needle_returns_false(self):
        # Empty needle should never "contain" anything — that would match
        # every string and cause catastrophic mis-bets.
        assert not fuzzy_contains("Anthony Edwards", "")

    def test_empty_haystack_returns_false(self):
        assert not fuzzy_contains("", "Anthony Edwards")

    def test_threshold_is_respected(self):
        # At a high threshold, noisy partials should fail
        assert not fuzzy_contains("Luka Doncic", "Anthony Edwards", threshold=95)


class TestFuzzyContainsThreshold:
    def test_default_threshold_is_85(self):
        # Document the default. Changing the default is a behavior change.
        import inspect

        import text_match

        sig = inspect.signature(text_match.fuzzy_contains)
        assert sig.parameters["threshold"].default == 85

    def test_lower_threshold_accepts_more_variants(self):
        # Threshold of 70 is permissive — a short near-match should pass
        assert fuzzy_contains("Jayson Tatum Jr", "Jayson Tatun", threshold=70)

    def test_threshold_100_requires_perfect_partial_match(self):
        # At 100, a perfect substring passes
        assert fuzzy_contains("The player is Rudy Gobert tonight", "Rudy Gobert", threshold=100)
        # But anything off is rejected
        assert not fuzzy_contains("Rudi Gobert", "Rudy Gobert", threshold=100)
