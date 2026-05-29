import pytest

from pick_matcher import (
    Pick, parse_pick, parse_threshold, line_equals, select_unique,
    NoPickError, AmbiguousPickError,
)
from bet_placer import BetPlacerError


# ---- parse_pick: the core "1.5 vs 11.5" bug class ----
def test_parse_pick_betmgm_over_decimal():
    assert parse_pick("O 11.5  2.00") == Pick(side="over", line=11.5, odds=2.00)

def test_parse_pick_betmgm_under_decimal():
    assert parse_pick("U 3.5 1.85") == Pick(side="under", line=3.5, odds=1.85)

def test_parse_pick_full_word_sides_and_embedded():
    assert parse_pick("LeBron James, Over, 25.5, Points").side == "over"
    assert parse_pick("LeBron James, Over, 25.5, Points").line == 25.5
    assert parse_pick("Anthony Davis, Under, 9.5, Rebounds").side == "under"

def test_parse_pick_captures_full_line_token_not_substring():
    # The whole point: "O 1" must NOT read 11.5 as 1, and 1.5 != 11.5.
    assert parse_pick("O 11.5 2.00").line == 11.5
    assert parse_pick("O 1 2.00").line == 1.0
    assert parse_pick("Over 1.5 Points").line == 1.5

def test_parse_pick_integer_line():
    assert parse_pick("O 1 1.95") == Pick(side="over", line=1.0, odds=1.95)

def test_parse_pick_non_pick_returns_none():
    assert parse_pick("Show More") is None
    assert parse_pick("") is None
    assert parse_pick("Stephen Curry") is None  # no side+number


# ---- parse_threshold: the "5+ vs 15+" bug class ----
def test_parse_threshold_exact():
    assert parse_threshold("5+ Stolen Bases") == 5
    assert parse_threshold("15+ Points") == 15

def test_parse_threshold_does_not_confuse_5_with_15():
    # "5+" must not be found inside "15+".
    assert parse_threshold("15+ Points") == 15  # not 5

def test_parse_threshold_one_verb_labels():
    assert parse_threshold("To Hit A Single") == 1
    assert parse_threshold("To Record An RBI") == 1
    assert parse_threshold("To Record A Run") == 1
    assert parse_threshold("To Record A Total Base") == 1


def test_every_fanduel_verb_phrase_is_recognized():
    """Cross-consistency guard: every FanDuel threshold-1 verb phrase that the
    alt-path query builds (FANDUEL_THRESHOLD_ONE_LABELS) must be recognized by
    parse_threshold as threshold 1. Otherwise the FD alt path would COLLECT the
    tile but select_unique would fail to match it (the gap that left
    batter_runs / batter_total_bases broken before the lists were aligned)."""
    from bet_placer_fanduel import FANDUEL_THRESHOLD_ONE_LABELS
    for display, (verb, article, noun) in FANDUEL_THRESHOLD_ONE_LABELS.items():
        phrase = f"{verb} {article} {noun}"
        assert parse_threshold(f"{phrase}, Some Player, 4.90") == 1, (
            f"parse_threshold does not recognize {phrase!r} (display={display!r}) "
            f"— add it to pick_matcher._THRESHOLD_ONE_PHRASES"
        )

def test_parse_threshold_none():
    assert parse_threshold("Over 4.5 Points") is None
    assert parse_threshold("") is None


# ---- line_equals ----
def test_line_equals():
    assert line_equals(11.5, 11.5)
    assert not line_equals(1.5, 11.5)
    assert line_equals(25.0, 25.0)


# ---- select_unique: exactly one or raise, no fallback ----
def _items(*texts):
    # (element, text) pairs; element is just the text for assertion convenience
    return [(t, t) for t in texts]

def test_select_unique_picks_exact_line_and_side():
    items = _items("O 11.5 2.00", "U 11.5 1.85", "O 1.5 1.50")
    assert select_unique(items, 11.5, "over") == "O 11.5 2.00"
    assert select_unique(items, 1.5, "over") == "O 1.5 1.50"

def test_select_unique_no_match_raises_nopick():
    items = _items("O 11.5 2.00", "U 11.5 1.85")
    with pytest.raises(NoPickError):
        select_unique(items, 2.5, "over")

def test_select_unique_no_wrong_side_fallback():
    # Only an under pick exists; asking for over must RAISE, never return it.
    items = _items("U 3.5 1.85")
    with pytest.raises(NoPickError):
        select_unique(items, 3.5, "over")

def test_select_unique_ambiguous_raises():
    items = _items("O 11.5 2.00", "O 11.5 2.01")
    with pytest.raises(AmbiguousPickError):
        select_unique(items, 11.5, "over")

def test_select_unique_threshold_mode():
    items = _items("5+ Stolen Bases", "15+ Stolen Bases", "To Hit A Single")
    # line 4.5 -> threshold 5
    assert select_unique(items, 4.5, "over", threshold=True) == "5+ Stolen Bases"
    # line 0.5 -> threshold 1 (verb label)
    assert select_unique(items, 0.5, "over", threshold=True) == "To Hit A Single"

def test_errors_are_betplacererror_subclasses():
    assert issubclass(NoPickError, BetPlacerError)
    assert issubclass(AmbiguousPickError, BetPlacerError)
