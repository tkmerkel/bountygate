"""Tests for bountygate.arb.markets — ported from archive pre-pivot pipeline."""
from __future__ import annotations

import os
import sys

_PKG_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

import pytest

from bountygate.arb.markets import (
    ARB_MARKETS,
    ARB_MARKETS_BY_SPORT,
    ARB_MARKET_BLACKLIST,
    ARB_SPORTS,
    SPORT_TITLE,
    _NBA_PROP_MARKETS,
    props_sports,
)


# ---------------------------------------------------------------------------
# Alternate-key integrity: every _alternate market must have its base in same list
# ---------------------------------------------------------------------------

class TestAlternateKeyIntegrity:
    @pytest.mark.parametrize("sport", list(ARB_MARKETS_BY_SPORT.keys()))
    def test_alternate_has_base_in_same_sport(self, sport):
        markets = ARB_MARKETS_BY_SPORT[sport]
        market_set = set(markets)
        for m in markets:
            if m.endswith("_alternate"):
                base = m[: -len("_alternate")]
                assert base in market_set, (
                    f"[{sport}] '{m}' has no base key '{base}' in the same sport list"
                )


# ---------------------------------------------------------------------------
# props_sports() default behaviour
# ---------------------------------------------------------------------------

class TestPropsSportsDefault:
    def test_default_includes_five_sports(self, monkeypatch):
        monkeypatch.delenv("BG_PROPS_SPORTS", raising=False)
        result = props_sports()
        expected = {
            "basketball_nba",
            "baseball_mlb",
            "icehockey_nhl",
            "americanfootball_nfl",
            "basketball_wnba",
        }
        assert set(result.keys()) == expected

    def test_default_excludes_ncaab(self, monkeypatch):
        monkeypatch.delenv("BG_PROPS_SPORTS", raising=False)
        result = props_sports()
        assert "basketball_ncaab" not in result


# ---------------------------------------------------------------------------
# props_sports() respects BG_PROPS_SPORTS env var
# ---------------------------------------------------------------------------

class TestPropsSportsEnvVar:
    def test_env_filters_to_known_sports_only(self, monkeypatch, capsys):
        monkeypatch.setenv("BG_PROPS_SPORTS", "baseball_mlb,bogus_sport")
        result = props_sports()
        assert list(result.keys()) == ["baseball_mlb"]
        # Unknown key should produce a warning print
        captured = capsys.readouterr()
        assert "bogus_sport" in captured.out

    def test_env_single_sport(self, monkeypatch):
        monkeypatch.setenv("BG_PROPS_SPORTS", "icehockey_nhl")
        result = props_sports()
        assert list(result.keys()) == ["icehockey_nhl"]
        assert result["icehockey_nhl"] == ARB_MARKETS_BY_SPORT["icehockey_nhl"]


# ---------------------------------------------------------------------------
# Blacklist membership
# ---------------------------------------------------------------------------

class TestBlacklist:
    def test_blacklist_exact_members(self):
        assert ARB_MARKET_BLACKLIST == frozenset({
            "pitcher_strikeouts",
            "pitcher_strikeouts_alternate",
        })


# ---------------------------------------------------------------------------
# Ported value spot-checks
# ---------------------------------------------------------------------------

class TestPortedValues:
    def test_mlb_tuple_exact(self):
        expected = (
            "batter_singles", "batter_singles_alternate",
            "batter_doubles", "batter_doubles_alternate",
            "batter_stolen_bases", "batter_stolen_bases_alternate",
        )
        assert ARB_MARKETS_BY_SPORT["baseball_mlb"] == expected

    def test_nba_ncaab_wnba_share_nba_markets(self):
        """NBA, NCAAB, and WNBA all map to the same _NBA_PROP_MARKETS tuple."""
        assert ARB_MARKETS_BY_SPORT["basketball_nba"] == _NBA_PROP_MARKETS
        assert ARB_MARKETS_BY_SPORT["basketball_ncaab"] == _NBA_PROP_MARKETS
        assert ARB_MARKETS_BY_SPORT["basketball_wnba"] == _NBA_PROP_MARKETS
        # Identity check — they truly share the same object
        assert ARB_MARKETS_BY_SPORT["basketball_nba"] is _NBA_PROP_MARKETS
        assert ARB_MARKETS_BY_SPORT["basketball_ncaab"] is _NBA_PROP_MARKETS
        assert ARB_MARKETS_BY_SPORT["basketball_wnba"] is _NBA_PROP_MARKETS

    def test_arb_markets_is_sorted_union(self):
        expected = tuple(
            sorted({m for markets in ARB_MARKETS_BY_SPORT.values() for m in markets})
        )
        assert ARB_MARKETS == expected

    def test_arb_sports_tuple(self):
        assert "basketball_nba" in ARB_SPORTS
        assert "basketball_ncaab" in ARB_SPORTS
        assert "basketball_wnba" in ARB_SPORTS
        assert "icehockey_nhl" in ARB_SPORTS
        assert "americanfootball_nfl" in ARB_SPORTS
        assert "baseball_mlb" in ARB_SPORTS

    def test_sport_title_keys_match_arb_sports(self):
        assert set(SPORT_TITLE.keys()) == set(ARB_SPORTS)

    def test_no_arb_bookmakers(self):
        """ARB_BOOKMAKERS was dropped in this port — it must not exist."""
        import bountygate.arb.markets as mod
        assert not hasattr(mod, "ARB_BOOKMAKERS")
