import pytest
from bountygate.connectors.base import Connector
from bountygate.connectors.registry import CONNECTORS, _env_int, get_connector


def test_registry_has_expected_sources():
    assert set(CONNECTORS) == {
        "kalshi",
        "polymarket",
        "the_odds_api",
        "the_odds_api_props",
    }


def test_registry_values_are_connectors():
    for conn in CONNECTORS.values():
        assert isinstance(conn, Connector)


def test_registry_keys_match_source_except_props_alias():
    # Every entry's key equals its landing source, except the props alias which
    # deliberately reuses the_odds_api landing source under a distinct key.
    for source, conn in CONNECTORS.items():
        if source == "the_odds_api_props":
            assert conn.source == "the_odds_api"
        else:
            assert conn.source == source


def test_get_connector_returns_instance():
    assert get_connector("polymarket").source == "polymarket"


# ---------------------------------------------------------------------------
# _env_int: tolerant env-var parsing
# ---------------------------------------------------------------------------

def test_env_int_returns_default_when_unset(monkeypatch):
    monkeypatch.delenv("BG_TEST_INT", raising=False)
    assert _env_int("BG_TEST_INT", 42) == 42


def test_env_int_parses_valid_integer(monkeypatch):
    monkeypatch.setenv("BG_TEST_INT", "72")
    assert _env_int("BG_TEST_INT", 42) == 72


def test_env_int_falls_back_to_default_on_malformed_value(monkeypatch, capsys):
    monkeypatch.setenv("BG_TEST_INT", "48h")
    result = _env_int("BG_TEST_INT", 99)
    assert result == 99
    out = capsys.readouterr().out
    assert "48h" in out
    assert "BG_TEST_INT" in out
