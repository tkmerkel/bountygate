"""Round-trip codegen tests.

Hand-authored fixture traces are converted into market config dicts and the
output is asserted against the corresponding entries in the production
selectors/{book}_markets.yaml files.

Volatile fields (validated_at, search_validated where stored on legacy
entries) are excluded from comparison since they're added on save by
SelectorManager and not by codegen.

These tests do NOT require a live browser. They use only fixture JSONL files
in toolkit/recorder/tests/fixtures/ and the production YAMLs.
"""
from __future__ import annotations

import os
import sys
import unittest

REPO_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
)
sys.path.insert(0, REPO_ROOT)

import yaml  # noqa: E402

from toolkit.codegen import betmgm as betmgm_codegen  # noqa: E402
from toolkit.codegen import fanduel as fanduel_codegen  # noqa: E402
from toolkit.recorder.schema import load_trace  # noqa: E402

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
SELECTORS_DIR = os.path.join(REPO_ROOT, "arbitrage_executor", "selectors")

VOLATILE_FIELDS = {"validated_at"}


def _load_market(book: str, key: str) -> dict:
    path = os.path.join(SELECTORS_DIR, f"{book}_markets.yaml")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get(key) or {}


def _drop_volatile(d: dict) -> dict:
    return {k: v for k, v in d.items() if k not in VOLATILE_FIELDS}


class FanDuelRoundTrip(unittest.TestCase):
    def test_player_assists(self):
        path = os.path.join(FIXTURE_DIR, "fanduel_player_assists.jsonl")
        header, records = load_trace(path)
        market_key, cfg = fanduel_codegen.trace_to_config(header, records)
        self.assertEqual(market_key, "player_assists")
        existing = _drop_volatile(_load_market("fanduel", "player_assists"))
        self.assertEqual(cfg["selector_type"], existing["selector_type"])
        self.assertEqual(cfg["search_strategy"], existing["search_strategy"])
        self.assertEqual(cfg["selector_pattern"], existing["selector_pattern"])
        self.assertEqual(cfg["test_player"], existing["test_player"])
        self.assertEqual(cfg["test_line"], existing["test_line"])
        # The trace records one canonical aria-label fragment per market; the
        # production YAML may carry additional hand-added synonyms (e.g.
        # "Player Assists" in addition to "Assists"). Codegen output need
        # only contain the canonical fragment.
        canonical = existing["display_names"][0]
        self.assertIn(canonical, cfg["display_names"])

    def test_player_points_alternate(self):
        path = os.path.join(FIXTURE_DIR, "fanduel_player_points_alternate.jsonl")
        header, records = load_trace(path)
        market_key, cfg = fanduel_codegen.trace_to_config(header, records)
        self.assertEqual(market_key, "player_points_alternate")
        existing = _drop_volatile(_load_market("fanduel", "player_points_alternate"))
        self.assertEqual(cfg["search_strategy"], "alternate_threshold_match")
        self.assertEqual(cfg["search_strategy"], existing["search_strategy"])
        self.assertEqual(cfg["is_alternate"], existing["is_alternate"])
        self.assertEqual(cfg["base_market"], existing["base_market"])
        canonical = existing["display_names"][0]
        self.assertIn(canonical, cfg["display_names"])


class BetMGMRoundTrip(unittest.TestCase):
    def test_player_assists(self):
        path = os.path.join(FIXTURE_DIR, "betmgm_player_assists.jsonl")
        header, records = load_trace(path)
        market_key, cfg = betmgm_codegen.trace_to_config(header, records)
        self.assertEqual(market_key, "player_assists")
        existing = _drop_volatile(_load_market("betmgm", "player_assists"))
        self.assertEqual(cfg["accordion_name"], existing["accordion_name"])
        self.assertEqual(cfg["accordion_selector"], existing["accordion_selector"])
        self.assertEqual(cfg["show_more_selector"], existing["show_more_selector"])
        self.assertEqual(cfg["bet_element_type"], existing["bet_element_type"])
        self.assertEqual(cfg["search_strategy"], existing["search_strategy"])
        self.assertEqual(cfg["test_player"], existing["test_player"])
        self.assertEqual(cfg["test_line"], existing["test_line"])

    def test_player_points_alternate(self):
        path = os.path.join(FIXTURE_DIR, "betmgm_player_points_alternate.jsonl")
        header, records = load_trace(path)
        market_key, cfg = betmgm_codegen.trace_to_config(header, records)
        self.assertEqual(market_key, "player_points_alternate")
        existing = _drop_volatile(_load_market("betmgm", "player_points_alternate"))
        self.assertEqual(cfg["accordion_name"], existing["accordion_name"])
        self.assertEqual(cfg["tab_selector_pattern"], existing["tab_selector_pattern"])
        self.assertEqual(cfg["search_strategy"], existing["search_strategy"])
        self.assertEqual(cfg["is_alternate"], existing["is_alternate"])
        self.assertEqual(cfg["has_threshold_tabs"], existing["has_threshold_tabs"])
        self.assertEqual(cfg["base_market"], existing["base_market"])


class TerminalGuard(unittest.TestCase):
    """Codegen does not depend on terminal=True records, but they MUST be
    present in the FanDuel and BetMGM standard fixtures so the replay engine
    halts before the simulated place-bet step. This test is the contract.
    """

    def test_fanduel_has_terminal(self):
        path = os.path.join(FIXTURE_DIR, "fanduel_player_assists.jsonl")
        _, records = load_trace(path)
        self.assertTrue(any(r.terminal for r in records))

    def test_betmgm_has_terminal(self):
        path = os.path.join(FIXTURE_DIR, "betmgm_player_assists.jsonl")
        _, records = load_trace(path)
        self.assertTrue(any(r.terminal for r in records))


if __name__ == "__main__":
    unittest.main()
