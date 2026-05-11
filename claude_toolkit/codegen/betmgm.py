"""BetMGM codegen: trace -> selectors/betmgm_markets.yaml entry.

Reads a recorder trace and produces a market config dict shaped to match
arbitrage_executor/selectors/betmgm_markets.yaml. Output is written via
arbitrage_executor.selector_finder.SelectorManager.save_market_config.

BetMGM's standard schema (e.g. player_points):
    accordion_name:      str           # market accordion header text
    accordion_selector:  str           # button[dsaccordiontoggle]:has-text("...")
    show_more_selector:  str           # constant: ms-option-panel-bottom-action:has-text("Show More")
    bet_element_type:    'ms-event-pick'
    search_strategy:     'player_container_then_line'
    search_validated:    true
    test_player:         str
    test_line:           float
    validated_at:        str           # added by SelectorManager

BetMGM's alternate schema (e.g. player_points_alternate):
    accordion_name:        str
    accordion_selector:    str
    tab_selector_pattern:  'button:has-text("{threshold}+")'
    show_more_selector:    str
    bet_element_type:      'ms-event-pick'
    search_strategy:       'alternate_tab_then_player'
    is_alternate:          true
    has_threshold_tabs:    true
    base_market:           str

Trace records are interpreted by phase tag:
    phase=nav            navigate to event/betfinder URL
    phase=search         fill home_team in betfinder
    phase=select_market  click the accordion header (and, for alternates, the
                         threshold tab — recorder distinguishes by record order
                         or by the element text matching r'\\d+\\+')
    phase=select_bet     click the ms-event-pick element
"""
from __future__ import annotations

import re
from typing import Optional

from claude_toolkit.recorder.schema import TraceHeader, TraceRecord, load_trace

SHOW_MORE_SELECTOR = 'ms-option-panel-bottom-action:has-text("Show More")'
BET_ELEMENT_TYPE = "ms-event-pick"

_THRESHOLD_RE = re.compile(r"^(\d+)\+$")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_HAS_TEXT_RE = re.compile(r'has-text\("([^"]+)"\)')


class CodegenError(Exception):
    pass


def _select_market_clicks(records: list[TraceRecord]) -> list[TraceRecord]:
    return [r for r in records if r.kind == "click" and r.phase == "select_market"]


def _is_threshold_tab(rec: TraceRecord) -> bool:
    text = _record_text(rec)
    return bool(text and _THRESHOLD_RE.match(text.strip()))


def _record_text(rec: TraceRecord) -> Optional[str]:
    if rec.element_signature and rec.element_signature.text:
        return rec.element_signature.text
    if rec.text:
        return rec.text
    if rec.selector:
        m = _HAS_TEXT_RE.search(rec.selector)
        if m:
            return m.group(1)
    return None


def _accordion_record(records: list[TraceRecord]) -> TraceRecord:
    for r in _select_market_clicks(records):
        if not _is_threshold_tab(r):
            return r
    raise CodegenError("no accordion click (phase=select_market) in trace")


def _threshold_record(records: list[TraceRecord]) -> Optional[TraceRecord]:
    for r in _select_market_clicks(records):
        if _is_threshold_tab(r):
            return r
    return None


def _bet_record(records: list[TraceRecord]) -> TraceRecord:
    for r in records:
        if r.kind == "click" and r.phase == "select_bet":
            return r
    raise CodegenError("no bet click (phase=select_bet) in trace")


def _search_fill(records: list[TraceRecord]) -> Optional[TraceRecord]:
    for r in records:
        if r.kind == "fill" and (r.phase == "search" or r.phase is None):
            if r.value:
                return r
    return None


def _accordion_selector(text: str) -> str:
    return f'button[dsaccordiontoggle]:has-text("{text}")'


def _extract_test_line(records: list[TraceRecord]) -> Optional[float]:
    """Pull the bet line from the bet element's selector or text."""
    for r in records:
        if r.kind != "click" or r.phase != "select_bet":
            continue
        text = _record_text(r) or ""
        sel = r.selector or ""
        for source in (text, sel):
            for m in _NUMBER_RE.finditer(source):
                try:
                    return float(m.group(0))
                except ValueError:
                    continue
    return None


def trace_to_config(header: TraceHeader, records: list[TraceRecord]) -> tuple[str, dict]:
    if header.book != "betmgm":
        raise CodegenError(f"trace book is {header.book!r}, expected 'betmgm'")
    market_key = header.market
    if not market_key:
        raise CodegenError("trace header missing market")

    is_alternate = market_key.endswith("_alternate")
    if is_alternate:
        return market_key, _alternate_config(market_key, records)
    return market_key, _standard_config(market_key, records)


def _standard_config(market_key: str, records: list[TraceRecord]) -> dict:
    acc = _accordion_record(records)
    accordion_text = _record_text(acc)
    if not accordion_text:
        raise CodegenError("accordion click record had no text/element_signature")

    cfg: dict = {
        "accordion_name": accordion_text,
        "accordion_selector": _accordion_selector(accordion_text),
        "show_more_selector": SHOW_MORE_SELECTOR,
        "bet_element_type": BET_ELEMENT_TYPE,
        "search_strategy": "player_container_then_line",
        "search_validated": True,
    }
    fill = _search_fill(records)
    if fill:
        cfg["test_player"] = fill.value
    line = _extract_test_line(records)
    if line is not None:
        cfg["test_line"] = line
    return cfg


def _alternate_config(market_key: str, records: list[TraceRecord]) -> dict:
    base_market = market_key[: -len("_alternate")]
    acc = _accordion_record(records)
    accordion_text = _record_text(acc)
    if not accordion_text:
        raise CodegenError("alternate accordion click had no text")

    tab_pattern = 'button:has-text("{threshold}+")'

    return {
        "accordion_name": accordion_text,
        "accordion_selector": _accordion_selector(accordion_text),
        "tab_selector_pattern": tab_pattern,
        "show_more_selector": SHOW_MORE_SELECTOR,
        "bet_element_type": BET_ELEMENT_TYPE,
        "search_strategy": "alternate_tab_then_player",
        "is_alternate": True,
        "has_threshold_tabs": True,
        "base_market": base_market,
    }


def save_from_trace(trace_path: str, *, overwrite: bool = False) -> tuple[str, dict, bool]:
    """Load a trace, derive config, persist via SelectorManager.

    Returns (market_key, config, written).
    """
    import os
    from arbitrage_executor.selector_finder import SelectorManager

    header, records = load_trace(trace_path)
    market_key, cfg = trace_to_config(header, records)

    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    executor_dir = os.path.join(repo_root, "arbitrage_executor")
    prev_cwd = os.getcwd()
    try:
        os.chdir(executor_dir)
        existing = SelectorManager.get_market("betmgm", market_key)
        if existing and not overwrite:
            return market_key, cfg, False
        ok = SelectorManager.save_market_config("betmgm", market_key, cfg)
    finally:
        os.chdir(prev_cwd)
    return market_key, cfg, ok
