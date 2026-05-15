"""FanDuel codegen: trace -> selectors/fanduel_markets.yaml entry.

Reads a recorder trace and produces a market config dict shaped to match
arbitrage_executor/selectors/fanduel_markets.yaml. Output is written via
arbitrage_executor.selector_finder.SelectorManager.save_market_config — the
existing YAML I/O contract — so this module never touches the YAML file
directly.

FanDuel's standard schema (e.g. player_points):
    display_names:    list[str]    # text variants from the bet element
    selector_type:    'aria_label'
    selector_pattern: str           # the aria-label CSS selector
    search_strategy:  'aria_label_match'
    test_player:      str
    test_line:        float
    validated_at:     str           # added by SelectorManager

FanDuel's alternate schema (e.g. player_points_alternate):
    display_names:    list[str]
    selector_type:    'aria_label'
    search_strategy:  'alternate_threshold_match'
    is_alternate:     true
    base_market:      str           # market name without _alternate suffix

The trace is expected to carry these tagged records (Claude/recorder marks
phase= when logging):
    phase=nav         navigate to search page
    phase=search      fill player name (selector_strategy=placeholder|label),
                      then press Enter
    phase=select_bet  click the bet element (this carries the canonical
                      selector and aria-label that we extract)

If a record carries an ElementSignature with aria_label, we prefer that for
display-name extraction; otherwise we parse the selector string itself.
"""
from __future__ import annotations

import re
from typing import Optional

from toolkit.recorder.schema import TraceHeader, TraceRecord, load_trace

# Match the [aria-label*="..."] segments produced by FanDuel's selectors.
_ARIA_PART_RE = re.compile(r'\[aria-label\*="([^"]+)"\]')
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")


class CodegenError(Exception):
    pass


def _find_bet_click(records: list[TraceRecord]) -> TraceRecord:
    candidates = [r for r in records if r.kind == "click" and r.phase == "select_bet"]
    if candidates:
        return candidates[0]
    fallback = [r for r in records if r.kind == "click" and not r.terminal]
    if not fallback:
        raise CodegenError("no click record found in trace")
    return fallback[0]


def _find_search_fill(records: list[TraceRecord]) -> Optional[TraceRecord]:
    for r in records:
        if r.kind == "fill" and (r.phase == "search" or r.phase is None):
            if r.value:
                return r
    return None


def _aria_label_parts(selector: str) -> list[str]:
    """Return the list of strings inside [aria-label*="..."] fragments."""
    return _ARIA_PART_RE.findall(selector or "")


def _aria_label_text(rec: TraceRecord) -> Optional[str]:
    if rec.element_signature and rec.element_signature.aria_label:
        return rec.element_signature.aria_label
    return None


def _extract_line_from_aria(parts: list[str]) -> Optional[float]:
    """Pick the numeric token (the line) from the aria-label fragments."""
    for p in parts:
        m = _NUMBER_RE.fullmatch(p.strip())
        if m:
            try:
                return float(p.strip())
            except ValueError:
                continue
    return None


def _extract_display_names(parts: list[str], player: Optional[str], line: Optional[float]) -> list[str]:
    """The remaining aria-label fragments — minus the player and the numeric
    line — are the display names of the market."""
    skip = set()
    if player:
        skip.add(player)
    if line is not None:
        skip.add(f"{line}")
        if line == int(line):
            skip.add(f"{int(line)}")
    out = []
    for p in parts:
        clean = p.strip()
        if clean in skip:
            continue
        if _NUMBER_RE.fullmatch(clean):
            continue
        if clean and clean not in out:
            out.append(clean)
    return out


def trace_to_config(header: TraceHeader, records: list[TraceRecord]) -> tuple[str, dict]:
    """Convert a trace into (market_key, config_dict) ready for SelectorManager.

    Raises CodegenError on missing-required-record or shape mismatch.
    """
    if header.book != "fanduel":
        raise CodegenError(f"trace book is {header.book!r}, expected 'fanduel'")
    market_key = header.market
    if not market_key:
        raise CodegenError("trace header missing market")

    is_alternate = market_key.endswith("_alternate")
    if is_alternate:
        return market_key, _alternate_config(market_key, records)
    return market_key, _standard_config(market_key, records)


def _standard_config(market_key: str, records: list[TraceRecord]) -> dict:
    bet = _find_bet_click(records)
    if not bet.selector:
        raise CodegenError("bet click record has no selector")
    parts = _aria_label_parts(bet.selector)
    aria_label = _aria_label_text(bet)
    if not parts and aria_label:
        # Fall back to splitting the captured aria_label by commas.
        parts = [p.strip() for p in aria_label.split(",") if p.strip()]

    fill = _find_search_fill(records)
    test_player = fill.value if fill else None
    test_line = _extract_line_from_aria(parts)
    display_names = _extract_display_names(parts, test_player, test_line)

    if not display_names:
        raise CodegenError(
            "could not extract display_names from bet selector / aria-label"
        )

    cfg: dict = {
        "display_names": display_names,
        "selector_type": "aria_label",
        "search_strategy": "aria_label_match",
    }
    if bet.selector and "aria-label" in bet.selector:
        cfg["selector_pattern"] = bet.selector
    if test_player:
        cfg["test_player"] = test_player
    if test_line is not None:
        cfg["test_line"] = test_line
    return cfg


def _alternate_config(market_key: str, records: list[TraceRecord]) -> dict:
    base_market = market_key[: -len("_alternate")]
    display_names: list[str] = []
    for r in records:
        if r.kind == "click" and r.phase == "select_market":
            if r.element_signature and r.element_signature.text:
                display_names.append(r.element_signature.text.strip())
            elif r.text:
                display_names.append(r.text.strip())
    if not display_names:
        bet = _find_bet_click(records)
        parts = _aria_label_parts(bet.selector or "")
        display_names = _extract_display_names(parts, None, None)
    if not display_names:
        raise CodegenError("alternate market trace had no select_market/click record with text")
    return {
        "display_names": display_names,
        "selector_type": "aria_label",
        "search_strategy": "alternate_threshold_match",
        "is_alternate": True,
        "base_market": base_market,
    }


def save_from_trace(trace_path: str, *, overwrite: bool = False) -> tuple[str, dict, bool]:
    """Load a trace and persist the derived config via SelectorManager.

    Returns (market_key, config, written). If a market with the same key
    already exists and overwrite=False, returns written=False without
    touching the file.
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
        existing = SelectorManager.get_market("fanduel", market_key)
        if existing and not overwrite:
            return market_key, cfg, False
        ok = SelectorManager.save_market_config("fanduel", market_key, cfg)
    finally:
        os.chdir(prev_cwd)
    return market_key, cfg, ok
