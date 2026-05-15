"""Trace schema for the arb-bot recorder.

A trace is a JSONL file: header line (one JSON object with metadata) followed
by N TraceRecord lines. Recorder daemon (cdp_recorder.py) and probe CLI
(probe.py) both produce this format. Codegen (codegen/{book}.py), replay
(replay/replay_trace.py), and drift detection (codegen/drift.py) all consume
it.

Format version bumps are breaking; bump only with explicit migration plan.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

FORMAT_VERSION = 1

ACTION_KINDS = (
    "navigate",
    "fill",
    "press",
    "click",
    "type",
    "wait_for",
    "wait_network",
    "set_viewport",
    "screenshot",
    "note",
)

PHASES = (
    "nav",
    "search",
    "select_market",
    "select_bet",
    "wager",
    "place",
    "confirm",
)

SELECTOR_STRATEGIES = (
    "css",
    "xpath",
    "text",
    "role",
    "testid",
    "placeholder",
    "label",
)


@dataclass
class ElementSignature:
    """Captured shape of a clicked or filled element.

    Codegen uses this to pick the most stable selector. Replay and drift
    detection use it to match elements across DOM changes.
    """

    tag: str
    role: Optional[str] = None
    aria_label: Optional[str] = None
    data_attrs: dict = field(default_factory=dict)
    text: Optional[str] = None
    css_classes: list = field(default_factory=list)
    xpath: Optional[str] = None
    closest_stable_ancestor: Optional[str] = None


@dataclass
class NetworkEvent:
    """Network response captured by a wait_network record.

    URL patterns intentionally line up with
    arbitrage_executor/docs/network_signatures.md.
    """

    url: str
    method: str
    status: Optional[int] = None
    elapsed_ms: Optional[int] = None
    request_body_len: Optional[int] = None
    response_body_excerpt: Optional[str] = None


@dataclass
class TraceRecord:
    """One row in a trace JSONL.

    Action-specific fields are populated based on `kind`. Producers should
    leave irrelevant fields as None so the JSON stays small.
    """

    seq: int
    ts: float
    kind: str
    book: Optional[str] = None
    market: Optional[str] = None
    phase: Optional[str] = None
    url: Optional[str] = None
    url_pattern: Optional[str] = None
    method: Optional[str] = None
    status_range: Optional[Any] = None
    selector: Optional[str] = None
    selector_strategy: Optional[str] = None
    value: Optional[str] = None
    key: Optional[str] = None
    state: Optional[str] = None
    delay_ms_per_char: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    tag_label: Optional[str] = None
    text: Optional[str] = None
    element_signature: Optional[ElementSignature] = None
    network: Optional[NetworkEvent] = None
    elapsed_ms: Optional[int] = None
    terminal: bool = False


@dataclass
class TraceHeader:
    """First line of a JSONL trace. Identifies session metadata."""

    format_version: int
    started_at: float
    book: str
    market: str
    session_id: str
    notes: Optional[str] = None


def to_jsonl_line(obj: Any) -> str:
    """Serialize a dataclass instance to one JSONL line (newline-terminated)."""
    return json.dumps(asdict(obj), separators=(",", ":"), default=str) + "\n"


def parse_header(line: str) -> TraceHeader:
    return TraceHeader(**json.loads(line))


def parse_record(line: str) -> TraceRecord:
    d = json.loads(line)
    if d.get("element_signature"):
        d["element_signature"] = ElementSignature(**d["element_signature"])
    if d.get("network"):
        d["network"] = NetworkEvent(**d["network"])
    return TraceRecord(**d)


def load_trace(path: str) -> tuple[TraceHeader, list[TraceRecord]]:
    """Load a JSONL trace file. Returns (header, records)."""
    with open(path, encoding="utf-8") as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip()]
    if not lines:
        raise ValueError(f"empty trace: {path}")
    header = parse_header(lines[0])
    if header.format_version != FORMAT_VERSION:
        raise ValueError(
            f"trace format_version {header.format_version} != supported {FORMAT_VERSION}"
        )
    records = [parse_record(ln) for ln in lines[1:]]
    return header, records
