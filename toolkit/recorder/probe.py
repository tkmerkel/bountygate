"""Probe CLI — the Claude-driven recording path.

Claude drives the browser via the Playwright MCP. After each action, Claude
calls this CLI to append a TraceRecord to the active session's JSONL trace.
The same trace format is consumed by the CDP recorder daemon (cdp_recorder.py),
so codegen and replay tools don't care which path produced the trace.

Subcommands:
    start         Open a new session. Writes the trace header.
    stop          Close the active session.
    status        Print active-session metadata.
    log_action    Append one TraceRecord (click, fill, press, navigate, ...).
    log_network   Append one wait_network TraceRecord with a NetworkEvent.

Active-session state lives at {trace_dir}/.active_session.json. Only one
session can be active at a time. The next-sequence counter is persisted there.

Example session:
    python -m toolkit.recorder.probe start --book fanduel --market player_points
    python -m toolkit.recorder.probe log_action --kind navigate --url 'https://mo.sportsbook.fanduel.com/search'
    python -m toolkit.recorder.probe log_action --kind fill --selector 'input[placeholder="Search"]' --value 'Stephen Curry' --strategy placeholder
    python -m toolkit.recorder.probe log_action --kind press --key Enter
    python -m toolkit.recorder.probe log_network --url-pattern 'smp\\.mo\\.sportsbook\\.fanduel\\.com/api/sports/fixedodds' --method GET --status 200 --elapsed-ms 412
    python -m toolkit.recorder.probe stop
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from dataclasses import asdict
from typing import Optional

from toolkit.recorder.schema import (
    ACTION_KINDS,
    FORMAT_VERSION,
    PHASES,
    SELECTOR_STRATEGIES,
    ElementSignature,
    NetworkEvent,
    TraceHeader,
    TraceRecord,
    to_jsonl_line,
)

DEFAULT_TRACE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "traces",
)
SESSION_FILE = ".active_session.json"


class ProbeError(Exception):
    pass


def _trace_dir(args: argparse.Namespace) -> str:
    d = getattr(args, "trace_dir", None) or DEFAULT_TRACE_DIR
    os.makedirs(d, exist_ok=True)
    return d


def _session_path(trace_dir: str) -> str:
    return os.path.join(trace_dir, SESSION_FILE)


def _load_session(trace_dir: str) -> Optional[dict]:
    path = _session_path(trace_dir)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_session(trace_dir: str, session: dict) -> None:
    with open(_session_path(trace_dir), "w", encoding="utf-8") as f:
        json.dump(session, f, indent=2)


def _clear_session(trace_dir: str) -> None:
    path = _session_path(trace_dir)
    if os.path.exists(path):
        os.remove(path)


def _require_session(trace_dir: str) -> dict:
    session = _load_session(trace_dir)
    if not session:
        raise ProbeError(
            "no active session. run `probe start --book BOOK --market MARKET` first."
        )
    return session


def _append_record(session: dict, record: TraceRecord) -> int:
    record.seq = session["next_seq"]
    record.ts = time.time()
    if session.get("last_ts"):
        record.elapsed_ms = int((record.ts - session["last_ts"]) * 1000)
    if not record.book:
        record.book = session["book"]
    if not record.market:
        record.market = session["market"]
    with open(session["trace_path"], "a", encoding="utf-8") as f:
        f.write(to_jsonl_line(record))
    session["next_seq"] = record.seq + 1
    session["last_ts"] = record.ts
    return record.seq


def cmd_start(args: argparse.Namespace) -> int:
    trace_dir = _trace_dir(args)
    if _load_session(trace_dir):
        raise ProbeError(
            "a session is already active. run `probe stop` first or pass --force."
            if not args.force
            else ""
        )
    session_id = uuid.uuid4().hex[:12]
    started_at = time.time()
    ts_label = time.strftime("%Y%m%d_%H%M%S", time.localtime(started_at))
    fname = f"{ts_label}_{args.book}_{args.market}_{session_id}.jsonl"
    trace_path = os.path.join(trace_dir, fname)
    header = TraceHeader(
        format_version=FORMAT_VERSION,
        started_at=started_at,
        book=args.book,
        market=args.market,
        session_id=session_id,
        notes=args.notes,
    )
    with open(trace_path, "w", encoding="utf-8") as f:
        f.write(to_jsonl_line(header))
    _save_session(
        trace_dir,
        {
            "trace_path": trace_path,
            "book": args.book,
            "market": args.market,
            "session_id": session_id,
            "started_at": started_at,
            "next_seq": 0,
            "last_ts": None,
        },
    )
    print(f"started session {session_id}")
    print(f"trace: {trace_path}")
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    trace_dir = _trace_dir(args)
    session = _load_session(trace_dir)
    if not session:
        print("no active session")
        return 0
    _clear_session(trace_dir)
    print(f"stopped session {session['session_id']}")
    print(f"trace: {session['trace_path']}")
    print(f"records: {session['next_seq']}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    trace_dir = _trace_dir(args)
    session = _load_session(trace_dir)
    if not session:
        print("no active session")
        return 1
    print(json.dumps(session, indent=2))
    return 0


def cmd_log_action(args: argparse.Namespace) -> int:
    trace_dir = _trace_dir(args)
    session = _require_session(trace_dir)
    if args.kind not in ACTION_KINDS:
        raise ProbeError(f"unknown action kind: {args.kind} (allowed: {ACTION_KINDS})")
    if args.phase and args.phase not in PHASES:
        raise ProbeError(f"unknown phase: {args.phase} (allowed: {PHASES})")
    if args.strategy and args.strategy not in SELECTOR_STRATEGIES:
        raise ProbeError(
            f"unknown selector strategy: {args.strategy} (allowed: {SELECTOR_STRATEGIES})"
        )
    sig = None
    if args.element_json:
        sig = ElementSignature(**json.loads(args.element_json))
    rec = TraceRecord(
        seq=0,
        ts=0.0,
        kind=args.kind,
        phase=args.phase,
        url=args.url,
        url_pattern=args.url_pattern,
        method=args.method,
        selector=args.selector,
        selector_strategy=args.strategy,
        value=args.value,
        key=args.key,
        state=args.state,
        delay_ms_per_char=args.delay_ms,
        width=args.width,
        height=args.height,
        tag_label=args.tag_label,
        text=args.text,
        element_signature=sig,
        terminal=args.terminal,
    )
    seq = _append_record(session, rec)
    _save_session(trace_dir, session)
    print(f"logged seq={seq} kind={args.kind}")
    return 0


def cmd_log_network(args: argparse.Namespace) -> int:
    trace_dir = _trace_dir(args)
    session = _require_session(trace_dir)
    net = NetworkEvent(
        url=args.url,
        method=args.method,
        status=args.status,
        elapsed_ms=args.elapsed_ms,
        request_body_len=args.req_body_len,
        response_body_excerpt=args.resp_excerpt,
    )
    rec = TraceRecord(
        seq=0,
        ts=0.0,
        kind="wait_network",
        phase=args.phase,
        url_pattern=args.url_pattern or args.url,
        method=args.method,
        network=net,
    )
    seq = _append_record(session, rec)
    _save_session(trace_dir, session)
    print(f"logged seq={seq} network {args.method} {args.url}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="probe",
        description="Append TraceRecords to an active recording session.",
    )
    p.add_argument("--trace-dir", default=None, help=f"defaults to {DEFAULT_TRACE_DIR}")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("start", help="open a new recording session")
    s.add_argument("--book", required=True, choices=["fanduel", "betmgm"])
    s.add_argument("--market", required=True, help="market key, e.g. player_points")
    s.add_argument("--notes", default=None)
    s.add_argument("--force", action="store_true")
    s.set_defaults(func=cmd_start)

    s = sub.add_parser("stop", help="close the active session")
    s.set_defaults(func=cmd_stop)

    s = sub.add_parser("status", help="print active session info")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("log_action", help="append a TraceRecord")
    s.add_argument("--kind", required=True, help=f"one of: {','.join(ACTION_KINDS)}")
    s.add_argument("--phase", default=None, help=f"one of: {','.join(PHASES)}")
    s.add_argument("--selector", default=None)
    s.add_argument("--strategy", default=None,
                   help=f"selector strategy: {','.join(SELECTOR_STRATEGIES)}")
    s.add_argument("--value", default=None)
    s.add_argument("--key", default=None)
    s.add_argument("--state", default=None, help="visible|attached|detached (wait_for)")
    s.add_argument("--url", default=None, help="for navigate")
    s.add_argument("--url-pattern", default=None, help="for wait_network")
    s.add_argument("--method", default=None, help="for wait_network")
    s.add_argument("--delay-ms", type=int, default=None, help="ms per char (type)")
    s.add_argument("--width", type=int, default=None, help="set_viewport")
    s.add_argument("--height", type=int, default=None, help="set_viewport")
    s.add_argument("--tag-label", default=None, help="screenshot label")
    s.add_argument("--text", default=None, help="free text (note)")
    s.add_argument("--element-json", default=None,
                   help="JSON-encoded ElementSignature dict")
    s.add_argument("--terminal", action="store_true",
                   help="mark as terminal (replay halts before this step)")
    s.set_defaults(func=cmd_log_action)

    s = sub.add_parser("log_network", help="append a wait_network record")
    s.add_argument("--url", required=True)
    s.add_argument("--method", required=True)
    s.add_argument("--status", type=int, default=None)
    s.add_argument("--elapsed-ms", type=int, default=None)
    s.add_argument("--req-body-len", type=int, default=None)
    s.add_argument("--resp-excerpt", default=None)
    s.add_argument("--url-pattern", default=None,
                   help="regex for replay (defaults to escaped url)")
    s.add_argument("--phase", default=None)
    s.set_defaults(func=cmd_log_network)

    return p


def main(argv: Optional[list] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ProbeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
