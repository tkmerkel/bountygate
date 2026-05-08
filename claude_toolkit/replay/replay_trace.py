"""Replay engine: re-execute a recorder trace against a live page.

Connects to a running Chrome via CDP and replays each TraceRecord in order.
Structurally halts at the first record marked terminal=True (the place-bet
click), so this engine NEVER places a real bet.

Use for:
- Validating a freshly recorded trace before committing the codegen output
- Debugging: deterministically replay a failed-bet audit dir's trace and
  watch where it breaks
- CI: a daily replay of a known-good trace catches catastrophic UI breakage

Trace contract (from claude_toolkit.recorder.schema):
    kind=navigate         page.goto(url)
    kind=fill             page.locator(selector).fill(value)
    kind=type             page.locator(selector).type(value, delay=delay_ms_per_char)
    kind=press            page.keyboard.press(key)  [or locator if selector given]
    kind=click            page.locator(selector).click()  -- HALTS if terminal=True
    kind=wait_for         page.locator(selector).wait_for(state=state)
    kind=wait_network     page.wait_for_response(matcher, timeout=...)
    kind=set_viewport     page.set_viewport_size(width, height)
    kind=screenshot       page.screenshot(path=...)
    kind=note             no-op log

CLI:
    python -m claude_toolkit.replay.replay_trace --trace path/to/trace.jsonl
    python -m claude_toolkit.replay.replay_trace --trace ... --dry-run
    python -m claude_toolkit.replay.replay_trace --trace ... --cdp-port 9223
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

from claude_toolkit.recorder.schema import TraceRecord, load_trace

DEFAULT_CDP_PORT = 9223
DEFAULT_NETWORK_TIMEOUT_MS = 15_000
DEFAULT_LOCATOR_TIMEOUT_MS = 10_000


@dataclass
class StepResult:
    seq: int
    kind: str
    status: str  # "ok" | "skipped" | "halted_terminal" | "error"
    detail: str = ""


@dataclass
class ReplayReport:
    trace_path: str
    book: str
    market: str
    halted_at: Optional[int] = None
    steps: list[StepResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(s.status in ("ok", "skipped", "halted_terminal") for s in self.steps)

    def to_dict(self) -> dict:
        return {
            "trace_path": self.trace_path,
            "book": self.book,
            "market": self.market,
            "halted_at": self.halted_at,
            "ok": self.ok,
            "steps": [vars(s) for s in self.steps],
        }


def _format_text(report: ReplayReport) -> str:
    lines = [f"replay {report.trace_path}"]
    lines.append(f"  book={report.book} market={report.market} ok={report.ok}")
    if report.halted_at is not None:
        lines.append(f"  halted at seq={report.halted_at} (terminal=True)")
    for s in report.steps:
        marker = {"ok": " ", "skipped": "-", "halted_terminal": "H", "error": "X"}.get(s.status, "?")
        lines.append(f"  [{marker}] seq={s.seq:03d} {s.kind:14s} {s.detail}")
    return "\n".join(lines)


def _execute_step(page, rec: TraceRecord) -> StepResult:
    detail = ""
    try:
        if rec.kind == "navigate":
            if not rec.url:
                return StepResult(rec.seq, rec.kind, "error", "missing url")
            page.goto(rec.url, wait_until="domcontentloaded")
            detail = rec.url
        elif rec.kind == "fill":
            if not rec.selector or rec.value is None:
                return StepResult(rec.seq, rec.kind, "error", "missing selector/value")
            page.locator(rec.selector).fill(rec.value, timeout=DEFAULT_LOCATOR_TIMEOUT_MS)
            detail = f"{rec.selector!r} <- {rec.value!r}"
        elif rec.kind == "type":
            if not rec.selector or rec.value is None:
                return StepResult(rec.seq, rec.kind, "error", "missing selector/value")
            delay = rec.delay_ms_per_char or 0
            page.locator(rec.selector).type(rec.value, delay=delay,
                                            timeout=DEFAULT_LOCATOR_TIMEOUT_MS)
            detail = f"type {rec.selector!r} <- {rec.value!r} delay={delay}ms"
        elif rec.kind == "press":
            if not rec.key:
                return StepResult(rec.seq, rec.kind, "error", "missing key")
            if rec.selector:
                page.locator(rec.selector).press(rec.key, timeout=DEFAULT_LOCATOR_TIMEOUT_MS)
                detail = f"press {rec.key!r} on {rec.selector!r}"
            else:
                page.keyboard.press(rec.key)
                detail = f"keyboard.press {rec.key!r}"
        elif rec.kind == "click":
            if not rec.selector:
                return StepResult(rec.seq, rec.kind, "error", "missing selector")
            page.locator(rec.selector).first.click(timeout=DEFAULT_LOCATOR_TIMEOUT_MS)
            detail = rec.selector
        elif rec.kind == "wait_for":
            if not rec.selector:
                return StepResult(rec.seq, rec.kind, "error", "missing selector")
            state = rec.state or "visible"
            page.locator(rec.selector).wait_for(state=state,
                                                timeout=DEFAULT_LOCATOR_TIMEOUT_MS)
            detail = f"wait_for {rec.selector!r} state={state}"
        elif rec.kind == "wait_network":
            if not rec.url_pattern:
                return StepResult(rec.seq, rec.kind, "error", "missing url_pattern")
            pat = re.compile(rec.url_pattern)
            method = rec.method
            def _match(resp):
                if not pat.search(resp.url):
                    return False
                if method and resp.request.method.upper() != method.upper():
                    return False
                return True
            page.wait_for_event("response", predicate=_match,
                                timeout=DEFAULT_NETWORK_TIMEOUT_MS)
            detail = f"wait_network /{rec.url_pattern}/ method={method}"
        elif rec.kind == "set_viewport":
            if rec.width is None or rec.height is None:
                return StepResult(rec.seq, rec.kind, "error", "missing width/height")
            page.set_viewport_size({"width": rec.width, "height": rec.height})
            detail = f"{rec.width}x{rec.height}"
        elif rec.kind == "screenshot":
            label = rec.tag_label or f"replay_seq{rec.seq}"
            path = os.path.join(os.getcwd(), f"replay_{label}_{int(time.time())}.png")
            page.screenshot(path=path)
            detail = path
        elif rec.kind == "note":
            detail = rec.text or ""
        else:
            return StepResult(rec.seq, rec.kind, "skipped", f"unhandled kind: {rec.kind}")
    except Exception as e:
        return StepResult(rec.seq, rec.kind, "error", f"{type(e).__name__}: {e}")
    return StepResult(rec.seq, rec.kind, "ok", detail)


def replay(trace_path: str, *, cdp_port: int = DEFAULT_CDP_PORT,
           dry_run: bool = False) -> ReplayReport:
    header, records = load_trace(trace_path)
    report = ReplayReport(trace_path=trace_path, book=header.book, market=header.market)

    if dry_run:
        for rec in records:
            if rec.terminal:
                report.halted_at = rec.seq
                report.steps.append(StepResult(rec.seq, rec.kind, "halted_terminal",
                                               "would halt before this step"))
                break
            report.steps.append(StepResult(rec.seq, rec.kind, "ok",
                                           "(dry-run) would execute"))
        return report

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(f"http://localhost:{cdp_port}")
        if not browser.contexts:
            raise RuntimeError("no Chrome contexts available on CDP")
        ctx = browser.contexts[0]
        page = ctx.pages[0] if ctx.pages else ctx.new_page()

        for rec in records:
            if rec.terminal:
                report.halted_at = rec.seq
                report.steps.append(StepResult(rec.seq, rec.kind, "halted_terminal",
                                               "halted before terminal step"))
                break
            report.steps.append(_execute_step(page, rec))
            if report.steps[-1].status == "error":
                # Continue executing subsequent steps -- the report shows
                # everything. Caller can decide whether to abort.
                continue
    return report


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(
        prog="replay_trace",
        description="Re-execute a recorder JSONL trace against a live page.",
    )
    p.add_argument("--trace", required=True)
    p.add_argument("--cdp-port", type=int, default=DEFAULT_CDP_PORT)
    p.add_argument("--dry-run", action="store_true",
                   help="parse + plan only, no live execution")
    p.add_argument("--json", action="store_true", help="emit JSON report")
    args = p.parse_args(argv)

    try:
        report = replay(args.trace, cdp_port=args.cdp_port, dry_run=args.dry_run)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print(_format_text(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
