"""CDP recorder daemon.

Attaches to a running Chrome via CDP (port 9223 by default) and writes a
JSONL trace as the user drives the browser. Captures:

- click events (DOM-level, via injected JS that posts to a Python-exposed
  function on the page)
- input/change events on form fields
- XHR/fetch responses (via Playwright's page.on("response"))
- top-level frame navigations

The user runs this in one terminal, drives Chrome themselves, and Ctrl-Cs
when the session is complete. Output is the same JSONL format produced by
probe.py, so codegen and replay tools accept either path.

Caveats:
- This sees only top frames by default. iframes need separate hooks; out of
  scope for the first version.
- The MCP browser shares the user's logged-in Chrome profile on Windows.
  Recording past a place-bet click captures a real bet. The recorder does
  not prevent it -- it's the operator's choice. Replay structurally halts
  at terminal=True, so even if a place-bet click is recorded, it will not
  be re-executed.

CLI:
    python -m toolkit.recorder.cdp_recorder \\
        --book fanduel --market player_points [--cdp-port 9223] [--trace-dir ...]
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
import uuid
from typing import Optional

from toolkit.recorder.schema import (
    FORMAT_VERSION,
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
DEFAULT_CDP_PORT = 9223


_INIT_SCRIPT = r"""
(() => {
  if (window.__bg_recorder_installed) return;
  window.__bg_recorder_installed = true;

  const dataAttrs = (el) => {
    const out = {};
    for (const a of el.attributes || []) {
      if (a.name.startsWith('data-')) out[a.name] = a.value;
    }
    return out;
  };

  const xpathOf = (el) => {
    if (!el || el.nodeType !== 1) return null;
    const parts = [];
    while (el && el.nodeType === 1) {
      let idx = 1;
      let sib = el.previousSibling;
      while (sib) {
        if (sib.nodeType === 1 && sib.nodeName === el.nodeName) idx++;
        sib = sib.previousSibling;
      }
      const seg = el.nodeName.toLowerCase() + '[' + idx + ']';
      parts.unshift(seg);
      el = el.parentNode;
      if (!el || el.nodeType !== 1) break;
    }
    return '/' + parts.join('/');
  };

  const stableAncestor = (el) => {
    let cur = el;
    while (cur && cur.nodeType === 1) {
      const tid = cur.getAttribute && cur.getAttribute('data-testid');
      if (tid) return '[data-testid="' + tid + '"]';
      if (cur.id) return '#' + cur.id;
      cur = cur.parentNode;
    }
    return null;
  };

  const sigOf = (el) => {
    if (!el || el.nodeType !== 1) return null;
    const text = (el.innerText || el.textContent || '').trim().slice(0, 200);
    return {
      tag: (el.tagName || '').toLowerCase(),
      role: el.getAttribute && el.getAttribute('role'),
      aria_label: el.getAttribute && el.getAttribute('aria-label'),
      data_attrs: dataAttrs(el),
      text: text,
      css_classes: Array.from(el.classList || []),
      xpath: xpathOf(el),
      closest_stable_ancestor: stableAncestor(el),
    };
  };

  const cssEscape = (s) => (window.CSS && CSS.escape) ? CSS.escape(s) : String(s).replace(/"/g, '\\"');

  const bestSelectorOf = (el, sig) => {
    if (!sig) return null;
    if (sig.data_attrs && sig.data_attrs['data-testid']) {
      return '[data-testid="' + cssEscape(sig.data_attrs['data-testid']) + '"]';
    }
    if (sig.data_attrs && sig.data_attrs['data-test-option-id']) {
      return '[data-test-option-id="' + cssEscape(sig.data_attrs['data-test-option-id']) + '"]';
    }
    if (el && el.id) return '#' + cssEscape(el.id);
    if (sig.aria_label) return '[aria-label="' + cssEscape(sig.aria_label) + '"]';
    if (sig.role) return sig.tag + '[role="' + cssEscape(sig.role) + '"]';
    return sig.xpath;
  };

  const send = (payload) => {
    if (window.__bgRecorderLog) {
      try { window.__bgRecorderLog(JSON.stringify(payload)); } catch (e) {}
    }
  };

  document.addEventListener('click', (ev) => {
    const sig = sigOf(ev.target);
    send({
      kind: 'click',
      selector: bestSelectorOf(ev.target, sig),
      element_signature: sig,
      ts: Date.now() / 1000,
    });
  }, true);

  document.addEventListener('change', (ev) => {
    const t = ev.target;
    if (!t) return;
    if (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT') {
      const sig = sigOf(t);
      send({
        kind: 'fill',
        selector: bestSelectorOf(t, sig),
        element_signature: sig,
        value: t.value,
        ts: Date.now() / 1000,
      });
    }
  }, true);

  // Capture Enter keypresses on inputs as a press record so codegen can
  // reproduce the typical "type + Enter" flow.
  document.addEventListener('keydown', (ev) => {
    if (ev.key !== 'Enter') return;
    const t = ev.target;
    if (!t) return;
    const sig = sigOf(t);
    send({
      kind: 'press',
      key: 'Enter',
      selector: bestSelectorOf(t, sig),
      ts: Date.now() / 1000,
    });
  }, true);
})();
"""


class _Writer:
    """Thread-safe JSONL appender."""

    def __init__(self, path: str, header: TraceHeader):
        self._path = path
        self._lock = threading.Lock()
        self._next_seq = 0
        self._last_ts: Optional[float] = None
        with open(path, "w", encoding="utf-8") as f:
            f.write(to_jsonl_line(header))

    def write(self, rec: TraceRecord) -> None:
        with self._lock:
            rec.seq = self._next_seq
            self._next_seq += 1
            if rec.ts == 0.0:
                rec.ts = time.time()
            if self._last_ts is not None:
                rec.elapsed_ms = int((rec.ts - self._last_ts) * 1000)
            self._last_ts = rec.ts
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(to_jsonl_line(rec))


def _attach_to_page(page, writer: _Writer, book: str, market: str) -> None:
    """Wire up event listeners + init script on one page."""
    def on_log(payload_json: str) -> None:
        try:
            d = json.loads(payload_json)
        except Exception:
            return
        sig = None
        if d.get("element_signature"):
            sig_d = d["element_signature"]
            sig = ElementSignature(
                tag=sig_d.get("tag", ""),
                role=sig_d.get("role"),
                aria_label=sig_d.get("aria_label"),
                data_attrs=sig_d.get("data_attrs") or {},
                text=sig_d.get("text"),
                css_classes=sig_d.get("css_classes") or [],
                xpath=sig_d.get("xpath"),
                closest_stable_ancestor=sig_d.get("closest_stable_ancestor"),
            )
        rec = TraceRecord(
            seq=0,
            ts=d.get("ts") or 0.0,
            kind=d.get("kind", "note"),
            book=book,
            market=market,
            selector=d.get("selector"),
            value=d.get("value"),
            key=d.get("key"),
            element_signature=sig,
        )
        writer.write(rec)

    try:
        page.expose_function("__bgRecorderLog", on_log)
    except Exception:
        # Already exposed (e.g. on a re-attach). Ignore.
        pass
    page.add_init_script(_INIT_SCRIPT)
    try:
        page.evaluate(_INIT_SCRIPT)
    except Exception:
        pass

    def on_response(response):
        try:
            req = response.request
            if req.resource_type not in ("xhr", "fetch"):
                return
            net = NetworkEvent(
                url=response.url,
                method=req.method,
                status=response.status,
                request_body_len=len(req.post_data or "") if req.post_data else None,
            )
            rec = TraceRecord(
                seq=0,
                ts=time.time(),
                kind="wait_network",
                book=book,
                market=market,
                method=req.method,
                url_pattern=response.url,
                network=net,
            )
            writer.write(rec)
        except Exception:
            return

    def on_navigation(frame):
        try:
            if frame.parent_frame is not None:
                return  # top frame only
            rec = TraceRecord(
                seq=0,
                ts=time.time(),
                kind="navigate",
                book=book,
                market=market,
                url=frame.url,
            )
            writer.write(rec)
        except Exception:
            return

    page.on("response", on_response)
    page.on("framenavigated", on_navigation)


def run(args: argparse.Namespace) -> int:
    from playwright.sync_api import sync_playwright

    os.makedirs(args.trace_dir, exist_ok=True)
    session_id = uuid.uuid4().hex[:12]
    started_at = time.time()
    fname = (
        f"{time.strftime('%Y%m%d_%H%M%S', time.localtime(started_at))}"
        f"_{args.book}_{args.market}_{session_id}.jsonl"
    )
    trace_path = os.path.join(args.trace_dir, fname)
    header = TraceHeader(
        format_version=FORMAT_VERSION,
        started_at=started_at,
        book=args.book,
        market=args.market,
        session_id=session_id,
        notes=args.notes,
    )
    writer = _Writer(trace_path, header)
    print(f"[recorder] session {session_id}")
    print(f"[recorder] trace: {trace_path}")

    stop = threading.Event()

    def _on_sigint(signum, frame):
        print("\n[recorder] stopping...")
        stop.set()

    signal.signal(signal.SIGINT, _on_sigint)

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(f"http://localhost:{args.cdp_port}")
        if not browser.contexts:
            print("[recorder] no Chrome contexts found", file=sys.stderr)
            return 2
        ctx = browser.contexts[0]

        attached = set()

        def _maybe_attach(page) -> None:
            if id(page) in attached:
                return
            try:
                _attach_to_page(page, writer, args.book, args.market)
                attached.add(id(page))
                print(f"[recorder] attached to page: {page.url[:80]}")
            except Exception as e:
                print(f"[recorder] attach failed for {page.url[:80]}: {e}",
                      file=sys.stderr)

        for page in ctx.pages:
            _maybe_attach(page)

        ctx.on("page", lambda pg: _maybe_attach(pg))

        print("[recorder] running. drive Chrome to record. Ctrl-C to stop.")
        while not stop.is_set():
            time.sleep(0.1)

    print(f"[recorder] stopped. trace: {trace_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cdp_recorder",
        description="Record browser actions via CDP into a JSONL trace.",
    )
    p.add_argument("--book", required=True, choices=["fanduel", "betmgm"])
    p.add_argument("--market", required=True)
    p.add_argument("--cdp-port", type=int, default=DEFAULT_CDP_PORT)
    p.add_argument("--trace-dir", default=DEFAULT_TRACE_DIR)
    p.add_argument("--notes", default=None)
    return p


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
