"""Single CLI dispatcher for the recorder/codegen/replay/drift toolchain.

This wraps the per-tool entrypoints under one command name so the workflow
doc has a single thing to teach. Each subcommand forwards its argv to the
underlying tool's `main()`; per-tool flags pass through unchanged.

Subcommands:
    record    -> claude_toolkit.recorder.cdp_recorder.main
    probe     -> claude_toolkit.recorder.probe.main           (start/stop/status/log_*)
    codegen   -> derive YAML from a trace and (optionally) persist via SelectorManager
    replay    -> claude_toolkit.replay.replay_trace.main
    drift     -> claude_toolkit.codegen.drift.main

Usage:
    python -m claude_toolkit.recorder_cli record --book fanduel --market player_points
    python -m claude_toolkit.recorder_cli probe start --book fanduel --market player_points
    python -m claude_toolkit.recorder_cli codegen --trace traces/<file>.jsonl --save
    python -m claude_toolkit.recorder_cli replay --trace traces/<file>.jsonl --dry-run
    python -m claude_toolkit.recorder_cli drift --trace traces/<file>.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Optional


def _cmd_codegen(rest: list[str]) -> int:
    p = argparse.ArgumentParser(prog="recorder_cli codegen")
    p.add_argument("--trace", required=True)
    p.add_argument("--save", action="store_true",
                   help="persist via SelectorManager.save_market_config")
    p.add_argument("--overwrite", action="store_true",
                   help="when --save, overwrite an existing entry")
    p.add_argument("--json", action="store_true")
    args = p.parse_args(rest)

    from claude_toolkit.recorder.schema import load_trace
    header, records = load_trace(args.trace)
    if header.book == "fanduel":
        from claude_toolkit.codegen import fanduel as gen
    elif header.book == "betmgm":
        from claude_toolkit.codegen import betmgm as gen
    else:
        print(f"error: unknown book in trace: {header.book!r}", file=sys.stderr)
        return 2

    market_key, cfg = gen.trace_to_config(header, records)
    written = False
    if args.save:
        market_key, cfg, written = gen.save_from_trace(
            args.trace, overwrite=args.overwrite
        )

    if args.json:
        print(json.dumps(
            {"book": header.book, "market": market_key, "config": cfg, "written": written},
            indent=2, default=str,
        ))
    else:
        print(f"{header.book}/{market_key}  written={written}")
        for k, v in cfg.items():
            print(f"  {k}: {v}")
    return 0


def main(argv: Optional[list] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0

    cmd, rest = argv[0], argv[1:]

    if cmd == "record":
        from claude_toolkit.recorder import cdp_recorder
        return cdp_recorder.main(rest)
    if cmd == "probe":
        from claude_toolkit.recorder import probe
        return probe.main(rest)
    if cmd == "codegen":
        return _cmd_codegen(rest)
    if cmd == "replay":
        from claude_toolkit.replay import replay_trace
        return replay_trace.main(rest)
    if cmd == "drift":
        from claude_toolkit.codegen import drift
        return drift.main(rest)

    print(f"unknown subcommand: {cmd}", file=sys.stderr)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
