# Recorder Workflow

The recorder/codegen/replay/drift toolchain (`claude_toolkit/recorder*`,
`claude_toolkit/codegen/`, `claude_toolkit/replay/`) lets Claude Code drive
the browser through any arb-bet flow and emit ready-to-merge YAML for
`arbitrage_executor/selectors/{book}_markets.yaml`.

This doc is the operator reference: when to use each tool, the exact
sequence of commands, and the safety guarantees.

## Mental model

A recording session produces a **trace** — a JSONL file capturing every
click, fill, keypress, navigation, and XHR. The trace is the unit of work.
From a trace you can:

| Tool | Reads | Produces |
|------|-------|----------|
| `codegen` | trace | YAML config dict (writes via `SelectorManager.save_market_config`) |
| `replay` | trace | re-executes the trace against a live page; halts at terminal step |
| `drift` | trace | diff vs stored YAML; non-zero exit on any field mismatch |

Two recording paths feed the same JSONL format:

1. **Probe path** (Claude-driven). Claude drives the browser via the
   Playwright MCP and calls `probe.py log_action` after each action. This
   is the path you'll use most often, because Claude can label each click
   semantically (`phase=select_bet`, `terminal=True`).
2. **CDP daemon path** (operator-driven). Run `cdp_recorder.py`, drive
   Chrome yourself, the daemon captures clicks via injected JS and XHRs
   via Playwright's response listener. Use this for full-session captures
   when you want every detail without manual labeling.

## Safety guard

`replay_trace.py` halts at the first record marked `terminal: True`. The
place-bet click should always be labeled terminal during recording. There
is no flag to disable this — it's structural. Replay can therefore be run
against the user's logged-in Chrome profile without any risk of placing a
real bet.

## End-to-end Claude-driven session (probe path)

The standard flow when Claude Code is mapping a new market or refreshing
selectors. All commands run from the repo root.

### 1. Start Chrome on port 9223

```bash
python -c "from arbitrage_executor.chrome_helpers import ensure_chrome_cdp; \
           ensure_chrome_cdp('arbitrage_executor/chrome_profile', 9223)"
```

You should already be logged into both books in this profile.

### 2. Open a new probe session

```bash
python -m claude_toolkit.recorder_cli probe start \
    --book fanduel --market player_points
```

This writes the trace header and creates `traces/.active_session.json`.
Subsequent `log_action` / `log_network` calls append to this trace.

### 3. Drive the browser via the Playwright MCP

Claude uses `mcp__playwright__browser_navigate`, `browser_click`, etc.
After each action, Claude logs it:

```bash
# After mcp__playwright__browser_navigate to the search page:
python -m claude_toolkit.recorder_cli probe log_action \
    --kind navigate --phase nav \
    --url 'https://mo.sportsbook.fanduel.com/search'

# After typing the player name:
python -m claude_toolkit.recorder_cli probe log_action \
    --kind fill --phase search \
    --selector 'input[placeholder="Search"]' --strategy placeholder \
    --value 'Stephen Curry'

# After pressing Enter:
python -m claude_toolkit.recorder_cli probe log_action \
    --kind press --phase search --key Enter

# After clicking the bet element (label this select_bet so codegen finds it):
python -m claude_toolkit.recorder_cli probe log_action \
    --kind click --phase select_bet \
    --selector '[aria-label*="Stephen Curry"][aria-label*="Points"][aria-label*="22.5"]'

# DO NOT actually click Place Bet during a Claude session. If you log it
# as a placeholder, mark it terminal so replay halts:
python -m claude_toolkit.recorder_cli probe log_action \
    --kind click --phase place \
    --selector '[data-testid="place-bet-button"]' --terminal
```

Phase tags the codegen looks for:

| Phase | Meaning |
|-------|---------|
| `nav` | Top-level navigation |
| `search` | Player/team search field interactions |
| `select_market` | Click the accordion (BetMGM) or threshold tab |
| `select_bet` | Click the bet element itself — codegen extracts `selector_pattern` from this |
| `wager` | Wager-input interactions |
| `place` | The terminal place-bet click — always pair with `--terminal` |

### 4. Stop the session

```bash
python -m claude_toolkit.recorder_cli probe stop
```

Prints the trace path and record count.

### 5. Run codegen against the trace

```bash
python -m claude_toolkit.recorder_cli codegen --trace traces/<file>.jsonl
```

Prints the derived config without persisting. To persist:

```bash
python -m claude_toolkit.recorder_cli codegen \
    --trace traces/<file>.jsonl --save
```

If a market config already exists for the same key, `--save` is a no-op
unless you also pass `--overwrite`.

### 6. Replay the trace as verification

```bash
python -m claude_toolkit.recorder_cli replay --trace traces/<file>.jsonl --dry-run
```

`--dry-run` parses and plans without touching the browser. Once that
prints the expected halt at the terminal step, run for real:

```bash
python -m claude_toolkit.recorder_cli replay --trace traces/<file>.jsonl
```

Replay re-executes every non-terminal step and halts before the place-bet
click. Errors per step are reported but don't abort the replay.

### 7. Commit the YAML diff

`SelectorManager.save_market_config` writes
`arbitrage_executor/selectors/{book}_markets.yaml`. Stage and commit that
file plus the trace if you want to keep it as a reference. Traces are
verbose; consider `git add -p` to keep only the new YAML row.

## Daemon path

Use this for full-session captures (e.g. recording a real bet end-to-end
to discover bet-slip-add and place-bet network endpoints — the **PENDING**
rows in `network_signatures.md`).

### 1. Start Chrome on port 9223 (same as above)

### 2. Run the daemon in one terminal

```bash
python -m claude_toolkit.recorder_cli record \
    --book fanduel --market player_points
```

The daemon attaches to all open pages, injects the click/keypress hook,
and logs every XHR. Output goes to `traces/<ts>_<book>_<market>_<id>.jsonl`.

### 3. Drive Chrome yourself

Open the search page, click through the bet, place the bet (or stop
before — your choice). The daemon doesn't intervene.

### 4. Ctrl-C the daemon when done

The trace is closed, ready for codegen / replay / drift just like a probe
session.

**Caveat**: the daemon does not auto-tag `phase=` or `terminal=True`.
Codegen falls back to "first non-terminal click is the bet click", but
for clean output, post-process the trace by hand or re-run via the probe
path.

## Drift detection

Once a market is in production, run drift periodically against a fresh
recording:

```bash
python -m claude_toolkit.recorder_cli drift --trace traces/<latest>.jsonl
```

Exits 0 if codegen output matches the stored YAML (modulo `validated_at`),
1 on any field mismatch, 2 on bad input. Suitable for cron + Discord
alerting.

The drift detector compares one market at a time. To check all stored
markets in one run, loop in shell:

```bash
for trace in traces/drift_check/*.jsonl; do
    python -m claude_toolkit.recorder_cli drift --trace "$trace" --json
done
```

## Round-trip tests

`claude_toolkit/recorder/tests/test_roundtrip.py` validates the codegen
contract against production YAML using committed fixture traces. No live
browser needed. Run before merging any codegen change:

```bash
python claude_toolkit/recorder/tests/test_roundtrip.py -v
```

Fixtures cover: FanDuel standard (`player_assists`), FanDuel alternate
(`player_points_alternate`), BetMGM standard (`player_assists`), BetMGM
alternate (`player_points_alternate`), and a `TerminalGuard` test that
asserts each fixture marks its place-bet as terminal.

## What this doesn't do (yet)

- **Replace `bet_placer.py`'s click sequences.** The codegen produces YAML
  that `bet_placer.py` consumes unchanged. A future plan can swap the
  hand-written sequences for a generic replay engine.
- **Capture bet-slip-add and place-bet endpoints automatically.** The
  daemon captures them when the user runs a real bet, but
  `network_signatures.md` is still the source of truth for those
  patterns. The recorder logs them; promoting them to the codegen schema
  is a follow-up.
- **Auto-discover unknown markets.** Codegen requires the operator (or
  Claude) to tell it the market key via `probe start --market KEY`. It
  doesn't infer market identity from DOM contents.

## File map

```
claude_toolkit/
  recorder/
    schema.py           # TraceRecord / TraceHeader / ElementSignature / NetworkEvent
    probe.py            # Claude-driven CLI (start/stop/log_action/log_network)
    cdp_recorder.py     # Operator-driven daemon (CDP attach + JS hook injection)
    tests/
      test_roundtrip.py # codegen contract tests
      fixtures/         # hand-authored JSONL traces
  codegen/
    fanduel.py          # trace -> fanduel_markets.yaml entry
    betmgm.py           # trace -> betmgm_markets.yaml entry
    drift.py            # trace vs stored YAML diff
  replay/
    replay_trace.py     # re-execute trace against live page
  recorder_cli.py       # single dispatcher: record/probe/codegen/replay/drift
arbitrage_executor/
  selectors/            # YAML output (existing; codegen writes through SelectorManager)
  docs/
    network_signatures.md
    recorder_workflow.md   # this file
```
