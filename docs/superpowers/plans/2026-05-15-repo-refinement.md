# Repo Refinement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Strip prototyping waste and tell the truth in the documentation so the repo is lean, navigable, and onboarding-friendly — without touching working production code (`bet_placer.py`, `chrome_helpers.py`, the Airflow DAGs, the `app/` shared package).

**Architecture:** This is a cleanup + clarification pass, not a re-architecture. Five phases: (1) strip loose cruft from the root, (2) rewrite the README and consolidate stale docs, (3) reorganize `scripts/` and clarify the `claude_toolkit/` role, (4) pin dependencies and add a single boot command, (5) decide the fate of empty placeholders (`tests/`, `infra/`). Each phase commits cleanly so any phase can be paused after.

**Tech Stack:** Python 3.10–3.12, uv (for `arbitrage_executor`), Poetry (root, possibly being deleted), Make, git.

---

## Honest Findings (the review you asked for)

**Scope of this review:** everything except `app/` (per user instruction) and everything that is working production code I shouldn't touch (`bet_placer.py`, `chrome_helpers.py`, the live DAGs).

A separate, comprehensive **`CRITIQUE.md` from 2026-04-20** already exists and covers *reliability and observability* (orphaned tasks, hardcoded credentials, broad excepts, balance reconciliation, etc.). **That document is still valid and complements this plan — this plan deliberately does not duplicate it.** Some of its items (selector smoke test, claude_toolkit existence) appear shipped; the rest are unaddressed but are *risk* problems, not *clarity* problems. They belong in a follow-up plan.

This review is specifically about: **what was created during exploration, never cleaned up, and is now actively obscuring the real codebase.**

### A. The root is a debug junk drawer

Nine throwaway Playwright probe scripts sit at the project root:
- `fd_inspect.py`, `fd_cleanup.py`, `fd_clear_slip.py`
- `mgm_probe.py`, `mgm_probe2.py`, `mgm_clear_slip.py`, `mgm_clear_wide.py`, `mgm_clear_wide2.py`, `mgm_clear_x.py`

Plus nine matching debug screenshots (`fd_*.png`, `mgm_*.png`), a typo-named file `Userstkmerbountygateplaywright_smoke.png` (clearly a path-handling bug that wrote the relative path as a filename), and `arbitrage_executor/test_recording.mp4`. None of these are imported, referenced, or invoked by anything in the codebase (verified via grep). They were all one-shot debugging probes for selector breaks that are already fixed and committed. Pure cruft.

There's also a root-level `audit_logs/` directory containing a single `20260513_211500_TEST_USER_player_points` directory — a manual smoke test that escaped the `arbitrage_executor/` working directory because of a CWD bug. And a root-level `traces/` with Playwright trace dumps from 2026-05-12. Neither is gitignored at root (only `arbitrage_executor/audit_logs/` is in `.gitignore`).

### B. The README describes a project that doesn't exist

`README.md` claims this is a "BountyGate monorepo" with:
- A **Streamlit dashboard** at `app/streamlit_app.py` — **doesn't exist**.
- A **`tests/` directory** running pytest — **empty**.
- An **`infra/` folder** with Heroku Procfile and runtime.txt — **empty**.
- An installable shared Python package via `pip install -e ./app/shared/python` — exists but the README's "Quick start" wires it for a Streamlit app that doesn't exist.

The actual product — `arbitrage_executor/`, a daily-running Playwright bot generating real bets via Chrome CDP — is not mentioned. The actual Airflow DAGs aren't documented at root either. A new collaborator reading the README would spend ~2 hours looking for things that aren't there before realizing the README is fiction. `CRITIQUE.md` already flagged this in April. It's still fiction a month later.

### C. Three "scripts holders" with overlapping responsibility

There are three places utility code lives, with no clear distinction:

- **`scripts/`** (13 files): mixes `migrate.py` (load-bearing — required to set up a fresh DB), `dq_checks.py` (operational), `load_aliases.py` / `load_market_aliases.py` (operational), and `seed_aliases_{nba,nfl,mlb,nhl}.py` + `normalize_{lines,team_blobs}.py` + `generate_market_aliases.py` (one-shot historical work, never run again). Also `local_dev_setup.ps1` and `start_watcher.ps1`. No README explaining which is which.
- **`claude_toolkit/`** (~10 files + subdirs): operational tools for the bot (`doctor.py`, `inspect_queue.py`, `rescue_stuck_tasks.py`, `tail_audit.py`, `selector_smoke_test.py`, `test_discord.py`, `recent_alerts.py`), plus the Playwright selector codegen system (`codegen/`, `recorder/`, `replay/`, `recorder_cli.py`), plus an Airflow helper (`dag_state.py`). Has a README but the contents are heterogeneous.
- **`arbitrage_executor/scripts/`**: contains only `drift_check.ps1`. A directory containing a single file is a strong smell.

The naming "claude_toolkit" is also load-bearing tribal knowledge — it has nothing to do with Claude the assistant; these are *operational scripts the user runs*. Renaming would aid clarity.

### D. Documentation has accreted

Active project docs at the repo root and inside `arbitrage_executor/`:
- `README.md` — fiction (see B).
- `CRITIQUE.md` (14KB, 2026-04-20) — comprehensive triage doc, still relevant, but freezes a moment in time.
- `RESEARCH.md` (30KB) — old exploration notes, nothing references it.
- `MAP_MISSING_MARKETS.md` — a runbook for *the specific 2026-05-14 batch* of missing markets. Dated, narrow. Once those markets are mapped, it's archive.
- `arbitrage_executor/CLAUDE.md` — current, accurate, the actual operational doc.
- `arbitrage_executor/GEMINI.md` — exists alongside `CLAUDE.md`; probably duplicate content for Gemini CLI.
- `arbitrage_executor/USAGE.md` — overlap with CLAUDE.md, slightly different framing.
- `arbitrage_executor/SOP.md` — UI-break recovery runbook, current.
- `arbitrage_executor/selectors/SCHEMA.md` — YAML schema doc.
- `arbitrage_executor/docs/recorder_workflow.md` — codegen flow doc.
- `airflow/README.md`, `db/aliases/README.md`, `db/market_aliases/README.md`, `claude_toolkit/README.md`, `watcher/INITIAL_PROMPT.md`, `watcher/review_prompt.md`.

Not all of this is bad — `CLAUDE.md`, `SOP.md`, `SCHEMA.md`, the watcher prompts are load-bearing — but the root-level archive (`RESEARCH.md`, `MAP_MISSING_MARKETS.md`, `CRITIQUE.md`) clutters the entry point.

### E. The dependency story is wrong in three places at once

- Root `pyproject.toml`: Poetry-managed, `package-mode = false`, declares only `python = ">=3.10,<3.13"` and `pytest = "^8.3"`. No actual deps. No lockfile. (CRITIQUE.md #8 flagged this.)
- `arbitrage_executor/pyproject.toml` + `uv.lock`: a `uv` project, but the lock file has empty dependencies despite a `.venv` with 100+ installed packages (Playwright, browser_use, anthropic, groq, etc.). The `.venv` was installed manually, not from the lock.
- Nothing tells a new operator which Python version, which Playwright version, which DB driver to install.

A fresh checkout on a new machine cannot start the bot. This is the highest-actual-cost item in this plan.

### F. The watcher system is undocumented at the root

`arbitrage_executor/screen_recorder.py` writes `audit_logs/{ts}_{player}_{market}/recording.mp4` + `review.pending` per execution. A *separate Claude Code session* (`watcher/INITIAL_PROMPT.md`, started via `scripts/start_watcher.ps1`) reads the recordings, calls `/watch:watch`, writes `review.md`, and appends to `dashboard/data.json`. The `dashboard/index.html` static page renders that JSON. The `stop_hook.ps1` re-invokes the watcher when more `review.pending` files exist.

This is clever and load-bearing but it lives across four top-level directories (`arbitrage_executor/`, `watcher/`, `dashboard/`, `scripts/`) with no map explaining the wiring. Onboarding requires reading the prompts to discover the architecture.

### G. The one real test is buried as a sibling of source

`arbitrage_executor/test_text_match.py` is a working pytest file with 20+ tests for `text_match.fuzzy_contains`. It's the only real test in the entire repo, and it lives next to the module under test instead of in a `tests/` directory. The root `tests/` directory is empty.

### H. `.gitignore` is missing real entries

Not ignored that should be:
- Root `audit_logs/` (only `arbitrage_executor/audit_logs/` is)
- Root `traces/`
- Root `*.png` (these should never be at the root, but the gitignore is also missing them as a safety net)
- Root `Userstkmer*.png` typo artifacts (path-bug class)
- `.venv` directories (only top-level `.venv/`; `arbitrage_executor/.venv/` is huge and not explicitly listed — it's working only because `.venv/` happens to match at any depth in Git's gitignore semantics. Worth being explicit.)

### What is *not* a problem (and why I'm not touching it)

- **`bet_placer.py` is 1062 lines.** Yes, it's a god object. CRITIQUE.md #12 already flagged this and the conclusion was *do not refactor until the selector smoke test is in place and proven*. Same conclusion here. Out of scope.
- **`chrome_helpers.py` launches Chrome with stealth flags.** Memory marks this as frozen — the bot detection bypass is hard-won. Out of scope.
- **Two `db_connection.py` files** (`arbitrage_executor/db_connection.py` and `app/shared/python/bountygate/utils/db_connection.py`). The user said "ignore /app" — and the split is documented in `CLAUDE.md`. Not refactoring.
- **The Airflow DAG layout.** Working as-is. The `airflow/dags/service_account.json` *might* be a credential — should be verified — but otherwise the DAG structure is fine.
- **The codegen / recorder system in `claude_toolkit/`.** Working. It's tooling for selectors, not part of the hot path. Treat as a unit.

---

## File Structure Changes

**End state:**

```
bountygate/
├── README.md                       # rewritten, honest
├── Makefile                        # NEW — doctor / worker / smoke / migrate / compose-up
├── .gitignore                      # expanded
├── pyproject.toml                  # either deleted or trimmed to dev-tools-only
├── airflow/                        # unchanged
├── app/                            # unchanged (ignored)
├── arbitrage_executor/
│   ├── CLAUDE.md                   # unchanged (canonical operator doc)
│   ├── SOP.md                      # unchanged
│   ├── USAGE.md                    # kept (operator quick-start)
│   ├── pyproject.toml              # NEW: real deps declared
│   ├── uv.lock                     # regenerated against real deps
│   ├── *.py                        # unchanged
│   ├── selectors/                  # unchanged
│   ├── tools/                      # MOVED from /claude_toolkit
│   ├── tests/                      # NEW: real test home, contains test_text_match.py
│   └── docs/                       # unchanged
├── db/                             # unchanged
├── dashboard/                      # unchanged
├── docs/
│   ├── superpowers/plans/          # already exists (this plan lives here)
│   └── archive/                    # NEW: CRITIQUE.md, RESEARCH.md, MAP_MISSING_MARKETS.md
├── scripts/
│   ├── README.md                   # NEW: explains operational vs archive
│   ├── migrate.py                  # operational, stays at scripts/ root
│   ├── dq_checks.py                # operational
│   ├── load_aliases.py             # operational
│   ├── load_market_aliases.py      # operational
│   ├── devdb.py                    # operational
│   ├── local_dev_setup.ps1         # operational
│   ├── start_watcher.ps1           # operational
│   └── archive/                    # MOVED: seed_aliases_*, normalize_*, generate_market_aliases
└── watcher/
    └── README.md                   # NEW: explains the watcher loop's wiring
```

**Files deleted:**

- All 9 root `fd_*.py` / `mgm_*.py` debug scripts
- All 9 root `fd_*.png` / `mgm_*.png` debug screenshots
- `Userstkmerbountygateplaywright_smoke.png`
- `arbitrage_executor/test_recording.mp4`
- Root `audit_logs/` directory (single test dir inside)
- Root `traces/` directory
- Root `pyproject.toml` (if deleted) OR trimmed
- `arbitrage_executor/scripts/` (single-file dir — move `drift_check.ps1` up or to `arbitrage_executor/tools/`)
- `arbitrage_executor/GEMINI.md` if duplicate of CLAUDE.md (Task 6 decides)
- Empty `tests/` directory at root (after `test_text_match.py` moves; see Task 17)
- Empty `infra/` directory

**Files moved:**

- `claude_toolkit/` → `arbitrage_executor/tools/`
- `arbitrage_executor/test_text_match.py` → `arbitrage_executor/tests/test_text_match.py`
- `CRITIQUE.md` → `docs/archive/CRITIQUE-2026-04-20.md`
- `RESEARCH.md` → `docs/archive/RESEARCH.md`
- `MAP_MISSING_MARKETS.md` → `docs/archive/MAP_MISSING_MARKETS-2026-05-14.md`
- `scripts/seed_aliases_{nba,nfl,mlb,nhl}.py` → `scripts/archive/`
- `scripts/normalize_lines.py` → `scripts/archive/`
- `scripts/normalize_team_blobs.py` → `scripts/archive/`
- `scripts/generate_market_aliases.py` → `scripts/archive/`

**Files created:**

- `Makefile`
- `scripts/README.md`
- `watcher/README.md`
- `arbitrage_executor/pyproject.toml` (rewrite with real deps)
- `arbitrage_executor/tests/__init__.py` (if pytest needs it)
- `docs/archive/README.md`

---

## Phase 1 — Strip prototype cruft from the root

### Task 1: Delete loose root debug scripts and screenshots

**Files:**
- Delete: `fd_inspect.py`, `fd_cleanup.py`, `fd_clear_slip.py`, `mgm_probe.py`, `mgm_probe2.py`, `mgm_clear_slip.py`, `mgm_clear_wide.py`, `mgm_clear_wide2.py`, `mgm_clear_x.py`
- Delete: `fd_after_done.png`, `fd_final.png`, `fd_state.png`, `mgm_after_clear.png`, `mgm_cleared.png`, `mgm_final.png`, `mgm_state.png`, `mgm_wide_after.png`, `mgm_wide_home.png`
- Delete: `Userstkmerbountygateplaywright_smoke.png`
- Delete: `arbitrage_executor/test_recording.mp4`

- [ ] **Step 1: Verify nothing imports the debug scripts** (already done in plan-writing — re-verify before deleting)

Run:
```powershell
Get-ChildItem -Recurse -Include *.py,*.md,*.ps1 -Path C:\Users\tkmer\bountygate -Exclude .venv | Select-String -Pattern "fd_inspect|fd_cleanup|fd_clear_slip|mgm_probe|mgm_clear|test_recording" | Where-Object { $_.Path -notlike "*\.venv\*" }
```

Expected: zero matches (already verified during plan writing).

- [ ] **Step 2: Remove the debug scripts**

```powershell
Remove-Item -Path C:\Users\tkmer\bountygate\fd_inspect.py, C:\Users\tkmer\bountygate\fd_cleanup.py, C:\Users\tkmer\bountygate\fd_clear_slip.py, C:\Users\tkmer\bountygate\mgm_probe.py, C:\Users\tkmer\bountygate\mgm_probe2.py, C:\Users\tkmer\bountygate\mgm_clear_slip.py, C:\Users\tkmer\bountygate\mgm_clear_wide.py, C:\Users\tkmer\bountygate\mgm_clear_wide2.py, C:\Users\tkmer\bountygate\mgm_clear_x.py
```

- [ ] **Step 3: Remove the debug screenshots**

```powershell
Remove-Item -Path C:\Users\tkmer\bountygate\fd_after_done.png, C:\Users\tkmer\bountygate\fd_final.png, C:\Users\tkmer\bountygate\fd_state.png, C:\Users\tkmer\bountygate\mgm_after_clear.png, C:\Users\tkmer\bountygate\mgm_cleared.png, C:\Users\tkmer\bountygate\mgm_final.png, C:\Users\tkmer\bountygate\mgm_state.png, C:\Users\tkmer\bountygate\mgm_wide_after.png, C:\Users\tkmer\bountygate\mgm_wide_home.png, C:\Users\tkmer\bountygate\Userstkmerbountygateplaywright_smoke.png
```

- [ ] **Step 4: Remove the test recording**

```powershell
Remove-Item -Path C:\Users\tkmer\bountygate\arbitrage_executor\test_recording.mp4
```

- [ ] **Step 5: Remove the orphan root-level audit_logs/ and traces/ directories**

```powershell
Remove-Item -Recurse -Force -Path C:\Users\tkmer\bountygate\audit_logs
Remove-Item -Recurse -Force -Path C:\Users\tkmer\bountygate\traces
```

(These were created by misnav from CWD. They're not the real `arbitrage_executor/audit_logs/` which is the live one.)

- [ ] **Step 6: Verify the root is clean**

Run:
```powershell
Get-ChildItem -Path C:\Users\tkmer\bountygate -Force | Where-Object { $_.Name -notmatch "^\." } | Select-Object Name
```

Expected output should contain ONLY: `airflow`, `app`, `arbitrage_executor`, `claude_toolkit`, `dashboard`, `db`, `docs`, `infra`, `scripts`, `tests`, `watcher`, `CRITIQUE.md`, `MAP_MISSING_MARKETS.md`, `pyproject.toml`, `README.md`, `RESEARCH.md`. **No loose `*.py`, `*.png`, or `*.mp4` files.**

### Task 2: Expand .gitignore for missing patterns

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Add root-level runtime output patterns**

Edit `C:\Users\tkmer\bountygate\.gitignore` and add this block at the end (before any existing OS section):

```
# Root-level runtime output (misnav guards)
/audit_logs/
/traces/
/*.png
/*.mp4
/Userstkmer*

# Explicit venv ignores (defense-in-depth)
arbitrage_executor/.venv/
```

- [ ] **Step 2: Verify gitignore catches them**

Run (should produce empty output for the previously-untracked files; or if files don't exist, output `Would skip ...`):
```powershell
git check-ignore -v audit_logs/ traces/ fd_test.png Userstkmer_smoke.png arbitrage_executor/.venv/pyvenv.cfg
```

Expected: each path returns a line citing the rule that ignored it.

### Task 3: Commit Phase 1

- [ ] **Step 1: Stage the cleanup**

```powershell
git add -A
git status
```

Expected status: deletions of the cruft files + modification of `.gitignore`. No surprise additions.

- [ ] **Step 2: Commit**

```powershell
git commit -m @'
chore: strip prototype debug artifacts and tighten gitignore

Remove 9 loose root Playwright debug scripts (fd_*.py, mgm_*.py),
9 matching debug screenshots, the path-typo artifact
Userstkmerbountygateplaywright_smoke.png, an orphan test recording,
and the misnav-created root-level audit_logs/ and traces/ dirs.
None were imported or referenced — pure exploration cruft.

Tighten .gitignore so future CWD bugs don't recreate the mess at root.
'@
```

---

## Phase 2 — Tell the truth in documentation

### Task 4: Rewrite the root README to match reality

**Files:**
- Modify: `README.md` (full rewrite)

- [ ] **Step 1: Replace README.md with accurate content**

Replace the entire contents of `C:\Users\tkmer\bountygate\README.md` with:

````markdown
# bountygate

Sports arbitrage stack. Two halves that share a Postgres database:

1. **Analytics** (`airflow/` + `app/shared/python/`) — Airflow DAGs ingest odds from The Odds API, normalize player/market names, write candidate arbitrage opportunities to Postgres.
2. **Execution** (`arbitrage_executor/`) — Local Windows Playwright bot that polls the queue, opens Chrome with stealth flags via CDP, places paired bets on FanDuel + BetMGM. Audit-screenshots every step. Discord alerts on success/failure/orphans.

A **watcher** loop (`watcher/`) records each execution to `arbitrage_executor/audit_logs/<run>/recording.mp4`, runs the `/watch:watch` skill in a parallel Claude session to review it, and writes findings to `dashboard/data.json` for the static page at `dashboard/index.html`.

## Layout

| Path | Role |
|------|------|
| `airflow/` | Airflow 3 DAGs + docker-compose local stack. Analytics pipeline. |
| `app/shared/python/bountygate/` | Shared ETL utilities imported by both DAGs and the executor. Installable via `pip install -e ./app/shared/python`. |
| `arbitrage_executor/` | The live betting bot. **Start here for operational work.** See `arbitrage_executor/CLAUDE.md`. |
| `arbitrage_executor/tools/` | Operational scripts: doctor, smoke test, queue inspector, stuck-task rescue, codegen recorder/replay. |
| `db/migrations/` | Hand-rolled SQL migrations applied by `scripts/migrate.py`. |
| `db/aliases/`, `db/market_aliases/` | Reference data loaded by `scripts/load_aliases.py` and `scripts/load_market_aliases.py`. |
| `dashboard/` | Static HTML dashboard rendering `data.json` (watcher output). |
| `watcher/` | Prompts + stop hook for the video-feedback-loop Claude session. See `watcher/README.md`. |
| `scripts/` | Load-bearing operational scripts (migrate, dq_checks, aliases loaders). `scripts/archive/` is historical one-offs. |
| `docs/` | Plans (`docs/superpowers/plans/`) and historical critiques (`docs/archive/`). |

## Quick start

### Run the bot

```powershell
cd arbitrage_executor
uv sync
python task_worker.py
```

Chrome auto-launches with the stealth profile if not already running. The worker polls `bot_execution_queue` every 15s. See `arbitrage_executor/CLAUDE.md` for the full operator runbook and Discord alert handling.

### Run the analytics pipeline locally

```powershell
cd airflow
docker compose up --build
```

Airflow webserver: <http://localhost:8080>.

### Apply DB migrations

```powershell
python scripts/migrate.py up
```

## Environment

Repo-root `.env` (gitignored). Required:

- `DATABASE_URL` — Postgres RDS connection string
- `BG_DISCORD_WEBHOOK_URL` — Discord webhook for alerts (no default; bot fails fast if missing)
- `ODDS_API_KEY` — The Odds API key
- Optional: `TESTING_MODE`, `WAGER_SCALE_FACTOR`, `MIN_ROI_THRESHOLD`, `HEARTBEAT_INTERVAL_MINUTES`

## Tests

```powershell
cd arbitrage_executor
python -m pytest tests/ -v
```

The bot's hot path is not test-driven (the value is in real Playwright runs against the live UIs). Tests focus on pure-function modules: text matching, ROI math.

## Where to look when something breaks

- Bot stopped placing bets → `arbitrage_executor/SOP.md` (UI-break recovery runbook).
- Stuck task in `RUNNING` → `python arbitrage_executor/tools/rescue_stuck_tasks.py`.
- New market not mapped → `python arbitrage_executor/map_selectors.py --site <site> --market <market>`.
- DAG not producing opportunities → check Airflow UI; data in `bg_arbitrage_player_props*` tables.
- Discord alerts → see `arbitrage_executor/CLAUDE.md` § "Operator runbook (Discord alerts)".

## Outstanding architectural concerns

See `docs/archive/CRITIQUE-2026-04-20.md` for a deep-dive triage of reliability and observability gaps (orphan reconciler, balance verification, secret rotation, etc.). Many remain unaddressed.
````

- [ ] **Step 2: Verify links resolve**

Run:
```powershell
Test-Path C:\Users\tkmer\bountygate\arbitrage_executor\CLAUDE.md, C:\Users\tkmer\bountygate\arbitrage_executor\SOP.md, C:\Users\tkmer\bountygate\watcher\README.md, C:\Users\tkmer\bountygate\docs\archive\CRITIQUE-2026-04-20.md, C:\Users\tkmer\bountygate\scripts\migrate.py
```

Some will be `False` until later tasks complete — that's expected — note which fail and confirm they're created by later tasks (watcher/README.md by Task 9, docs/archive/CRITIQUE-2026-04-20.md by Task 5, arbitrage_executor/tools/ by Task 10).

### Task 5: Archive stale top-level docs

**Files:**
- Create: `docs/archive/CRITIQUE-2026-04-20.md` (moved from `CRITIQUE.md`)
- Create: `docs/archive/RESEARCH.md` (moved from `RESEARCH.md`)
- Create: `docs/archive/MAP_MISSING_MARKETS-2026-05-14.md` (moved from `MAP_MISSING_MARKETS.md`)
- Create: `docs/archive/README.md`
- Delete: `CRITIQUE.md`, `RESEARCH.md`, `MAP_MISSING_MARKETS.md` (after move via `git mv`)

- [ ] **Step 1: Create archive directory**

```powershell
New-Item -ItemType Directory -Force -Path C:\Users\tkmer\bountygate\docs\archive
```

- [ ] **Step 2: Move docs preserving git history**

```powershell
git mv CRITIQUE.md docs/archive/CRITIQUE-2026-04-20.md
git mv RESEARCH.md docs/archive/RESEARCH.md
git mv MAP_MISSING_MARKETS.md docs/archive/MAP_MISSING_MARKETS-2026-05-14.md
```

- [ ] **Step 3: Create archive index**

Write `C:\Users\tkmer\bountygate\docs\archive\README.md`:

```markdown
# Archive

Historical documents kept for context. **Not authoritative** — refer to current files in the repo for what's live.

| File | Date | Purpose | Status |
|------|------|---------|--------|
| `CRITIQUE-2026-04-20.md` | 2026-04-20 | Deep-dive triage of reliability/observability gaps. Many items still open. | Reference. Items should migrate to GitHub issues when worked. |
| `RESEARCH.md` | early prototyping | Exploration notes from the original prototype phase. | Frozen. |
| `MAP_MISSING_MARKETS-2026-05-14.md` | 2026-05-14 | One-time runbook for mapping a batch of missing markets discovered in that run. | Frozen. If markets are still unmapped, re-derive from current `logs/unmapped_markets.log`. |
```

### Task 6: Resolve duplicate / overlapping `arbitrage_executor` docs

**Files:**
- Read: `arbitrage_executor/GEMINI.md`, `arbitrage_executor/CLAUDE.md`, `arbitrage_executor/USAGE.md`
- Action: decide on consolidation

- [ ] **Step 1: Compare GEMINI.md vs CLAUDE.md**

Run:
```powershell
fc.exe C:\Users\tkmer\bountygate\arbitrage_executor\CLAUDE.md C:\Users\tkmer\bountygate\arbitrage_executor\GEMINI.md
```

If files are identical (likely): proceed to step 2. If they differ meaningfully (e.g., GEMINI.md has Gemini-CLI-specific notes), skip to step 3.

- [ ] **Step 2: If identical, replace GEMINI.md with a stub that references CLAUDE.md**

Write `C:\Users\tkmer\bountygate\arbitrage_executor\GEMINI.md`:

```markdown
See [CLAUDE.md](./CLAUDE.md) — the same content applies to Gemini CLI.
```

(Rationale: keeping GEMINI.md as a discoverable file matters for Gemini-CLI auto-loading; but having two copies of the same content is a documentation hazard. A pointer is the lean fix.)

- [ ] **Step 3: If they differ, leave both in place but note the difference**

Add a one-line note at the top of GEMINI.md explaining what it adds beyond CLAUDE.md. Skip step 2.

- [ ] **Step 4: Confirm USAGE.md and CLAUDE.md don't dangerously overlap**

Read both files side by side. Acceptable overlap: high-level overview. Hazardous overlap: contradictory instructions (e.g., different commands to run, different env vars). Note any contradictions in a follow-up TODO at the bottom of CLAUDE.md (don't fix in this plan — those edits risk breaking the operational doc).

### Task 7: Commit Phase 2

- [ ] **Step 1: Verify the README links resolve to existing files where applicable**

Run:
```powershell
Test-Path C:\Users\tkmer\bountygate\docs\archive\CRITIQUE-2026-04-20.md, C:\Users\tkmer\bountygate\docs\archive\RESEARCH.md, C:\Users\tkmer\bountygate\docs\archive\MAP_MISSING_MARKETS-2026-05-14.md
```

Expected: all `True`.

- [ ] **Step 2: Stage and commit**

```powershell
git add -A
git status
```

Expected: README modified, three doc files moved into `docs/archive/`, archive README created, possibly `GEMINI.md` modified.

```powershell
git commit -m @'
docs: rewrite root README to match reality + archive stale docs

The old README described a Streamlit/Heroku monorepo that doesn't
exist. New README describes what's actually in the repo: the
analytics pipeline (Airflow), the execution bot (arbitrage_executor),
the watcher feedback loop, and the operational scripts.

CRITIQUE.md, RESEARCH.md, MAP_MISSING_MARKETS.md moved to docs/archive/
with date suffixes so their point-in-time nature is obvious.
'@
```

---

## Phase 3 — Reorganize scripts/ and clarify the toolkit

### Task 8: Split `scripts/` into operational vs archive

**Files:**
- Create: `scripts/archive/` directory
- Move (via `git mv`): `seed_aliases_mlb.py`, `seed_aliases_nba.py`, `seed_aliases_nfl.py`, `seed_aliases_nhl.py`, `normalize_lines.py`, `normalize_team_blobs.py`, `generate_market_aliases.py` → `scripts/archive/`
- Stays at `scripts/` root: `migrate.py`, `dq_checks.py`, `load_aliases.py`, `load_market_aliases.py`, `devdb.py`, `local_dev_setup.ps1`, `start_watcher.ps1`
- Create: `scripts/README.md`

- [ ] **Step 1: Create archive subdir**

```powershell
New-Item -ItemType Directory -Force -Path C:\Users\tkmer\bountygate\scripts\archive
```

- [ ] **Step 2: Move historical one-offs**

```powershell
git mv scripts/seed_aliases_mlb.py scripts/archive/seed_aliases_mlb.py
git mv scripts/seed_aliases_nba.py scripts/archive/seed_aliases_nba.py
git mv scripts/seed_aliases_nfl.py scripts/archive/seed_aliases_nfl.py
git mv scripts/seed_aliases_nhl.py scripts/archive/seed_aliases_nhl.py
git mv scripts/normalize_lines.py scripts/archive/normalize_lines.py
git mv scripts/normalize_team_blobs.py scripts/archive/normalize_team_blobs.py
git mv scripts/generate_market_aliases.py scripts/archive/generate_market_aliases.py
```

- [ ] **Step 3: Verify nothing references the moved scripts by path**

Run:
```powershell
Get-ChildItem -Recurse -Include *.py,*.md,*.ps1,*.yaml,*.yml -Path C:\Users\tkmer\bountygate -Exclude .venv,chrome_profile | Select-String -Pattern "seed_aliases_(mlb|nba|nfl|nhl)|normalize_lines|normalize_team_blobs|generate_market_aliases" | Where-Object { $_.Path -notlike "*\.venv\*" -and $_.Path -notlike "*\archive\*" -and $_.Path -notlike "*\chrome_profile\*" }
```

Expected: zero matches (these are pure one-offs). If a match appears in a load-bearing script (e.g., `load_aliases.py`), update the reference to point at `scripts/archive/` — but more likely the reference is in DAG code or a doc that's now stale.

- [ ] **Step 4: Write `scripts/README.md`**

```markdown
# scripts/

## Operational (used in normal operation)

| Script | Purpose |
|--------|---------|
| `migrate.py` | Run/rollback DB migrations from `db/migrations/`. **Run on every fresh DB setup.** |
| `dq_checks.py` | Data-quality checks against the analytics tables. Wired into the Airflow DQ DAG. |
| `load_aliases.py` | Load player aliases from `db/aliases/` into Postgres. |
| `load_market_aliases.py` | Load market name aliases from `db/market_aliases/` into Postgres. |
| `devdb.py` | Local dev DB helpers (drop/reset/seed). |
| `local_dev_setup.ps1` | Bootstrap a fresh dev machine. |
| `start_watcher.ps1` | Launch the video-feedback-loop Claude watcher session. See `watcher/README.md`. |

## archive/

Historical one-off scripts kept for reference. Not run in normal operation. If a sport is added or aliases need re-seeding from scratch, these are the templates — but expect to update them before running.
```

### Task 9: Document the watcher loop

**Files:**
- Create: `watcher/README.md`

- [ ] **Step 1: Write `watcher/README.md`**

````markdown
# watcher — Video-feedback loop

A second Claude Code session reviews every arb execution's screen recording and surfaces issues to a dashboard. The bot doesn't need this to function — it's a quality-improvement loop.

## Wiring

```
arbitrage_executor/task_worker.py
        |
        | each execution writes:
        v
arbitrage_executor/audit_logs/<ts>_<player>_<market>/
        ├── recording.mp4         ← screen_recorder.py captures the run
        ├── opportunity_info.json
        ├── *.png                 ← failure screenshots
        └── review.pending        ← signal: needs review
                |
                | a watcher Claude session picks this up:
                v
        scripts/start_watcher.ps1
                |
                | runs INITIAL_PROMPT.md → loops:
                |   1. find oldest review.pending
                |   2. /watch:watch on recording.mp4 with review_prompt.md
                |   3. write review.md
                |   4. append entry to dashboard/data.json
                |   5. delete review.pending → create review.done
                |
                | watcher/stop_hook.ps1 re-invokes the session if
                | review.pending files remain
                v
        dashboard/data.json  ← rendered by dashboard/index.html
```

## Files

- `INITIAL_PROMPT.md` — the system prompt for the watcher session. Defines the per-recording loop.
- `review_prompt.md` — the question passed to `/watch:watch` for each recording.
- `stop_hook.ps1` — Claude Code stop hook that re-invokes if `review.pending` files exist.

## Operating

Start the watcher: `& 'C:\Users\tkmer\bountygate\scripts\start_watcher.ps1'`

It will keep running (processing then sleeping) until there are no pending reviews, then exit.

## Why it lives across four directories

The recording producer (`arbitrage_executor/screen_recorder.py`), the prompts (`watcher/`), the launch script (`scripts/start_watcher.ps1`), and the dashboard (`dashboard/`) are split because each piece has a different deployment surface: the recorder runs inside the bot process, the watcher runs as a separate Claude session, the dashboard is a static page, and the launch script bridges the user's shell to the Claude session. Keeping them separate keeps each piece minimal.
````

### Task 10: Move `claude_toolkit/` under `arbitrage_executor/tools/`

**Rationale:** 90% of `claude_toolkit/` is bot operational tools. The remaining piece (`dag_state.py`) is also bot-adjacent — it inspects Airflow DAG state from the perspective of "is the analytics pipeline feeding the bot?". The `claude_toolkit` name is misleading (these aren't Claude skills; they're operational scripts). Moving to `arbitrage_executor/tools/` colocates them with the bot they support.

**Files:**
- Move via `git mv`: `claude_toolkit/` → `arbitrage_executor/tools/`
- Update references: any doc, prompt, or memory pointing at `claude_toolkit/`

- [ ] **Step 1: Locate all references**

Run:
```powershell
Get-ChildItem -Recurse -Include *.py,*.md,*.ps1,*.yaml,*.yml -Path C:\Users\tkmer\bountygate -Exclude .venv,chrome_profile | Select-String -Pattern "claude_toolkit" | Where-Object { $_.Path -notlike "*\.venv\*" -and $_.Path -notlike "*\chrome_profile\*" }
```

Note all hits — you'll need to update each.

- [ ] **Step 2: Move the directory preserving history**

```powershell
git mv claude_toolkit arbitrage_executor/tools
```

- [ ] **Step 3: Update every reference**

For each file in the list from step 1, use `Edit` to replace `claude_toolkit/` with `arbitrage_executor/tools/` (or just `tools/` if the context is already inside `arbitrage_executor/`).

Known files likely needing updates (verify against your step-1 output):
- `README.md` (already references `arbitrage_executor/tools/` per Task 4 — verify no stragglers)
- `arbitrage_executor/CLAUDE.md`
- `docs/archive/CRITIQUE-2026-04-20.md` (5–6 references to `claude_toolkit/...`)
- Possibly `airflow/dags/*.py` for `dag_state.py` import
- Possibly `watcher/INITIAL_PROMPT.md` or `watcher/review_prompt.md`

- [ ] **Step 4: Verify imports still resolve**

If any Python module in the moved tree was being imported from outside `arbitrage_executor/` (e.g., from an Airflow DAG), update the import. Most likely culprit: `dag_state.py` referenced by an Airflow DAG.

Run:
```powershell
Get-ChildItem -Recurse -Include *.py -Path C:\Users\tkmer\bountygate\airflow | Select-String -Pattern "claude_toolkit|from tools"
```

Update any matches.

- [ ] **Step 5: Verify the move from a test**

```powershell
cd C:\Users\tkmer\bountygate\arbitrage_executor
python -c "import sys; sys.path.insert(0, '.'); from tools import doctor; print('OK')"
```

Expected: `OK`. (If `tools/doctor.py` imports relative-pathed siblings, this will surface that.)

### Task 11: Collapse single-file `arbitrage_executor/scripts/`

**Files:**
- Move: `arbitrage_executor/scripts/drift_check.ps1` → `arbitrage_executor/tools/drift_check.ps1`
- Delete: empty `arbitrage_executor/scripts/` directory

- [ ] **Step 1: Move the file**

```powershell
git mv arbitrage_executor/scripts/drift_check.ps1 arbitrage_executor/tools/drift_check.ps1
```

- [ ] **Step 2: Remove the now-empty directory**

```powershell
Remove-Item -Recurse -Force C:\Users\tkmer\bountygate\arbitrage_executor\scripts
```

(Git ignores empty directories, so no `git rm` needed.)

- [ ] **Step 3: Check for references to `arbitrage_executor/scripts/drift_check.ps1`**

Run:
```powershell
Get-ChildItem -Recurse -Include *.md,*.ps1,*.py -Path C:\Users\tkmer\bountygate -Exclude .venv | Select-String -Pattern "arbitrage_executor[/\\]scripts" | Where-Object { $_.Path -notlike "*\.venv\*" }
```

Update any matches to point to `arbitrage_executor/tools/drift_check.ps1`.

### Task 12: Commit Phase 3

- [ ] **Step 1: Stage and verify**

```powershell
git add -A
git status
```

Expected status: ~30 files renamed under `scripts/archive/`, `arbitrage_executor/tools/`, plus the new READMEs, plus reference updates in a handful of docs.

- [ ] **Step 2: Commit**

```powershell
git commit -m @'
refactor: consolidate scripts/, move claude_toolkit -> arbitrage_executor/tools

- scripts/ now has only load-bearing scripts; one-offs moved to scripts/archive/
- claude_toolkit/ is renamed and moved under arbitrage_executor/tools/ since
  its contents are operational tools for the bot, not Claude skills
- arbitrage_executor/scripts/ (single file) collapsed into tools/
- watcher/README.md documents the video-feedback-loop wiring across
  arbitrage_executor/, watcher/, scripts/, and dashboard/
'@
```

---

## Phase 4 — Pin dependencies and add a single boot command

### Task 13: Declare real dependencies in `arbitrage_executor/pyproject.toml`

**Files:**
- Modify: `arbitrage_executor/pyproject.toml`
- Regenerate: `arbitrage_executor/uv.lock`

- [ ] **Step 1: Read the current pyproject.toml**

```powershell
Get-Content C:\Users\tkmer\bountygate\arbitrage_executor\pyproject.toml
```

Note: this is the source of truth right now. Whatever's in it stays unless explicitly changed.

- [ ] **Step 2: Discover actual imports in the bot**

Run:
```powershell
Get-ChildItem -Path C:\Users\tkmer\bountygate\arbitrage_executor -Filter *.py -File | Select-String -Pattern "^(import|from) " | ForEach-Object { $_.Line } | Sort-Object -Unique
```

Identify external packages (anything that's not stdlib and not a sibling `.py` in the same directory). Typical list:
- `playwright`
- `sqlalchemy`
- `pandas`
- `requests`
- `pyyaml` (imported as `yaml`)
- `python-dotenv` (imported as `dotenv`)
- `psycopg2-binary` (or `psycopg`) — implied by `sqlalchemy` + Postgres
- `pillow` — for screenshots? verify
- `ffmpeg-python` or just subprocess — verify against `screen_recorder.py`

- [ ] **Step 3: Edit pyproject.toml to declare the discovered deps**

In `[project]` or `[tool.uv]` (depending on current schema — keep the current style), add the dependencies discovered in step 2, with version constraints that match the installed versions in `arbitrage_executor/.venv/Lib/site-packages/`.

For each, find the installed version:
```powershell
Get-ChildItem -Path C:\Users\tkmer\bountygate\arbitrage_executor\.venv\Lib\site-packages -Filter "*.dist-info" -Directory | Select-Object Name
```

Pin to a known-working pair: e.g. `playwright ~= 1.55` (whatever's installed), `sqlalchemy ~= 2.0`, `pandas ~= 2.2`, etc. Use `~=` (compatible release) for stability without total stagnation.

- [ ] **Step 4: Regenerate the lockfile**

```powershell
cd C:\Users\tkmer\bountygate\arbitrage_executor
uv lock
```

Expected: `uv.lock` now contains pinned entries for every declared dependency and its transitive closure.

- [ ] **Step 5: Verify a clean install reproduces the working environment**

This is risk-bearing — back up the venv first.

```powershell
Rename-Item C:\Users\tkmer\bountygate\arbitrage_executor\.venv C:\Users\tkmer\bountygate\arbitrage_executor\.venv.bak
cd C:\Users\tkmer\bountygate\arbitrage_executor
uv sync
```

Then run a smoke test that doesn't touch sportsbooks:
```powershell
python -c "import playwright, sqlalchemy, pandas, yaml, requests; print('imports OK')"
python -m pytest tests/ -v   # only test_text_match.py for now (post Task 17)
```

If both succeed: delete `.venv.bak`. If either fails: rename `.venv.bak` back to `.venv` and iterate on pyproject.toml until it works.

### Task 14: Decide the root pyproject.toml's fate

**Files:**
- Decision: keep slim, or delete

The root `pyproject.toml` declares Poetry, `package-mode = false`, only declares `pytest` as a dev dep. Two clean options:

1. **Delete it.** The repo isn't a single Python project; the real project is `arbitrage_executor/`. Root tests run via `cd arbitrage_executor && pytest`. Cleaner.
2. **Trim it to dev-tools-only.** Useful only if you want repo-wide `ruff`/`pytest` settings for `airflow/` and `scripts/` too.

- [ ] **Step 1: Check whether anything reads the root pyproject.toml**

Run:
```powershell
Get-ChildItem -Recurse -Include *.yml,*.yaml,*.toml,*.cfg,Makefile,*.ps1,*.sh -Path C:\Users\tkmer\bountygate -Exclude .venv | Select-String -Pattern "pyproject.toml|poetry|tool\.poetry" | Where-Object { $_.Path -notlike "*\.venv\*" -and $_.Path -notlike "*\arbitrage_executor\*" }
```

If nothing references it: prefer option 1 (delete). If CI or `local_dev_setup.ps1` references it: prefer option 2 (trim).

- [ ] **Step 2A (if deleting): remove it**

```powershell
git rm pyproject.toml
```

- [ ] **Step 2B (if trimming): replace contents**

Write the new minimal `pyproject.toml`:

```toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.pytest.ini_options]
testpaths = ["arbitrage_executor/tests", "tests"]
```

(No Poetry section. Not a buildable project — just a config carrier.)

### Task 15: Add a Makefile

**Files:**
- Create: `Makefile`

- [ ] **Step 1: Verify `make` is available on the user's machine**

```powershell
Get-Command make -ErrorAction SilentlyContinue
```

If not present: switch to a `justfile` instead (just is more idiomatic on Windows + cross-platform). Adjust the step below.

- [ ] **Step 2: Create the Makefile (or justfile)**

Write `C:\Users\tkmer\bountygate\Makefile`:

```makefile
.PHONY: doctor worker smoke migrate compose-up help

help:
	@echo "Targets:"
	@echo "  doctor       — run tools/doctor.py (DB, Chrome, Discord checks)"
	@echo "  worker       — start the arbitrage executor worker"
	@echo "  smoke        — run the selector smoke test"
	@echo "  migrate      — apply DB migrations"
	@echo "  compose-up   — start Airflow stack via docker compose"

doctor:
	cd arbitrage_executor && python tools/doctor.py

worker:
	cd arbitrage_executor && python task_worker.py

smoke:
	cd arbitrage_executor && python tools/selector_smoke_test.py

migrate:
	python scripts/migrate.py up

compose-up:
	cd airflow && docker compose up --build
```

- [ ] **Step 3: Sanity-test each target**

```powershell
make help
```

Expected: lists all targets. Don't run the workload targets (they have side effects); just confirm `make` resolves them.

### Task 16: Commit Phase 4

- [ ] **Step 1: Stage and verify**

```powershell
git add -A
git status
```

Expected: modified `arbitrage_executor/pyproject.toml`, new/updated `arbitrage_executor/uv.lock`, root `pyproject.toml` deleted or trimmed, new `Makefile`.

- [ ] **Step 2: Commit**

```powershell
git commit -m @'
build: pin arbitrage_executor deps + add Makefile boot commands

arbitrage_executor/pyproject.toml now declares real dependencies
(playwright, sqlalchemy, pandas, requests, pyyaml, python-dotenv,
psycopg2-binary). uv.lock regenerated; a fresh checkout can reproduce
the working venv via `uv sync`.

Root pyproject.toml trimmed/removed — the repo isn't a single project,
and the empty Poetry config was misleading new contributors.

Makefile at root collapses tribal onboarding ("cd here, run that") into
one-word commands: doctor, worker, smoke, migrate, compose-up.
'@
```

---

## Phase 5 — Decide the fate of empty placeholders

### Task 17: Move the one real test into a proper home

**Files:**
- Move: `arbitrage_executor/test_text_match.py` → `arbitrage_executor/tests/test_text_match.py`
- Create: `arbitrage_executor/tests/__init__.py` (only if pytest config needs it)
- Decide: root `tests/` fate

- [ ] **Step 1: Create the proper tests directory**

```powershell
New-Item -ItemType Directory -Force -Path C:\Users\tkmer\bountygate\arbitrage_executor\tests
```

- [ ] **Step 2: Move the test file preserving git history**

```powershell
git mv arbitrage_executor/test_text_match.py arbitrage_executor/tests/test_text_match.py
```

- [ ] **Step 3: Adjust the import in the moved test**

Read `arbitrage_executor/tests/test_text_match.py` — the current import is `from text_match import fuzzy_contains`. Since the test now lives one dir deeper, decide:

- Option A: Add a `conftest.py` at `arbitrage_executor/tests/` that injects the parent dir into `sys.path`:

```python
# arbitrage_executor/tests/conftest.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
```

- Option B: Run pytest from `arbitrage_executor/` and rely on pytest's rootdir behavior. Test the smaller change first.

Try Option B first:
```powershell
cd C:\Users\tkmer\bountygate\arbitrage_executor
python -m pytest tests/test_text_match.py -v
```

If it fails with `ModuleNotFoundError: text_match`: add the conftest.py from Option A.

- [ ] **Step 4: Confirm tests pass**

```powershell
cd C:\Users\tkmer\bountygate\arbitrage_executor
python -m pytest tests/ -v
```

Expected: 20+ tests in `test_text_match.py` pass.

- [ ] **Step 5: Delete the empty root `tests/` directory**

```powershell
Remove-Item -Recurse -Force C:\Users\tkmer\bountygate\tests
```

(The root README's pytest section already points to `arbitrage_executor/tests/`.)

### Task 18: Delete the empty `infra/` directory

**Rationale:** README claimed `infra/` had a Heroku Procfile and runtime.txt. Both fictions. The directory is empty. Deleting it removes a confusing pointer.

- [ ] **Step 1: Confirm it's empty**

```powershell
Get-ChildItem -Path C:\Users\tkmer\bountygate\infra -Force
```

Expected: empty output (or just `.gitkeep` if there is one). If non-empty, examine the contents before deleting.

- [ ] **Step 2: Delete it**

```powershell
Remove-Item -Recurse -Force C:\Users\tkmer\bountygate\infra
```

(Git won't notice a deleted empty directory; no `git rm`. But if there's a `.gitkeep`, `git rm` it first.)

### Task 19: Commit Phase 5

- [ ] **Step 1: Stage and verify**

```powershell
git add -A
git status
```

Expected: `arbitrage_executor/test_text_match.py` renamed to `arbitrage_executor/tests/test_text_match.py`, possibly a new `conftest.py`, possibly a deleted `.gitkeep` in `infra/`, deleted empty `tests/` dir if it had a tracked `.gitkeep`.

- [ ] **Step 2: Commit**

```powershell
git commit -m @'
test: move text_match tests into arbitrage_executor/tests/

The one real test in the repo (20+ pytest cases for fuzzy_contains)
lived as a sibling of source. Now in the proper tests/ directory
alongside the module it tests. Empty root tests/ and infra/ dirs
removed — README's pytest target is now real.
'@
```

---

## Self-Review Checklist

After all 19 tasks complete:

- [ ] **Coverage check.** Every finding A–H in the review section has a corresponding task: A (Tasks 1–3), B (Task 4), C (Tasks 8, 10, 11), D (Tasks 5, 6), E (Tasks 13, 14), F (Task 9), G (Task 17), H (Task 2). ✓
- [ ] **Placeholder scan.** No "TBD", no "add appropriate", no "similar to above". Each step has the actual commands or the actual content.
- [ ] **Reference consistency.** `arbitrage_executor/tools/` (the new home of `claude_toolkit/`) is referenced consistently in the README (Task 4), the watcher README (Task 9), and the Makefile (Task 15).
- [ ] **Reversibility.** Every git commit is independently revertable. Phase 1's cruft deletion can be undone with `git revert`. The moves in Phases 2/3/5 use `git mv` so history is preserved.
- [ ] **Out-of-scope items declared.** `bet_placer.py` refactor, `chrome_helpers.py` changes, the `/app` directory, the live DAGs, CRITIQUE.md reliability items — all explicitly out of scope and listed in the Findings.

## Validation Gates (between phases)

After each phase commits:

```powershell
cd C:\Users\tkmer\bountygate\arbitrage_executor
python -c "from execute_arb import ArbExecutor; print('imports OK')"
```

Expected: `imports OK` with no errors. If a phase breaks imports, the next phase shouldn't proceed.

For Phase 4 specifically, also:
```powershell
cd C:\Users\tkmer\bountygate\arbitrage_executor
uv sync
python -m pytest tests/ -v
```

## What this plan deliberately does NOT do

- Refactor `bet_placer.py` (CRITIQUE #12 — risky without smoke test).
- Touch `chrome_helpers.py` launch logic (frozen).
- Move shared code in or out of `app/` (per user instruction).
- Change Airflow DAG structure.
- Address reliability items in CRITIQUE.md (orphan reconciler, balance verification, credential rotation) — those are a separate, higher-priority plan.
- Add new features.
- Introduce CI/linting/type-checking (deferred to a separate plan; depends on the deps being pinned first, which this plan does in Phase 4).
