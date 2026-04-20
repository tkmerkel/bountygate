# bountygate — Deep-Dive Critique

_Prepared 2026-04-20. Scope: long-term stability and ease of use/maintenance, with weight toward reliability and observability (per project economics: ~$0.25 per-bet margin × ~100 bets/day means one unhedged leg costs 40+ successful bets)._

This is a triage document, not a redesign. Issues are ordered by financial/operational risk and grouped P0 → P1 → P2. Each item has **Why it matters**, **Where** (file:line), **Fix** (concrete; minimal blast radius preferred).

Nothing under `arbitrage_executor/chrome_helpers.py` launch logic is proposed for change — it is frozen, and any refactor there belongs in a separate, risk-aware session.

---

## P0 — actively leaks money, hides reality, or wastes onboarding time

### 1. Stuck `RUNNING` tasks are silently abandoned

**Where:** `arbitrage_executor/task_worker.py:97`, `arbitrage_executor/db_connection.py:219–244`
**Why it matters:** `claim_pending_task` only selects `WHERE status = 'PENDING'`. If the worker crashes after the atomic `UPDATE ... status='RUNNING', started_at=NOW()` but before `complete_task()` runs (e.g. Chrome OOMs, a `KeyboardInterrupt` between phases, power loss), the row sits in `RUNNING` forever. A human has to hand-edit the DB. No timeout, no reconciler, no alert.
**Fix (minimal):** At worker startup, run a one-shot reconciler: any row with `status='RUNNING'` and `started_at < NOW() - interval '30 min'` → mark `FAILED` with `error_log='Orphaned RUNNING; reclaimed at startup'`. Surface the count in the heartbeat. Optionally add a `claude_toolkit/rescue_stuck_tasks.py` for ad-hoc recovery (included in the toolkit shipped alongside this critique).

### 2. Odds API key hardcoded in source

**Where:** `app/shared/python/bountygate/utils/etl_assets.py:11` — `odds_apiKey = '9bc17b9e4...'`
**Why it matters:** Secret rotation requires a code edit; anyone with repo access has the key; the key cannot be scoped or expired cheaply. This is the one paid external API in the analytics side.
**Fix:** `os.environ["ODDS_API_KEY"]` with `KeyError` fail-fast. Move the current key into the already-gitignored `.env`. Rotate it at the Odds API.

### 3. Discord webhook fallback hardcoded in source

**Where:** `app/shared/python/bountygate/utils/discord_notify.py:29–32` — `_DEFAULT_WEBHOOK_URL = "https://discord.com/api/webhooks/..."`
**Why it matters:** The critical alert channel is the **only** signal you use to act on unhedged exposure. A leaked repo means an attacker can (a) spam the channel to desensitize you, or (b) impersonate a `🚨 CRITICAL` orphan message — which would cause you to manually "resolve" a phantom bet. `.env` is gitignored; the fallback is not.
**Fix:** Remove the fallback. `_webhook_url()` should raise `RuntimeError` if `BG_DISCORD_WEBHOOK_URL` is missing. Paying the cost of a one-time env setup across all callers is cheaper than one impersonated critical alert.

### 4. No balance reconciliation between phases

**Where:** `arbitrage_executor/execute_arb.py` phase transitions; no query against the sportsbook account.
**Why it matters:** Phase outcomes are trusted from `(status, message)` tuples returned by `bet_placer`. Failure modes that bypass this trust:
  - BetMGM slip shows accepted, server later rejects the ticket → you believe leg 1 is live when it isn't.
  - BetMGM accepts at a smaller stake than requested (limit applied silently) → hedge is sized wrong.
  - FanDuel shows accepted in slip, ticket later voided → unhedged exposure you don't see.
**Fix:** Add a pre-phase and post-phase account-balance read. Between phases 2 and 3, verify `balance_after_mgm ≈ balance_before_mgm − expected_stake`. Deviation > $1 → halt + CRITICAL. This is the single highest-leverage reliability addition; everything else is an approximation of it.

### 5. README describes a project that doesn't exist

**Where:** `README.md` (root)
**Why it matters:** Claims a Streamlit dashboard at `app/streamlit_app.py` (missing), Heroku deploy (`infra/` empty), and a test suite (`tests/` empty — pytest declared but zero tests). Future-you or Future-me will waste hours looking for these. `app/` is actually a shared Python package, not a UI.
**Fix:** Rewrite to what actually exists:
  - `airflow/` — Airflow 3 DAGs, docker-compose local dev.
  - `app/shared/python/bountygate/` — shared ETL utilities imported by DAGs and executor.
  - `arbitrage_executor/` — local Windows-only Playwright bot (links to `arbitrage_executor/CLAUDE.md`).
  - `scripts/` — migrations + one-offs (split into `operational/` vs `oneoff/` first, P2 #17).
  - `db/migrations/` — hand-rolled SQL migrations, applied via `scripts/migrate.py`.

---

## P1 — stability cliffs; fix before the next unsupervised run

### 6. Selector brittleness with no pre-flight check

**Where:** `arbitrage_executor/bet_placer.py` — e.g. `div.aq` (line 104), `div.hk`, `div.jo`, `div.bt`, `div.cg`
**Why it matters:** Obfuscated CSS classes change on every FanDuel/BetMGM build. Current failure mode: `BetPlacerError` → opportunity skipped → bot continues, silently unprofitable. Because this happens per-opportunity, the first signal is "earnings dropped to zero" hours later.
**Fix:** A selector smoke test that runs before each trading session: launch Chrome via the existing (frozen) `ensure_chrome_cdp`, navigate to an evergreen market on each site, assert each critical selector resolves, WARNING to Discord if any miss. Included as `claude_toolkit/selector_smoke_test.py`. Requires a user-specified "stable player" flag — nothing hardcoded (stable picks rot).

### 7. Broad `except Exception` in hedge-critical code

**Where:** `arbitrage_executor/bet_placer.py` — 30+ bare `except Exception:` blocks (lines 70, 105, 156, 248, 263, 297, 342, 424, 498, 521, 571, 583, 602, 612, 642, 646, 664, 689, 709, 754, 766, 828, 879, 901, 905, 944, 950, 983, 989, 1040, 1045)
**Why it matters:** Several swallow and proceed as if success (e.g. line 571 where "search input not found" falls through). In the hedge phase, silent success means unhedged exposure. This is the single mechanism by which a small UI change becomes a financial event instead of an alert.
**Fix (targeted, not global):** Audit each broad `except` in the hedge path (Phase 3 in `execute_arb.py` + everything `_place_fanduel_bet` touches). Narrow to specific transient errors (`PlaywrightTimeoutError`, `TargetClosedError`, `requests.ConnectionError`); let any other exception propagate. Expected scope: ~10 of the 30 — the ones on the hedge path. Do **not** "clean up" all 30; that's a P2/refactor job.

### 8. No pinning at the root project

**Where:** `pyproject.toml` (root) — `pandas>=2.2`, `numpy>=1.26`, `SQLAlchemy>=1.4`; no lockfile
**Why it matters:** `arbitrage_executor/uv.lock` exists but lists no third-party packages (empty `dependencies`). Root has no lockfile at all. Fresh install of the project on a new machine picks floating latest versions. Playwright updates have historically broken CDP connections; a Pandas 3 release can reshape DataFrame behavior in silent ways.
**Fix:** Declare actual dependencies in `arbitrage_executor/pyproject.toml` (at minimum: `playwright`, `sqlalchemy`, `pandas`, `requests`, `pyyaml`). Lock with `uv lock`. For analytics, prefer `uv` or `pip-tools` to produce a committed lockfile; leave the version ranges in `pyproject.toml` alone.

### 9. `chrome_profile` directory is resolved from CWD

**Where:** `arbitrage_executor/chrome_helpers.py:18` — `profile_dir = os.path.join(os.getcwd(), "chrome_profile")`
**Why it matters:** If `task_worker.py` is launched from any directory other than `arbitrage_executor/`, Chrome spins up a fresh profile — no sportsbook login, no hard-won plugin state, and bot detection likely trips immediately. This is a failure mode one command-line typo away.
**Fix:** `profile_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chrome_profile")`. Pure path-resolution change; identical runtime semantics once the worker is launched from `arbitrage_executor/`. This is the one exception I'd note inside the frozen file — it's anchoring to the file, not changing any launch flag or subprocess semantics. Still: confirm with the user before touching.

### 10. Airflow Fernet key is empty

**Where:** `airflow/docker-compose.yml:~61` — `AIRFLOW__CORE__FERNET_KEY: ''`
**Why it matters:** Connections and Variables stored in Airflow's metadata DB are stored unencrypted. Anyone with read access to the `airflow` Postgres container reads them in plaintext. Today most real secrets are in `.env` (`DATABASE_URL`, `BG_DISCORD_WEBHOOK_URL`), so blast radius is small — but any new Airflow Connection you configure later will leak.
**Fix:** `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` → store in `.env` → reference as `AIRFLOW__CORE__FERNET_KEY: "${AIRFLOW_FERNET_KEY}"`. Rotate will require re-encrypting existing values; easiest to do before any real Connection is added.

### 11. DataFrame writes auto-create tables

**Where:** Shared `db_connection.py` in `app/shared/python/bountygate/utils/` — `_ensure_table_exists()` called from `insert_data`.
**Why it matters:** A typo in a DAG (wrong table name) births a new empty table silently. Schema drift has no alert. Future "why is this query returning empty?" investigations start ~2 hours late.
**Fix:** Gate `_ensure_table_exists` behind an explicit `allow_create=False` default; require migrations to declare tables; fail loudly if a write target doesn't exist.

---

## P2 — quality of life; fix when you're not firefighting

### 12. `bet_placer.py` is a 1062-line god object

**Where:** `arbitrage_executor/bet_placer.py`
**Why it matters:** Site-specific logic for FanDuel and BetMGM is interleaved: navigation, accordion expansion, selector routing, wager entry, confirm-button polling, screenshot management. Per-site drift is hard to localize. A failed FanDuel hedge can look structurally identical to a failed BetMGM placement in the logs.
**Do not start this refactor until (6) exists.** Once the smoke test is there and passes reliably, split by site: `FanduelPlacer`, `BetMGMPlacer`, shared base handling screenshots/logging. Selector drift becomes a diff in one file. But refactoring placement logic without a smoke test is how you introduce a regression that costs ~$50/day until noticed.

### 13. Screenshot audit trail is unbounded

**Where:** `arbitrage_executor/audit_logs/{timestamp}_{player}_{market}/` created per execution
**Why it matters:** Screenshots on every click × 100 executions/day × months = fills the disk. Loading 40 GB of PNGs when you're trying to triage the one that mattered is friction.
**Fix:** A retention pass in `rescue_stuck_tasks.py` siblings or as a standalone script — delete audit dirs older than 30 days _unless_ they correspond to a `FAILED` task in `bot_execution_queue`. Keep CRITICAL/orphaned audits forever.

### 14. No task runner

**Why it matters:** Onboarding requires tribal knowledge ("activate the venv, then `cd arbitrage_executor && python task_worker.py`, but first run `scripts/migrate.py up`, unless…"). A new machine boot takes 30 minutes.
**Fix:** `Makefile` (or `justfile`) at repo root with: `make doctor`, `make migrate`, `make worker`, `make smoke`, `make compose-up`. Each is two lines. No logic — just composition.

### 15. `tests/` empty, pytest declared

**Where:** `tests/` and root `pyproject.toml`
**Why it matters:** Pure theater today. The two pieces where tests pay for themselves: `opportunity.py` ROI math (`calculate_wagers`, `hours_until_commence`, wager scaling), and the hedge-halt logic in `execute_arb.py` (the `_raise_orphaned` path — a pure function that should reliably produce a Discord-ready action string regardless of missing keys in the opportunity dict).
**Fix:** Seed 5–10 unit tests on those two surfaces. Don't aim for coverage; aim for "a regression here would cost money."

### 16. 2 commits in git history

**Where:** `git log`
**Why it matters:** No ability to `git bisect` anything. If a selector change breaks the bot, you can't walk back to the last known-good state. Nothing to do retroactively.
**Fix:** Going forward: commit per change, commit messages that describe the "why" (one line is fine). No aspiration beyond that.

### 17. `scripts/` has 13 utilities with no cohesion

**Where:** `scripts/`
**Why it matters:** Mix of load-bearing (`migrate.py`) and one-off (`normalize_lines.py`, `seed_aliases_*.py`). Every new contributor has to audit all 13 to decide which to run for a fresh setup.
**Fix:** `scripts/operational/` (migrate, load_aliases, load_market_aliases, dq_checks) vs `scripts/oneoff/` (everything else, `README.md` explaining they're archival). Purely organizational; zero runtime risk.

### 18. No CI / pre-commit / linting / type-check

**Why it matters:** A typo in a DAG module sits in main until the next scheduled run breaks. A DAG-side regression caught by `python -c "import dag_module"` is caught hours earlier than by Airflow.
**Fix (cheapest useful):** A GitHub Actions workflow with three steps: `python -c` imports every DAG, `ruff check`, `python scripts/migrate.py dry-run`. Nothing more. No coverage gates, no style bikeshedding.

---

## What I did not flag (and why)

- **The Chrome launch is "fragile."** It's purposefully fragile in a bot-detection sense — touching it is what breaks it. Memory marks it frozen; I concur.
- **The `task_worker` polling interval is 15s.** Fine for this economics. Not worth a change.
- **The lack of a message queue between analytics and executor.** Direct DB polling is simpler than adding SQS/Kafka and hasn't caused a bug. Don't fix what isn't broken.
- **The `bg_executed_opportunities` hash-based dedup is "homegrown."** It's idempotent via `ON CONFLICT DO NOTHING`. That's the right level of engineering for this.
- **Many scattered `print()` calls and inconsistent logging.** Real, but cosmetic. Would be P3.

---

## Priority order if you fix one thing per week

1. Week 1 — P0 #5 (README honesty) + P0 #1 (stuck RUNNING reconciler). Both under 2 hours. Both eliminate compounding frustration.
2. Week 2 — P0 #2, #3 (credentials out of source). Under 2 hours, ongoing exposure removed.
3. Week 3 — P1 #6 (selector smoke test) + ship a pre-session runbook: "run `make smoke` before you flip the worker on."
4. Week 4 — P0 #4 (balance reconciliation). Biggest behavioral improvement in the bot's risk profile.
5. Week 5+ — P1 #7, #9, #11, then P2 items. By then you'll know which matter.
