# Decommission Arb Execution + Inventory & Re-Architecture Blueprint

**Date:** 2026-06-05
**Status:** Design — approved, pending implementation plan
**Scope:** First spec in the pivot from arb-execution to analytics-aggregator. Decommission only, inventory only, blueprint only. **No new pipeline or application code.**

---

## 1. Context

`bountygate` has been an arb-execution project: Airflow analytics → Postgres queue → a local Playwright bot placing paired bets on FanDuel/BetMGM. It is not scalable and the books may limit the accounts regardless. We are pivoting the project to a **read-only prediction-market analytics aggregator** and archiving the execution machinery for a possible future revisit.

This is the first of several specs. It establishes a clean foundation: the execution layer is archived, the existing analytics is catalogued, and a target architecture is written down. The actual rebuild (connectors, Postgres backend, frontend) happens in later specs.

### Locked decisions (from brainstorming)

| Decision | Choice |
|---|---|
| End-state repo topology | **Consolidate into `bountygate`.** Kalshi's keep-worthy code migrates in later; both originals get tagged. |
| Archive method | **Git tag + delete from `main`.** |
| Aggregator product | **All four:** cross-market prices/data, read-only edge/arb signals, historical/backtest analytics, sportsbook odds comparison. → Almost all analytics is *kept-and-rewritten*; only the live-execution layer is archived. |
| This spec's boundary | **Decommission + inventory + blueprint. No new code.** |
| Existing DB data | **`pg_dump` then clean slate** (drop all tables). |
| Inventory method | **Workflow-driven parallel** (structural pre-scan → fan-out agent per DAG → synthesis). |
| Heroku | **Take the web dyno down** (scale `web=0`). Keep the app + Postgres addon attached for the rebuild. |

---

## 2. Deliverables

When this spec is complete:

1. **Executed decommission** — arb execution archived via git tag + deletion in `bountygate`; `kalshi` tagged and frozen; Heroku Postgres dumped then emptied; web dyno scaled to 0; analytics DAGs paused.
2. **Structured DAG inventory** — one record per DAG across both repos, a dependency graph (Mermaid), and a Postgres table catalog.
3. **Keep / Rewrite / Archive manifest** — per-item classification that drives the decommission and seeds the rebuild.
4. **Re-architecture blueprint** — target consolidated structure, Airflow/Postgres best-practice principles, and the ordered list of downstream specs.

Explicitly **out of scope:** writing any new DAG/connector/app code, migrating Kalshi code into bountygate, choosing Postgres extensions, choosing the frontend framework, building the new schema.

---

## 3. Components (isolated units)

Four independently checkable units. Only (c) mutates state, and it runs **after** (a)+(b) so the manifest informs deletions.

| Unit | Purpose | Depends on |
|---|---|---|
| (a) Pre-scan script | Statically parse all DAG files into structured skeletons | the two repos (read-only) |
| (b) Inventory workflow | Fan-out semantic analysis per DAG → inventory + graph + manifest | (a)'s output |
| (c) Decommission runbook | Git tag/delete, DB dump/drop, Heroku, pause DAGs | (b)'s manifest |
| (d) Blueprint authoring | Write the target architecture + downstream spec queue | (b)'s inventory |

---

## 4. Decommission mechanics

**Order: tag → inventory → delete.** Nothing is deleted before the tag exists and the manifest is reviewed.

### 4.1 Tag (both repos)
`git tag arb-execution-final` in **both** `bountygate` and `kalshi` before any deletion. Fully recoverable.

### 4.2 bountygate — delete from `main`
- `arbitrage_executor/` — FanDuel/BetMGM Playwright bot
- `watcher/` — video-review loop
- `toolkit/` — bot-ops scripts (queue inspector, stuck-task rescue)
- `dashboard/` + `app/web/` — bot-run **monitoring** surfaces, superseded by the future site
- Execution-only support: `Procfile`, `.slugignore` entries, and any root scripts that only served the bot/web (resolved precisely from the manifest)

### 4.3 bountygate — keep (paused, not deleted)
- `airflow/dags/` — the `bg_*` analytics DAGs (paused)
- `app/shared/python/bountygate/analytics/` — `devig`/`ev`/`kelly`/`clv`/`consensus`/`signals`
- `db/` — migrations + reference data (retained for reference even though tables are dropped)

### 4.4 kalshi — freeze in place
Tag `arb-execution-final`; **no deletions, no migration in this spec.** Keep-worthy `utils/` (`kalshi_client`, `odds_client`, `risk_gates`, `event_match`, `team_names`) migrate into bountygate in the later connectors spec. Live-trading DAGs (`cross_poll`, `maker_ev`, `arb_explorer`) are classified **archive** in the manifest but physically retired only when kalshi is consolidated.

### 4.5 Database
1. `pg_dump` the full Heroku Postgres; store the dump alongside the `arb-execution-final` tag (e.g. committed to an archive location or saved with the tag notes — exact storage decided in the plan).
2. Drop all tables (clean slate). Kept analytics DAGs stay paused, so nothing repopulates until a rebuild spec defines the new schema.

### 4.6 Heroku
- `heroku ps:scale web=0` — take the web dyno down.
- **Keep** the Heroku app and the Postgres addon attached (the rebuilt site will redeploy here; the DB is reused empty).
- Heroku CLI is installed and authenticated.

---

## 5. Inventory method (Approach A: workflow-driven parallel)

### 5.1 Pre-scan (script)
Statically parse all DAG files (~17 `bg_*` in bountygate + 4 `kalshi_*`) into a skeleton per DAG: `dag_id`, schedule, operators/tasks, table reads/writes (SQL + `to_sql`/hook calls), and imports. Grounds the agents in real symbols rather than guesses.

### 5.2 Fan-out (workflow)
~One agent per DAG. Each reads its DAG plus transitive imports (`utils/`, `app/shared/python/`) and emits a structured record.

**Per-DAG record schema:**

| Field | Meaning |
|---|---|
| `dag_id`, `repo`, `file` | identity |
| `purpose` | one-paragraph what/why |
| `schedule` / `trigger` | cron / dataset / manual |
| `source_connectors` | The Odds API, Kalshi, etc. |
| `reads` / `writes` | Postgres tables touched |
| `upstream` / `downstream` | DAG/table dependencies |
| `perf_notes` | obvious bottlenecks (serial API calls, no pooling, full rescans) |
| `classification` | `keep-rewrite` / `archive` / `merge-into-X` |
| `rationale` | why that classification |

### 5.3 Synthesis
Merge records into:
- **Inventory doc** — all per-DAG records, organized by repo and classification.
- **Dependency graph** — Mermaid diagram of DAG→table and DAG→DAG edges.
- **Table catalog** — every Postgres table, its producer DAG(s), and consumer(s).
- **Keep/Rewrite/Archive manifest** — the actionable list that drives §4 deletions.

Output location: `docs/superpowers/inventory/` in bountygate.

---

## 6. Re-architecture blueprint

A document at `docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`, informed by the inventory, covering:

- **Target consolidated layout** for bountygate — directory-level only (e.g. `connectors/`, `dags/`, `analytics/` lib, `db/`, `web/`). No code.
- **Airflow best-practice principles** to adopt: TaskFlow API, dataset/asset-driven scheduling, idempotent tasks, centralized connections/variables, a real test layer, and performance practices (connection pooling, deferrable operators, sane parallelism/pools).
- **Data-architecture principles:** raw → normalized → marts layering; a *shortlist* of Postgres extensions to evaluate (TimescaleDB, pg_cron, PostgREST, pg_partman) — **final choice deferred to the dedicated Postgres-backend spec.**
- **Downstream spec queue** (ordered hand-off):
  1. Ingestion connectors (GET pollers to prediction marketplaces; migrate Kalshi utils in; add Polymarket et al.)
  2. Postgres backend + extensions
  3. Frontend (React/Next.js) + Heroku redeploy

**Deliberately not in this blueprint:** exact extensions, the frontend framework decision, connector-by-connector design. Each is its own later spec so nothing is prematurely frozen.

---

## 7. Success criteria

- `git tag arb-execution-final` exists in both repos and points at the pre-deletion state.
- `arbitrage_executor/`, `watcher/`, `toolkit/`, `dashboard/`, `app/web/` are gone from bountygate `main`; analytics + shared lib remain.
- A `pg_dump` artifact exists and is recoverable; the Heroku Postgres has no application tables.
- `heroku ps` shows `web` at 0 dynos; the app and Postgres addon still exist.
- All analytics DAGs are paused.
- `docs/superpowers/inventory/` contains the inventory doc, dependency graph, table catalog, and keep/rewrite/archive manifest covering all 21 DAGs.
- The re-architecture blueprint exists and names the three ordered downstream specs.

---

## 8. Risks / notes

- **Recoverability is load-bearing.** The tag and the `pg_dump` are the only things standing between "archived" and "lost." The plan must verify both before any destructive step.
- **Kalshi is frozen, not migrated.** Keeping its `utils/` un-migrated in this spec respects the no-new-code boundary; the connectors spec owns the migration.
- **DAGs go dark during the transition.** Acceptable for a pivot; analytics resumes when rebuild specs define the new schema.
- **The manifest gates deletions.** If the inventory classifies something unexpectedly (e.g. an analytics DAG entangled with execution), resolve it in the manifest review before deleting.
