# Internal dashboard & living wiki — design

**Status:** approved (brainstorm), pending implementation plan
**Date:** 2026-05-16
**Heroku app:** `bountygate` (already cut over from pick6plug)

## Context

The freshly-renamed `bountygate` Heroku app currently serves a single page: a static dashboard reading `dashboard/data.json` (a feed updated by the existing review-watcher Claude Code loop). That works as a starting point, but two things are missing:

1. **Operational visibility** — we want a richer dashboard surfacing latest runs, sportsbook account stats (liquidity, balance, P&L), and the health of the watcher/scraper processes themselves. The existing card-and-table view is a foundation, not the destination.
2. **A living wiki** — a visualization-heavy internal docs surface (diagrams > text) where the first page is a combined decision-tree / execution-map / value-stream-map of the bot. Critically, Claude should keep the wiki up to date automatically as the underlying code changes, using the same skill-invocation pattern that the existing review-watcher uses.

Why now: we just spent a session repurposing the Heroku slot and now have a clean FastAPI base to build on. The watcher pattern is proven on this repo (review.pending → /watch:watch → data.json append) and is the cheapest way to reach the auto-update behavior we want.

## Goals

- Extend the existing dashboard with **account stats** (per book) and **watcher health** (per watcher) cards, without breaking the current runs view.
- Stand up a **wiki** at `/wiki/{slug}`, server-rendered from markdown source files, capable of embedding both Mermaid diagrams (always) and interactive React Flow diagrams (on the pages that earn it).
- Ship the **bot-flow wiki page** as the first concrete example — one diagram with toggleable layers (Execution, Decisions, Value stream, Recent failures), driven by static page structure plus live data from the API.
- **Auto-update**: a git post-commit hook marks affected wiki pages dirty; a Claude Code wiki-watcher session regenerates them by invoking a new `/wiki:sync` skill. User reviews and commits the regenerated `.md` — no auto-commit.
- **Move dashboard runtime state to Postgres** so producers running locally and dashboard running on Heroku share one source of truth. Wiki content stays as git-versioned `.md`.

## Non-goals (v1)

- **Auth** — Heroku-app URL is unguessable; defer until we want to share it. Add HTTP basic auth via one config var when needed.
- **Wiki search, comments, history view, draft mode** — out of scope. Markdown files are searchable via the repo and history lives in git.
- **GitHub Action regen** — explicitly punted. A future GH Action can post a comment listing affected pages, but actual regen happens locally via the watcher (keeps Claude API costs off your GH bill).
- **S3 / external object storage** — Postgres handles the runtime-state need.
- **Real-time pushes / websockets** — dashboard polls; wiki pages render on request.

## Architecture

Every new piece is either (a) a new producer that writes to Postgres, or (b) a new FastAPI route that reads from Postgres or filesystem. No new infrastructure beyond the existing Heroku web dyno + Postgres essential-1 addon + local Claude Code sessions.

```
                            ┌────────── Heroku ──────────┐
   ┌──── Browser ────┐      │                            │
   │                 │◄─────│  FastAPI (app.web.main)    │
   │  dashboard, /,  │      │                            │
   │  /wiki/{slug}   │──────►   ┌──────────────────┐     │
   │                 │      │   │ Postgres         │     │
   └─────────────────┘      │   │ (essential-1)    │     │
                            │   └────▲─────────────┘     │
                            └────────│─────────────────  ┘
                                     │ writes
                                     │
   ┌────────────────────────── Local (your machine) ─────────────────────┐
   │                                                                      │
   │  task_worker.py ──► (existing) bg_executed_opportunities             │
   │  review-watcher ──► (existing) audit_logs/ + dashboard_runs (NEW)    │
   │  account_scraper ─► (NEW)     account_stats, account_stats_history   │
   │  wiki-watcher    ─► (NEW)     wiki/*.md (committed by user)          │
   │  git post-commit ─► (NEW)     wiki/.pending/{slug}                   │
   │                                                                      │
   └──────────────────────────────────────────────────────────────────────┘
```

## Data model

### New Postgres tables

`db/migrations/006_dashboard_state.sql`:

```sql
-- One row per analyzed bot run. Replaces dashboard/data.json over time.
CREATE TABLE dashboard_runs (
    run_id              text PRIMARY KEY,
    occurred_at         timestamptz NOT NULL,
    player              text NOT NULL,
    market              text NOT NULL,
    outcome             text NOT NULL CHECK (outcome IN ('success','failure','skipped')),
    duration_s          numeric,
    issues              jsonb NOT NULL DEFAULT '{}',
    top_finding         text,
    video_url           text,
    review_url          text,
    inserted_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dashboard_runs_occurred_at_idx ON dashboard_runs (occurred_at DESC);

-- Latest snapshot per book. Upserted by account_scraper on each tick.
CREATE TABLE account_stats (
    book                text PRIMARY KEY,
    balance             numeric,
    pending_wagers      numeric,
    available_liquidity numeric,
    pnl_7d              numeric,
    scrape_status       text NOT NULL,
    last_error          text,
    scraped_at          timestamptz NOT NULL
);

-- Append-only history for trend charts.
CREATE TABLE account_stats_history (
    book        text NOT NULL,
    scraped_at  timestamptz NOT NULL,
    balance     numeric,
    pnl_7d      numeric,
    PRIMARY KEY (book, scraped_at)
);

-- One row per watcher process. Upserted on every loop tick + on start/stop.
CREATE TABLE watcher_heartbeats (
    name                  text PRIMARY KEY,
    is_running            boolean NOT NULL,
    last_tick_at          timestamptz NOT NULL,
    pending_count         int NOT NULL DEFAULT 0,
    oldest_pending_age_s  int,
    completed_24h         int NOT NULL DEFAULT 0,
    errors_24h            int NOT NULL DEFAULT 0,
    last_error            text,
    expected_interval_s   int NOT NULL    -- producer-declared; UI uses to compute amber/red
);
```

The existing `bg_*` tables are not touched.

### API endpoints

All four are added to `app/web/main.py`. All read from Postgres (no file I/O in the hot path). All include `updated_at` and (where relevant) `stale_after_minutes` so the client can show a freshness pill.

| Endpoint | Source | Cache | Shape |
|---|---|---|---|
| `GET /api/runs?limit=N` | `dashboard_runs` ordered by `occurred_at DESC` | 30s | `{version, updated_at, runs: [...]}` |
| `GET /api/account-stats` | `account_stats` joined to latest history rows | 30s | `{version, updated_at, books: {fanduel: {...}, betmgm: {...}}}` |
| `GET /api/watchers` | `watcher_heartbeats` + computed `status` (ok/amber/red) | none | `{version, checked_at, watchers: [...]}` |
| `GET /api/wiki/{slug}.json` | computed from Postgres (run counts, durations, failure rates per node) | 60s | `{slug, computed_at, node_metrics: {...}, edge_metrics: {...}}` |

Amber/red status logic for `/api/watchers` lives in FastAPI so the UI never needs threshold constants. Thresholds:
- **Amber**: `pending_count > 0 AND oldest_pending_age_s > 15 * 60`, OR `last_tick_at` older than `2 × expected_interval_s`.
- **Red**: `errors_24h > 0`, OR `last_tick_at` older than `6 × expected_interval_s`, OR `scrape_status != 'ok'` (for the scraper specifically).

### Wiki source format

A wiki page is a single markdown file under `wiki/`:

```markdown
---
title: Bot execution flow
slug: bot-flow
watches:
  - arbitrage_executor/execute_arb.py
  - arbitrage_executor/task_worker.py
  - arbitrage_executor/opportunity.py
updated_at: 2026-05-16T14:32:00Z
generated_by: /wiki:sync
---

# Bot execution flow

…prose…

```mermaid
sequenceDiagram
  …
```

:::reactflow id="bot-decision-graph"
{
  "nodes": [...],
  "edges": [...],
  "layers": [
    { "id": "execution", "label": "Execution", "color": "#4a5568", "default": true },
    { "id": "decisions", "label": "Decisions", "color": "#a78bfa", "default": true },
    { "id": "value_stream", "label": "Value stream", "color": "#f7c873", "default": true },
    { "id": "failures", "label": "Recent failures", "color": "#ff6b6b", "default": false }
  ],
  "data_endpoint": "/api/wiki/bot-flow.json"
}
:::
```

Front-matter is required. `watches:` is the list of source files the page depends on — the git post-commit hook reads this to decide which pages to mark dirty.

## Components

Each component has one clear purpose and a single owner module. Interfaces are file paths / function signatures, not classes.

### 1. `dashboard_renderer` (extend existing `dashboard/index.html`)
**Purpose:** Render the dashboard. Vanilla JS, polls `/api/runs`, `/api/account-stats`, `/api/watchers` every 30s.
**Adds vs. today:** two new cards (account stats per book; watcher health table). Freshness pill on each card derived from the endpoint's `updated_at`.
**Dependencies:** the three API endpoints above. No new libraries.

### 2. `wiki_renderer` (`app/web/wiki.py`, new)
**Purpose:** Pure function: `markdown_text → html`. Uses `markdown` + `pymdown-extensions` for fences; one custom extension parses `:::reactflow` fenced blocks into mount stubs.
**Output contract:** HTML with `<pre class="mermaid">…</pre>` for Mermaid blocks and `<div class="reactflow-mount" data-id="…" data-endpoint="…">…JSON…</div>` for React Flow blocks. Only includes the React Flow `<script>` tag when at least one `:::reactflow` block is present.
**Dependencies:** stdlib + two pip packages added to `requirements.txt` (`markdown>=3.7`, `pymdown-extensions>=10`).

### 3. `wiki_route` (extend `app/web/main.py`)
**Purpose:** `GET /wiki/{slug}` reads `wiki/{slug}.md` from disk, runs `wiki_renderer`, returns HTML wrapped in a minimal page chrome (sidebar listing all wiki pages, freshness pill from front-matter `updated_at`).
**Caching:** 30s TTL keyed by slug + file mtime. Defeats stampedes.
**Plus:** `GET /wiki` (index) lists all pages with last-updated timestamps.

### 4. `post_commit_hook` (`scripts/wiki_hook.py`, installed into `.git/hooks/post-commit` by `scripts/install_wiki_hook.ps1`)
**Purpose:** After every commit, walk `wiki/*.md`, parse front-matter, intersect each page's `watches:` with `git diff --name-only HEAD~1..HEAD`. For each match: `touch wiki/.pending/{slug}`.
**Safety cap:** if a single commit matches > 5 pages (e.g., merge commit), log and require explicit `--force` re-run (avoids fanning out the whole wiki on a merge).
**Cross-platform:** Python script + PowerShell wrapper for Windows / bash wrapper for git-bash.

### 5. `wiki_watcher` (`watcher/wiki/INITIAL_PROMPT.md`, started via `scripts/start_wiki_watcher.ps1`)
**Purpose:** Same loop shape as the existing review-watcher. While `wiki/.pending/*` exists: pick oldest → invoke `/wiki:sync <slug>` skill → on success move to `wiki/.done/{slug}`. Writes a heartbeat row to `watcher_heartbeats` on every tick.
**Idempotent**: re-running `/wiki:sync` on the same slug should produce stable output (idempotent prompt design).

### 6. `/wiki:sync` skill (`.claude/skills/wiki/sync.md`, new)
**Purpose:** Regenerate one wiki page from its `watches:` source files.
**Behavior:**
1. Read the current `wiki/{slug}.md` (preserves intent, current layer set, custom annotations).
2. Read each file in `watches:`.
3. Regenerate the body, updating: prose to match current code reality, Mermaid diagrams to match the current call graph, React Flow `nodes`/`edges` JSON to match current state machines and decision gates.
4. **Granularity bar:** enumerate every meaningful UI interaction (navigate, wait-for-element, dismiss-modal, click-search, type-query, etc.) — do NOT collapse multiple interactions into one node. Diagram readability at high node counts is handled by collapsible super-nodes (Phase 1/2/3 headers act as collapse toggles in the React Flow renderer).
5. Update front-matter `updated_at`.
6. Write the new `.md` (overwrites in place — user reviews diff and commits).

### 7. `account_scraper` (`arbitrage_executor/account_scraper.py`, new)
**Purpose:** Reuses the existing Playwright session/stealth setup (`arbitrage_executor/browser.py` patterns). On a configurable cadence (default: every 60 minutes), opens each book's account page, scrapes balance / pending wagers / settled P&L, upserts `account_stats` + appends to `account_stats_history`.
**Failure mode:** on auth modal / scrape error, writes `scrape_status='error'` + `last_error` to `account_stats`; the dashboard surfaces this in the freshness pill and watcher card. Does not retry within a tick — let the next tick try fresh.
**Schedule:** runs as part of the task_worker loop (every Nth iteration), explicitly **between** bot tasks — never concurrent with an in-flight execution, since both want the same browser. A separate Windows scheduled task is rejected as v1 because it would require a second stealth profile (the scraper must use the warm logged-in session, not a cold one). Implementation plan picks N (likely "every 5 iterations or every 60min, whichever comes first").

### 8. `watcher_heartbeat` shared utility (`app/shared/python/bountygate/watcher_heartbeat.py`, new)
**Purpose:** `def heartbeat(name: str, **fields) -> None` that upserts `watcher_heartbeats`. Used by review-watcher, wiki-watcher, account_scraper.
**Why shared:** so all three watchers report through the same schema and the dashboard can render them uniformly.

## Auto-update flow (sequence)

1. **t=0** — user runs `git commit -m "tweak: BetMGM market tab selector"`.
2. **t+0.1s** — post-commit hook fires. Walks `wiki/*.md`, finds `wiki/bot-flow.md` declares `arbitrage_executor/execute_arb.py` in `watches:`. Touches `wiki/.pending/bot-flow`.
3. **t+~1s** — wiki-watcher's Claude Code session (if running locally) detects the new pending file via its stop-hook, re-invokes the loop.
4. **t+~30s** — `/wiki:sync bot-flow` skill runs: reads `wiki/bot-flow.md`, reads watched sources, regenerates the body, writes the new `.md`, moves `.pending/bot-flow` → `.done/bot-flow`, writes a `watcher_heartbeats` row.
5. **t+minutes** — user sees `wiki/bot-flow.md` as a dirty file, reviews the diff, commits (or amends earlier commit). The regenerated page is now in git, and on next deploy the Heroku app serves the updated content.

If no wiki-watcher session is running, `.pending` files queue harmlessly and drain on the next `start_wiki_watcher.ps1`.

## First wiki page detail — `bot-flow.md`

The first page is the showcase. Concrete layout:

- **Page header:** title + `updated_at` + sync-status badge ("in sync with main" / "drifted N commits").
- **Body:** brief prose intro (~2 sentences), then a Mermaid sequence diagram of the three-phase pipeline (Queue → Worker → FanDuel probe / BetMGM place / FanDuel hedge), then the React Flow decision graph.
- **React Flow diagram:** one combined graph where:
  - **Nodes** = bot states. Grouped by phase. Phase headers (Phase 1 · Probe, Phase 2 · Place, Phase 3 · Hedge) render as collapsible super-nodes — default collapsed for top-level view, expand to reveal granular UI interactions (search, dismiss-modal, click, wait-for-element, etc.).
  - **Edges** = transitions. Decision edges colored by branch (green=yes, red=no).
  - **Layers** = visual overlays toggleable in the right-side legend:
    - *Execution* (on): base step rendering.
    - *Decisions* (on): highlights branching nodes with the gate question label.
    - *Value stream* (on): annotates nodes with avg duration; flags waste (e.g., "21.3s avg, mostly wasted_wait").
    - *Recent failures* (off): red badge on nodes that failed in last 24h.
  - **Interactivity:** hover a node → tooltip with run count / avg duration / last failure timestamp. Click → drill to filtered runs.
- **Right-side panel:** layer toggles + a node-info panel (populated by current hover/click).

## Rendering pipeline

```
GET /wiki/bot-flow
  → wiki_route reads wiki/bot-flow.md
  → wiki_renderer:
      markdown → HTML
      code-fences (lang=mermaid) → <pre class="mermaid">…</pre>
      :::reactflow blocks → <div class="reactflow-mount" data-…>…</div>
  → page chrome adds:
      <script type="module" src="//cdn…/mermaid/…/mermaid.esm.min.mjs">
      (only if any reactflow mount present)
      <script type="module" src="/static/wiki/reactflow-bootstrap.js">
  → returns HTML
```

`reactflow-bootstrap.js` (new, ~100 lines) loads React + React Flow from CDN UMD, finds every `.reactflow-mount`, reads its `data-id` and `data-endpoint`, fetches the endpoint, and renders the diagram with layer-toggle behavior.

## Auth posture

None in v1. Heroku URL is unguessable enough for an internal tool. Implementation plan should add a one-line knob (`if os.environ.get("DASHBOARD_AUTH"): app.add_middleware(BasicAuthMiddleware, …)`) so we can enable it via config var when we want to share the URL.

## Deployment

No new build step. No slug-size change beyond the two new pip packages (~few hundred KB). Procfile is unchanged. `release: python scripts/migrate.py up` already runs migrations on every deploy, so the new tables apply automatically.

Local-only producers (account_scraper, watchers) connect to the same Postgres via `DATABASE_URL` from the existing `.env`. The git post-commit hook is installed once per local clone via `scripts/install_wiki_hook.ps1`.

## Testing

Pure-function unit tests only. The existing repo convention (hot path tested through real Playwright runs, pure modules tested via pytest) is respected.

- `tests/unit/test_wiki_hook.py` — front-matter parse + diff-intersection logic.
- `tests/unit/test_wiki_renderer.py` — markdown→HTML with each special block type (Mermaid, reactflow), empty body, missing front-matter.
- `tests/unit/test_watcher_status.py` — amber/red threshold computation for various input combinations.
- `tests/unit/test_postgres_url_rewrite.py` — already covered for the web app; add for any new producer connecting to DB.

Smoke check on deploy: `/health` already verifies DB connection. Extend to also assert the new tables exist (one `SELECT 1 FROM dashboard_runs LIMIT 0` per new table).

## Critical files (to create or modify)

**Create:**
- `db/migrations/006_dashboard_state.sql`
- `scripts/backfill_dashboard_runs.py` (one-time: data.json → dashboard_runs)
- `wiki/bot-flow.md`
- `app/web/wiki.py` (renderer + custom extensions)
- `dashboard/static/wiki/reactflow-bootstrap.js`
- `scripts/wiki_hook.py`
- `scripts/install_wiki_hook.ps1`
- `scripts/start_wiki_watcher.ps1`
- `watcher/wiki/INITIAL_PROMPT.md`
- `watcher/wiki/stop_hook.ps1`
- `.claude/skills/wiki/sync.md` (the `/wiki:sync` skill)
- `arbitrage_executor/account_scraper.py`
- `app/shared/python/bountygate/watcher_heartbeat.py`
- `tests/unit/test_wiki_hook.py`, `test_wiki_renderer.py`, `test_watcher_status.py`

**Modify:**
- `app/web/main.py` — add `/wiki/{slug}`, `/wiki`, `/api/runs`, `/api/account-stats`, `/api/watchers`, `/api/wiki/{slug}.json`, freshness extension on `/health`.
- `dashboard/index.html` — add account-stats card, watcher-health card, freshness pills.
- `requirements.txt` — add `markdown>=3.7`, `pymdown-extensions>=10`.
- `watcher/` existing review-watcher loop — (a) call `watcher_heartbeat.heartbeat("review-watcher", ...)` on every tick so it shows up in `/api/watchers` consistently, and (b) **switch from appending to `dashboard/data.json` to inserting into the `dashboard_runs` Postgres table**. One-time backfill script (`scripts/backfill_dashboard_runs.py`, also new) reads the current `dashboard/data.json` and inserts each entry; after that the file is no longer the source of truth. Leave the file in git as a frozen snapshot for one release cycle, then delete.
- `README.md` — add "Internal dashboard" and "Wiki" sections under "Quick start".

## Reused existing patterns

- **Watcher loop shape** — INITIAL_PROMPT + stop_hook + start script — copied from `watcher/` (the existing review-watcher), adapted for `wiki/.pending` instead of `audit_logs/*/review.pending`.
- **Migration runner** — `scripts/migrate.py` already handles new `db/migrations/NNN_*.sql` files idempotently.
- **`postgres://` rewrite** — `app/web/main.py` and `scripts/migrate.py` already do this; producers must do the same when reading `DATABASE_URL`.
- **Skill-invocation pattern** — `/wiki:sync` is a new skill in `.claude/skills/wiki/`, invoked by the wiki-watcher session same way `/watch:watch` is by review-watcher.

## Risks / open questions

1. **CDN dependency** for Mermaid + React Flow — works fine for an internal tool. If we later want offline support, vendor the JS bundles into `dashboard/static/vendor/`.
2. **Wiki growing past hand-curated markdown** — if we get past ~10 pages with rich interactivity, migrate to Astro per the brainstorm (Approach 2 we considered and rejected for v1). Migration is mechanical: `.md` files become `.mdx`, the renderer is replaced.
3. **`reactflow-bootstrap.js` complexity** — UMD-loaded React isn't fun. Capped at ~100 lines; if it grows beyond that, that's the signal to migrate to Astro.
4. **Account scraper scheduling** — running as part of `task_worker.py`'s loop ties scraper health to worker health. Standalone scheduled task is cleaner but adds setup. Implementation plan should pick one.
5. **`scripts/migrate.py` non-trivial dependency** — already addressed in cutover (guarded import), keep watching for similar issues if other producers grow shared-lib dependencies that aren't on Heroku.
