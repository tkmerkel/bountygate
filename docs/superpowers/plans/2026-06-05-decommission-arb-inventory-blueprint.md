# Decommission Arb Execution + Inventory & Re-Architecture Blueprint — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Archive the arb-execution layer recoverably, inventory every DAG across `bountygate` + `kalshi` via a workflow, and write a target re-architecture blueprint — leaving a clean tree and planning artifacts, with no new pipeline/app code.

**Architecture:** Strict order **tag → inventory → review → blueprint → destructive decommission**. A small TDD'd static pre-scan script feeds a fan-out Workflow that classifies each DAG (keep-rewrite / archive / merge). The resulting manifest gates all deletions. Destructive steps (Heroku Postgres reset, web dyno down, dir deletions) run only after the recovery tag and a server-side DB backup both exist and are verified.

**Tech Stack:** Python 3.12 + `ast`/`re` (pre-scan), the Workflow tool (inventory orchestration), git tags (archive), Heroku CLI `pg:backups`/`pg:reset`/`ps:scale` (DB + dyno), docker compose (local Airflow stack).

**Spec:** `docs/superpowers/specs/2026-06-05-decommission-arb-inventory-blueprint-design.md`

---

## File Structure

| Path | Responsibility | Status |
|---|---|---|
| `scripts/inventory/prescan.py` | Static AST/regex parse of DAG files → JSON skeletons | Create |
| `scripts/inventory/test_prescan.py` | TDD test for the parser | Create |
| `scripts/inventory/inventory_workflow.js` | Fan-out Workflow: per-DAG record → synthesis | Create |
| `docs/superpowers/inventory/prescan.json` | Pre-scan output (skeletons for 21 DAGs) | Generate |
| `docs/superpowers/inventory/inventory.md` | Per-DAG records grouped by repo/classification | Generate |
| `docs/superpowers/inventory/dependency-graph.md` | Mermaid DAG→table / DAG→DAG graph | Generate |
| `docs/superpowers/inventory/table-catalog.md` | Table → producer/consumer catalog | Generate |
| `docs/superpowers/inventory/keep-archive-manifest.md` | KEEP/REWRITE/ARCHIVE checklist (gates deletions) | Generate |
| `docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md` | Target architecture + downstream spec queue | Create |
| `README.md`, `Makefile`, `requirements.txt`, `Procfile` | Trim to the reduced post-decommission surface | Modify |

Deletions (Phase: destructive): `arbitrage_executor/`, `watcher/`, `toolkit/`, `dashboard/`, `app/web/`.

---

## Task 1: Create recovery tags + freeze Kalshi

**Files:** none (git operations in both repos).

- [ ] **Step 1: Tag bountygate at its current pre-pivot state**

```bash
cd /c/Users/tkmer/bountygate
git tag -a arb-execution-final -m "Final arb-execution state before analytics-aggregator pivot"
```

- [ ] **Step 2: Tag kalshi at its current pre-pivot state**

```bash
cd /c/Users/tkmer/kalshi
git tag -a arb-execution-final -m "Final arb-execution state; repo frozen pending consolidation into bountygate"
```

- [ ] **Step 3: Push tags if a remote exists (otherwise local tag is the archive)**

```bash
cd /c/Users/tkmer/bountygate && (git remote | grep -q . && git push origin arb-execution-final || echo "no remote: local tag only")
cd /c/Users/tkmer/kalshi    && (git remote | grep -q . && git push origin arb-execution-final || echo "no remote: local tag only")
```

- [ ] **Step 4: Verify both tags resolve to a commit**

```bash
git -C /c/Users/tkmer/bountygate rev-parse arb-execution-final
git -C /c/Users/tkmer/kalshi    rev-parse arb-execution-final
```
Expected: each prints a 40-char SHA (no "unknown revision" error).

- [ ] **Step 5: Add a FROZEN banner to the top of kalshi/README — but kalshi has no README; create a FROZEN.md instead**

Create `/c/Users/tkmer/kalshi/FROZEN.md`:

```markdown
# FROZEN — 2026-06-05

This repo is frozen at tag `arb-execution-final`. The project pivoted to a
read-only analytics aggregator consolidated into `bountygate`. Keep-worthy
code here (`dags/utils/kalshi_client.py`, `odds_client.py`, `risk_gates.py`,
`event_match.py`, `team_names.py`) migrates into bountygate in the later
**connectors** spec. Live-trading DAGs (`cross_poll`, `maker_ev`, `arb_explorer`)
are retired. Do not build new work here.
```

- [ ] **Step 6: Commit the freeze marker in kalshi**

```bash
cd /c/Users/tkmer/kalshi && git add FROZEN.md && git commit -m "chore: freeze repo at arb-execution-final (pivot to bountygate aggregator)"
```

---

## Task 2: Pre-scan parser (TDD)

**Files:**
- Create: `scripts/inventory/prescan.py`
- Test: `scripts/inventory/test_prescan.py`

- [ ] **Step 1: Write the failing test**

Create `scripts/inventory/test_prescan.py`:

```python
import textwrap
from pathlib import Path

from prescan import scan_file


def test_scan_file_extracts_skeleton(tmp_path: Path):
    dag = tmp_path / "sample_dag.py"
    dag.write_text(
        textwrap.dedent(
            '''
            from airflow import DAG
            from utils.kalshi_client import KalshiClient

            with DAG(dag_id="sample_dag", schedule="@hourly") as dag:
                sql = "INSERT INTO bg_results SELECT * FROM bg_arbitrage_opportunities"
            '''
        ),
        encoding="utf-8",
    )

    rec = scan_file(dag, repo="bountygate")

    assert rec["dag_id"] == "sample_dag"
    assert rec["schedule"] == "@hourly"
    assert "utils.kalshi_client" in rec["imports"]
    assert "bg_results" in rec["tables"]
    assert "bg_arbitrage_opportunities" in rec["tables"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/Users/tkmer/bountygate/scripts/inventory && python -m pytest test_prescan.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'prescan'`.

- [ ] **Step 3: Write the parser**

Create `scripts/inventory/prescan.py`:

```python
#!/usr/bin/env python3
"""Static pre-scan of Airflow DAG files into structured skeletons.

Grounds the inventory workflow's per-DAG agents in real symbols (dag_id,
schedule, imports, referenced tables) so they classify from facts, not guesses.
"""
from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

# Table references inside SQL string literals.
_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)",
    re.IGNORECASE,
)
# pandas .to_sql("table", ...)
_TOSQL_RE = re.compile(r"to_sql\(\s*[\"']([a-zA-Z_][a-zA-Z0-9_]*)[\"']")


def _literal_strings(tree: ast.AST) -> list[str]:
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]


def _find_dag_id(tree: ast.AST) -> str | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "id", getattr(func, "attr", ""))
            if name in {"DAG", "dag"}:
                for kw in node.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        return str(kw.value.value)
                if node.args and isinstance(node.args[0], ast.Constant):
                    return str(node.args[0].value)
    return None


def _find_schedule(tree: ast.AST) -> str | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg in {"schedule", "schedule_interval"} and isinstance(
                    kw.value, ast.Constant
                ):
                    return str(kw.value.value)
    return None


def _find_imports(tree: ast.AST) -> list[str]:
    mods: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                mods.add(a.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.add(node.module)
    return sorted(mods)


def _find_tables(strings: list[str]) -> list[str]:
    tables: set[str] = set()
    for s in strings:
        for m in _TABLE_RE.findall(s):
            tables.add(m.split(".")[-1])
        for m in _TOSQL_RE.findall(s):
            tables.add(m)
    return sorted(tables)


def scan_file(path: Path, repo: str) -> dict:
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    return {
        "file": str(path),
        "repo": repo,
        "dag_id": _find_dag_id(tree),
        "schedule": _find_schedule(tree),
        "imports": _find_imports(tree),
        "tables": _find_tables(_literal_strings(tree)),
    }


def scan_dir(dag_dir: Path, repo: str) -> list[dict]:
    records = []
    for path in sorted(Path(dag_dir).glob("*.py")):
        if path.name.startswith("test_") or path.name == "__init__.py":
            continue
        try:
            records.append(scan_file(path, repo))
        except SyntaxError as exc:
            records.append({"file": str(path), "repo": repo, "error": str(exc)})
    return records


def main(argv: list[str]) -> int:
    # argv entries: "<repo_label>=<dag_dir>"
    records: list[dict] = []
    for arg in argv:
        repo, _, dag_dir = arg.partition("=")
        records.extend(scan_dir(Path(dag_dir), repo))
    json.dump(records, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /c/Users/tkmer/bountygate/scripts/inventory && python -m pytest test_prescan.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add scripts/inventory/prescan.py scripts/inventory/test_prescan.py
git commit -m "feat(inventory): static DAG pre-scan parser (ast + sql-table regex)"
```

---

## Task 3: Generate the pre-scan skeleton over both repos

**Files:**
- Generate: `docs/superpowers/inventory/prescan.json`

- [ ] **Step 1: Create the inventory output directory**

```bash
mkdir -p /c/Users/tkmer/bountygate/docs/superpowers/inventory
```

- [ ] **Step 2: Run the pre-scan across bountygate + kalshi DAG dirs**

```bash
cd /c/Users/tkmer/bountygate
python scripts/inventory/prescan.py \
  "bountygate=airflow/dags" \
  "kalshi=/c/Users/tkmer/kalshi/dags" \
  > docs/superpowers/inventory/prescan.json
```

- [ ] **Step 3: Verify it captured 21 DAGs (17 bg + 4 kalshi) with dag_ids**

```bash
python -c "import json; r=json.load(open('docs/superpowers/inventory/prescan.json')); print('records:', len(r)); print('with_dag_id:', sum(1 for x in r if x.get('dag_id'))); print('errors:', [x['file'] for x in r if 'error' in x])"
```
Expected: `records: 21`, `with_dag_id` at or near 21, `errors: []`. If `records` ≠ 21, confirm the `kalshi/dags` path and that no DAG file was skipped (utils/ subdir is excluded by `glob("*.py")` — that's intended; utils are imports, not DAGs).

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/inventory/prescan.json
git commit -m "chore(inventory): static pre-scan skeletons for all 21 DAGs"
```

---

## Task 4: Author + run the inventory workflow

**Files:**
- Create: `scripts/inventory/inventory_workflow.js`
- Generate: `docs/superpowers/inventory/inventory.md`, `dependency-graph.md`, `table-catalog.md`, `keep-archive-manifest.md`

- [ ] **Step 1: Write the workflow script**

Create `scripts/inventory/inventory_workflow.js`:

```javascript
export const meta = {
  name: 'dag-inventory',
  description: 'Inventory all DAGs across bountygate + kalshi into records, graph, table catalog, and keep/archive manifest',
  phases: [
    { title: 'Analyze', detail: 'one agent per DAG -> structured record' },
    { title: 'Synthesize', detail: 'merge into inventory, graph, catalog, manifest' },
  ],
}

const RECORD_SCHEMA = {
  type: 'object',
  required: ['dag_id', 'repo', 'file', 'purpose', 'classification', 'rationale'],
  properties: {
    dag_id: { type: 'string' },
    repo: { type: 'string' },
    file: { type: 'string' },
    purpose: { type: 'string' },
    schedule: { type: 'string' },
    source_connectors: { type: 'array', items: { type: 'string' } },
    reads: { type: 'array', items: { type: 'string' } },
    writes: { type: 'array', items: { type: 'string' } },
    upstream: { type: 'array', items: { type: 'string' } },
    downstream: { type: 'array', items: { type: 'string' } },
    perf_notes: { type: 'string' },
    classification: { type: 'string', enum: ['keep-rewrite', 'archive', 'merge'] },
    rationale: { type: 'string' },
  },
}

const skeletons = args // parsed contents of prescan.json, passed verbatim

phase('Analyze')
const records = await parallel(skeletons.map((s) => () =>
  agent(
    'You are inventorying ONE Airflow DAG for a pivot from arb-execution to a read-only ' +
    'prediction-market analytics aggregator.\n' +
    'KEEP product = cross-market data, read-only edge/arb signals, historical/backtest ' +
    'analytics, sportsbook odds comparison.\n' +
    'ARCHIVE = anything that places/cancels live bets or exists only to feed the execution bot.\n' +
    'MERGE = redundant with another DAG and should fold into it.\n\n' +
    'Read this DAG file AND its local imports (look under utils/, app/shared/python/):\n' +
    '  file: ' + s.file + '\n  repo: ' + s.repo + '\n' +
    'Static pre-scan (may be incomplete): ' + JSON.stringify(s) + '\n\n' +
    'Return the structured record. Be concrete about reads/writes (Postgres tables) and ' +
    'source_connectors. classification must be one of keep-rewrite | archive | merge.',
    { label: 'dag:' + (s.dag_id || s.file), phase: 'Analyze', schema: RECORD_SCHEMA }
  )
))
const clean = records.filter(Boolean)
log('analyzed ' + clean.length + '/' + skeletons.length + ' DAGs')

phase('Synthesize')
const DOC_SCHEMA = {
  type: 'object',
  required: ['inventory_md', 'graph_md', 'table_catalog_md', 'manifest_md'],
  properties: {
    inventory_md: { type: 'string' },
    graph_md: { type: 'string' },
    table_catalog_md: { type: 'string' },
    manifest_md: { type: 'string' },
  },
}
const docs = await agent(
  'Synthesize these ' + clean.length + ' DAG inventory records into four markdown documents.\n' +
  'RECORDS:\n' + JSON.stringify(clean, null, 2) + '\n\n' +
  '1) inventory_md: all records grouped by repo then classification, as readable tables.\n' +
  '2) graph_md: a Mermaid flowchart of DAG->table (reads/writes) and DAG->DAG (upstream/downstream) edges.\n' +
  '3) table_catalog_md: every Postgres table with its producer DAG(s) and consumer DAG(s).\n' +
  '4) manifest_md: a KEEP / REWRITE / ARCHIVE checklist with explicit file paths and dag_ids per ' +
  'bucket, so a decommission step can act on the ARCHIVE bucket directly.',
  { label: 'synthesize', phase: 'Synthesize', schema: DOC_SCHEMA }
)

return { count: clean.length, records: clean, docs }
```

- [ ] **Step 2: Run the workflow (via the Workflow tool, not bash)**

Read `docs/superpowers/inventory/prescan.json`, parse it to a JSON array, then invoke:

`Workflow({ scriptPath: "C:\\Users\\tkmer\\bountygate\\scripts\\inventory\\inventory_workflow.js", args: <parsed prescan.json array> })`

Wait for the completion notification. The result object is `{ count, records, docs: { inventory_md, graph_md, table_catalog_md, manifest_md } }`.

- [ ] **Step 3: Verify coverage before writing files**

Confirm `count === 21` (every DAG produced a record). If fewer, note which `dag_id`s are missing from `records` vs. `prescan.json` and re-run the workflow (it is idempotent; resume with `resumeFromRunId` to reuse cached agents).

- [ ] **Step 4: Write the four markdown artifacts from the workflow return**

Write each `docs.*_md` string to its file:
- `docs.inventory_md` → `docs/superpowers/inventory/inventory.md`
- `docs.graph_md` → `docs/superpowers/inventory/dependency-graph.md`
- `docs.table_catalog_md` → `docs/superpowers/inventory/table-catalog.md`
- `docs.manifest_md` → `docs/superpowers/inventory/keep-archive-manifest.md`

- [ ] **Step 5: Sanity-check the manifest names real paths**

```bash
grep -E "arbitrage_executor|watcher|toolkit" /c/Users/tkmer/bountygate/docs/superpowers/inventory/keep-archive-manifest.md
```
Expected: the execution dirs appear under ARCHIVE. If `app/web` or `dashboard` are absent, that is fine (they are non-DAG surfaces handled directly in Task 8) — but the analytics `bg_*` DAGs should appear under KEEP/REWRITE, not ARCHIVE.

- [ ] **Step 6: Commit the workflow + artifacts**

```bash
cd /c/Users/tkmer/bountygate
git add scripts/inventory/inventory_workflow.js docs/superpowers/inventory/inventory.md docs/superpowers/inventory/dependency-graph.md docs/superpowers/inventory/table-catalog.md docs/superpowers/inventory/keep-archive-manifest.md
git commit -m "docs(inventory): workflow-driven DAG inventory, graph, table catalog, keep/archive manifest"
```

---

## Task 5: Manifest review gate (checkpoint — STOP)

**Files:** possibly `docs/superpowers/inventory/keep-archive-manifest.md` (manual edits).

- [ ] **Step 1: Present the manifest to the user for sign-off**

Show the KEEP / REWRITE / ARCHIVE buckets. Explicitly confirm:
- ARCHIVE includes exactly: `arbitrage_executor/`, `watcher/`, `toolkit/`, `dashboard/`, `app/web/`, and the kalshi live-trading DAGs (`cross_poll`, `maker_ev`, `arb_explorer`).
- No `bg_*` analytics DAG and no `app/shared/python/bountygate/analytics/` module is in ARCHIVE.
- Nothing unexpected (e.g., an analytics DAG entangled with execution) is misfiled.

- [ ] **Step 2: Reconcile any disagreement**

If the user moves an item between buckets, edit `keep-archive-manifest.md` to match and commit:

```bash
cd /c/Users/tkmer/bountygate && git add docs/superpowers/inventory/keep-archive-manifest.md && git commit -m "docs(inventory): reconcile manifest after review"
```

- [ ] **Step 3: Do NOT proceed to destructive tasks until the user approves the ARCHIVE bucket.** This is the gate for Tasks 7–11.

---

## Task 6: Write the re-architecture blueprint

**Files:**
- Create: `docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`

- [ ] **Step 1: Author the blueprint**

Create `docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md` with these sections (fill the bracketed parts from the inventory):

```markdown
# Target Architecture Blueprint — Analytics Aggregator

**Date:** 2026-06-05
**Derived from:** docs/superpowers/inventory/ (inventory, graph, table-catalog, manifest)
**Status:** Blueprint — frames the downstream specs. No code.

## 1. Target consolidated layout (bountygate)

    connectors/   # GET pollers per marketplace (Kalshi migrated in, + Polymarket, etc.)
    dags/         # Airflow 3 TaskFlow DAGs, dataset/asset-scheduled
    analytics/    # the kept shared lib (devig/ev/kelly/clv/consensus/signals)
    db/           # migrations + schema for the new raw->normalized->marts model
    web/          # rebuilt frontend (later spec)

## 2. Airflow best-practice principles to adopt
- TaskFlow API; dataset/asset-driven scheduling over cron where producers feed consumers.
- Idempotent tasks (safe re-run); deterministic partitioning by date/event.
- Centralized Connections/Variables (no inline secrets).
- Real test layer (pure-function unit tests + DAG-import tests).
- Performance: connection pooling, deferrable operators for polling, explicit pools/parallelism.

## 3. Data-architecture principles
- Layering: raw ingest -> normalized -> marts.
- Extensions to EVALUATE (final choice = Postgres-backend spec): TimescaleDB (time-series
  prices), pg_cron (in-db scheduling), PostgREST (auto REST), pg_partman (partition mgmt).

## 4. Keep / Rewrite / Archive summary
[Paste the high-level bucket counts and the per-DAG keep-rewrite targets from
 docs/superpowers/inventory/keep-archive-manifest.md.]

## 5. Downstream spec queue (ordered)
1. **Ingestion connectors** — migrate kalshi utils into connectors/, add Polymarket et al.,
   read-only GET pollers writing to the raw layer.
2. **Postgres backend + extensions** — choose/enable extensions, define raw->normalized->marts
   schema, API surface (PostgREST vs thin app server).
3. **Frontend (React/Next.js) + Heroku redeploy** — rebuild web/, redeploy to the bountygate
   Heroku app, bring the web dyno back up.

## 6. Explicitly deferred
Exact extension choices, the frontend framework decision, and connector-by-connector
design each belong to their own spec — not frozen here.
```

- [ ] **Step 2: Commit**

```bash
cd /c/Users/tkmer/bountygate
git add docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md
git commit -m "docs(spec): target architecture blueprint + downstream spec queue"
```

---

## Task 7: Back up + empty the Heroku Postgres (DESTRUCTIVE — gated on Task 1 tag + Task 5 approval)

**Files:** none (Heroku operations). Local dump copy stored outside the repo at `C:\Users\tkmer\_archive\`.

- [ ] **Step 1: Confirm the recovery tag exists before touching data**

```bash
git -C /c/Users/tkmer/bountygate rev-parse arb-execution-final && echo "tag OK"
```
Expected: a SHA + `tag OK`. If this fails, STOP and redo Task 1.

- [ ] **Step 2: Capture a server-side backup of the Heroku Postgres**

```bash
heroku pg:backups:capture -a bountygate
```
Expected: `Backup completed` with a backup id like `b001`.

- [ ] **Step 3: Verify the backup is listed and download a local copy to the archive**

```bash
mkdir -p /c/Users/tkmer/_archive
heroku pg:backups -a bountygate
heroku pg:backups:download -a bountygate -o /c/Users/tkmer/_archive/bountygate-pre-pivot.dump
ls -la /c/Users/tkmer/_archive/bountygate-pre-pivot.dump
```
Expected: a backup row exists AND the `.dump` file is present with non-zero size. Recoverability now exists in two places (Heroku-retained backup + local dump). **Do not proceed if either is missing.**

- [ ] **Step 4: Reset the database to empty (clean slate)**

```bash
heroku pg:reset DATABASE_URL -a bountygate --confirm bountygate
```
Expected: `Resetting DATABASE_URL on ⬢ bountygate... done`.

- [ ] **Step 5: Verify the database has no application tables**

```bash
heroku pg:psql -a bountygate -c "\dt"
```
Expected: `Did not find any relations.`

---

## Task 8: Take the Heroku web dyno down

**Files:** none (Heroku operation).

- [ ] **Step 1: Scale the web dyno to zero**

```bash
heroku ps:scale web=0 -a bountygate
```
Expected: `Scaling dynos... done, now running web at 0:Basic`.

- [ ] **Step 2: Verify no dynos are running and the app + Postgres addon still exist**

```bash
heroku ps -a bountygate
heroku addons -a bountygate | grep -i postgres
```
Expected: `ps` shows no running `web` dyno; the Postgres addon is still listed (app and DB are retained for the rebuild).

---

## Task 9: Stop the local Airflow stack (analytics goes dark)

**Files:** none (docker compose operation).

- [ ] **Step 1: Bring the local Airflow stack down (no scheduler = no DAG runs)**

```bash
cd /c/Users/tkmer/bountygate/airflow
docker compose ps
docker compose down
```
Expected: `docker compose down` removes the containers. (If the stack was already down, `docker compose ps` shows nothing and `down` is a no-op — fine; with no scheduler running, no DAG executes.) Note: stopping the stack is the operational equivalent of "DAGs paused" in the spec — the kept `bg_*` DAG files stay in `airflow/dags/` but run nowhere. The rewrite specs reintroduce them paused-on-creation under the new schema.

- [ ] **Step 2: Verify the stack is stopped**

```bash
cd /c/Users/tkmer/bountygate/airflow && docker compose ps
```
Expected: no running services. Analytics is now dark; the kept `bg_*` DAG files remain in `airflow/dags/` for the rewrite specs but execute nowhere.

---

## Task 10: Delete the arb-execution layer from bountygate `main` (DESTRUCTIVE — gated on Task 5 approval)

**Files:**
- Delete: `arbitrage_executor/`, `watcher/`, `toolkit/`, `dashboard/`, `app/web/`

- [ ] **Step 1: Re-confirm the ARCHIVE bucket matches these exact paths**

```bash
grep -E "arbitrage_executor|watcher|toolkit|dashboard|app/web" /c/Users/tkmer/bountygate/docs/superpowers/inventory/keep-archive-manifest.md
```
Expected: all five appear under ARCHIVE. If the manifest disagrees, STOP and return to Task 5.

- [ ] **Step 2: Remove the execution directories with git**

```bash
cd /c/Users/tkmer/bountygate
git rm -r arbitrage_executor watcher toolkit dashboard app/web
```

- [ ] **Step 3: Verify the kept analytics survived the delete**

```bash
ls app/shared/python/bountygate/analytics/ && ls airflow/dags/ | head
```
Expected: the analytics modules (`devig.py`, `ev.py`, `kelly.py`, `clv.py`, `consensus.py`, `signals.py`) and the `bg_*` DAGs are all still present.

- [ ] **Step 4: Commit the deletion**

```bash
git commit -m "chore(decommission): remove arb-execution layer (recoverable at tag arb-execution-final)

Deletes arbitrage_executor/, watcher/, toolkit/, dashboard/, app/web/.
Analytics DAGs + app/shared analytics lib retained for the aggregator rewrite."
```

---

## Task 11: Trim repo metadata to the reduced surface

**Files:**
- Modify: `Procfile`, `requirements.txt`, `Makefile`, `README.md`

- [ ] **Step 1: Remove the now-dead web/release Procfile (no web app + no Heroku deploy yet)**

```bash
cd /c/Users/tkmer/bountygate && git rm Procfile
```
(The frontend spec adds a fresh `Procfile` when it redeploys.)

- [ ] **Step 2: Empty the web-only requirements.txt to a placeholder**

Overwrite `requirements.txt` with:

```text
# Web-app dependencies removed with app/web during the 2026-06-05 decommission.
# The rebuilt frontend (later spec) will declare its own deps.
```

- [ ] **Step 3: Strip bot/web targets from the Makefile**

Replace the entire `Makefile` with:

```makefile
.PHONY: help migrate compose-up

help:
	@echo "bountygate task runner (analytics-aggregator pivot in progress). Targets:"
	@echo "  make migrate      — apply DB migrations"
	@echo "  make compose-up   — start Airflow stack via docker compose"

migrate:
	python scripts/migrate.py up

compose-up:
	cd airflow && docker compose up --build
```

- [ ] **Step 4: Replace the README with an accurate post-decommission state**

Overwrite `README.md` with:

```markdown
# bountygate

**Pivoting** from arb execution to a read-only prediction-market **analytics aggregator**
(in progress, 2026-06-05). The arb-execution layer was archived at git tag
`arb-execution-final` and removed from `main`.

## Current contents
- `airflow/dags/` — analytics DAGs (`bg_*`), currently paused/dark pending the rewrite.
- `app/shared/python/bountygate/analytics/` — kept analytics lib (devig/ev/kelly/clv/consensus/signals).
- `db/` — SQL migrations + reference data.
- `scripts/inventory/` — DAG pre-scan + inventory workflow.

## Planning artifacts
- Spec: `docs/superpowers/specs/2026-06-05-decommission-arb-inventory-blueprint-design.md`
- Inventory: `docs/superpowers/inventory/`
- Blueprint + downstream spec queue: `docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`

## Recover the archived arb bot
`git checkout arb-execution-final` (full execution layer + kalshi at freeze point).
The Heroku Postgres pre-pivot backup: `heroku pg:backups -a bountygate` (server-side) and
`C:\Users\tkmer\_archive\bountygate-pre-pivot.dump` (local).
```

- [ ] **Step 5: Commit the metadata trim**

```bash
cd /c/Users/tkmer/bountygate
git add Procfile requirements.txt Makefile README.md
git commit -m "chore(decommission): trim Procfile/requirements/Makefile/README to analytics surface"
```

---

## Task 12: Final verification against the spec's success criteria

**Files:** none (verification only).

- [ ] **Step 1: Tags exist in both repos**

```bash
git -C /c/Users/tkmer/bountygate rev-parse arb-execution-final && git -C /c/Users/tkmer/kalshi rev-parse arb-execution-final
```
Expected: two SHAs.

- [ ] **Step 2: Execution layer is gone; analytics remains**

```bash
cd /c/Users/tkmer/bountygate
for d in arbitrage_executor watcher toolkit dashboard app/web; do test ! -e "$d" && echo "removed: $d" || echo "STILL PRESENT: $d"; done
test -d app/shared/python/bountygate/analytics && echo "analytics kept OK"
```
Expected: five `removed:` lines + `analytics kept OK`.

- [ ] **Step 3: Heroku DB empty + backup recoverable + web at 0**

```bash
heroku pg:psql -a bountygate -c "\dt"
heroku pg:backups -a bountygate | head
heroku ps -a bountygate
ls -la /c/Users/tkmer/_archive/bountygate-pre-pivot.dump
```
Expected: `Did not find any relations.`; ≥1 backup row; no running web dyno; local dump present.

- [ ] **Step 4: Local Airflow stack stopped**

```bash
cd /c/Users/tkmer/bountygate/airflow && docker compose ps
```
Expected: no running services.

- [ ] **Step 5: Planning artifacts present**

```bash
cd /c/Users/tkmer/bountygate
ls docs/superpowers/inventory/ && test -f docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md && echo "blueprint OK"
python -c "import json; r=json.load(open('docs/superpowers/inventory/prescan.json')); print('DAGs inventoried:', len(r))"
```
Expected: `inventory.md`, `dependency-graph.md`, `table-catalog.md`, `keep-archive-manifest.md`, `prescan.json` all listed; `blueprint OK`; `DAGs inventoried: 21`.

- [ ] **Step 6: Report completion**

Summarize against the spec's §7 success criteria. All checks green → the decommission + inventory + blueprint spec is complete, and the downstream spec queue (connectors → Postgres backend → frontend) is ready to brainstorm next.
