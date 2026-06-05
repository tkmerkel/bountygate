# Target Architecture Blueprint — Analytics Aggregator

**Date:** 2026-06-05
**Derived from:** `docs/superpowers/inventory/` (inventory.md, dependency-graph.md, table-catalog.md, keep-archive-manifest.md)
**Status:** Blueprint — frames the downstream specs. No code.

This blueprint turns the DAG inventory into a target shape and an ordered backlog. It deliberately stops at principles + structure; concrete technology choices belong to the dedicated specs named in §5.

## 1. Target consolidated layout (bountygate)

```
connectors/   # read-only GET pollers per marketplace; Kalshi utils migrated in (kalshi_client,
              #   odds_client, event_match, team_names), + Polymarket and other prediction venues
dags/         # Airflow 3 TaskFlow DAGs, dataset/asset-scheduled (the 19 keep-rewrite DAGs land here)
analytics/    # the kept shared lib (devig / ev / kelly / clv / consensus / signals)
db/           # migrations + schema for the new raw -> normalized -> marts model
web/          # rebuilt frontend (later spec); redeploys to the bountygate Heroku app
```

Kalshi's repo is frozen at `arb-execution-final`; its keep-worthy `utils/` migrate into `connectors/`
in the connectors spec. The 2 archived live-order DAGs (`kalshi_maker_ev_bot`, `kalshi_cross_poll_bot`)
are retired, not migrated. (`utils.trading_logic.find_plus_ev_trades` may be salvaged as a read-only
signal if wanted — flagged in the manifest.)

## 2. Airflow best-practice principles to adopt

- **TaskFlow API** over classic operators; **dataset/asset-driven scheduling** where producers feed
  consumers (the inventory already shows asset-trigger lineage, e.g. `bg_unified_analysis` →
  `bg_analysis_sheets` — generalize this).
- **Idempotent tasks** (safe re-run); deterministic partitioning by date/event. Replace the current
  `TRUNCATE`/`if_exists=replace` full-table swaps (flagged in `bg_unified_analysis`,
  `update_underdog_outlier_analysis`) with append + dedup-on-hash.
- **Centralized Connections/Variables** — no inline secrets. The inventory found hardcoded
  OddsAPI keys in `bg_unified`, `bg_game_arb_pipeline`, `update_underdog_outlier_analysis`; these
  move to Airflow Variables/Connections.
- **Real test layer** — pure-function unit tests (the `analytics/` lib is already test-friendly) +
  DAG-import smoke tests.
- **Performance** — connection pooling/shared engines (already partially done via
  `bountygate.utils.db_connection`), Postgres `COPY` bulk loads (already used in `bg_arb_pipeline`),
  deferrable operators for polling, explicit pools/parallelism, and `statement_timeout`/keepalives.

## 3. Data-architecture principles

- **Layering:** raw ingest → normalized → marts. The inventory's table catalog already implies this
  (`bg_arb_stage_lines`/`bg_unified_lines` raw → `*_normalized` → `dim_*`/`fact_*` → `mart_*`);
  formalize it.
- **Remove execution-era tables:** `bot_execution_queue`, `bg_executed_opportunities` and the
  `enqueue`/`trigger_bot_execution` couplings (per the manifest) are dropped during the rewrite.
- **Extensions to EVALUATE** (final choice = Postgres-backend spec): TimescaleDB (time-series odds/
  price history), pg_cron (in-db scheduling), PostgREST (auto REST surface for the frontend),
  pg_partman (partition management for the snapshot/history tables).

## 4. Keep / Rewrite / Archive summary (from the inventory)

| Bucket | Count | Notes |
|---|---|---|
| KEEP (as-is) | 0 | every retained DAG needs cleanup |
| REWRITE | 19 | 17 `bg_*` + `kalshi_arb_explorer` + `kalshi_snapshot` |
| ARCHIVE | 2 | `kalshi_maker_ev_bot`, `kalshi_cross_poll_bot` (live order placement) |

Non-DAG execution surfaces archived directly (this spec): `arbitrage_executor/`, `watcher/`,
`toolkit/`, `dashboard/`, `app/web/`. Full per-DAG rewrite items: `keep-archive-manifest.md`.

## 5. Downstream spec queue (ordered)

1. **Ingestion connectors** — migrate Kalshi `utils/` into `connectors/`; add Polymarket et al.;
   read-only GET pollers writing to the raw layer. Strip the `bot_execution_queue`/`trigger_bot`
   couplings from `bg_arb_pipeline` and `bg_arbitrage_player_props` as part of this.
2. **Postgres backend + extensions** — choose/enable extensions (§3 shortlist), define the
   raw→normalized→marts schema and migrations, and the API surface (PostgREST vs thin app server).
3. **Frontend (React/Next.js) + Heroku redeploy** — rebuild `web/`, redeploy to the bountygate
   Heroku app, bring the web dyno back up.

Each gets its own brainstorm → spec → plan → build cycle.

## 6. Explicitly deferred

Exact extension choices, the frontend framework decision, and connector-by-connector design each
belong to their own spec — not frozen here.
