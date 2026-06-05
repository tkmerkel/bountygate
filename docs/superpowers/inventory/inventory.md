# DAG Inventory

Grouped by repo, then by classification.

## Repo: bountygate

### keep-rewrite

| dag_id | file | schedule | purpose (short) |
|---|---|---|---|
| bg_analysis_sheets | airflow/dags/bg_analysis_sheets_dag.py | asset-triggered (bg_unified_analysis_complete + bg_unified_arbitrage_complete) | Read-only export of cross-market analysis + arb signals to a Google Sheet for manual review. No bets. |
| bg_arb_pipeline | airflow/dags/bg_arb_pipeline.py | `*/5 * * * *` | Self-contained player-prop arb pipeline: ingest OddsAPI lines, build arb opps, append history; (execution enqueue task to be dropped). |
| bg_arbitrage_player_props | airflow/dags/bg_arbitrage_player_props.py | asset-triggered (odds_player_props_staged) | Compute two-sided player-prop arb signals (base + alt market), append history, Discord alerts; (bot-trigger task to be dropped). |
| bg_arbitrage_sheets | airflow/dags/bg_arbitrage_sheets_dag.py | asset-triggered (bg_arbitrage_player_props_complete) | Read-only mirror of player-prop arb tables to a Google Sheet (4 worksheets). No writes to DB. |
| bg_closing_line | airflow/dags/bg_closing_line.py | `*/5 * * * *` | Capture sharp closing lines, devig to fair probs (fact_closing_line), compute CLV vs logged +EV opps (fact_clv). Read-only analytics. |
| bg_dimensional_model | airflow/dags/bg_dimensional_model.py | asset-triggered (bg_normalization_complete) | Kimball dim/fact loader: upsert dim_event/player/market, append fact_odds_snapshot, prune 14d; emits bg_model_loaded. |
| bg_game_arb_pipeline | airflow/dags/bg_game_arb_pipeline.py | `*/10 * * * *` | Detection-only game-line (h2h/spreads/totals) arb pipeline: ingest, build opps, append history, Discord alerts. No executor. |
| bg_injuries | airflow/dags/bg_injuries.py | `@hourly` | Hourly ESPN injury/availability snapshots into dim_player_status. Read-only enrichment. |
| bg_marts | airflow/dags/bg_marts.py | asset-triggered (bg_methodology_complete) | Build mart_market_consensus, mart_arbitrage, mart_good_bets from the dimensional model; emits bg_good_bets_ready. |
| bg_methodology | airflow/dags/bg_methodology.py | asset-triggered (bg_model_loaded) | Devig fair probs, +EV opportunities (+Kelly), line-movement/RLM signals -> fact_* tables; emits bg_methodology_complete. |
| bg_normalization | airflow/dags/bg_normalization.py | asset-triggered (bg_fetch_complete) | Normalize/dedupe cross-market odds, backfill team/market keys, DQ metrics, refresh bg_unified_lines_normalized_mv. |
| bg_results | airflow/dags/bg_results.py | `*/30 * * * *` | Settle final game scores + player box scores from public feeds into fact_game_result / fact_player_stat_result. |
| bg_unified_analysis | airflow/dags/bg_unified_analysis_dag.py | asset-triggered (bg_normalization_complete) | Cross-market consensus + arb signals (bg_unified_analysis / bg_unified_arbitrage); emits analysis/arbitrage-complete assets. |
| bg_unified | airflow/dags/bg_unified_dag.py | `*/5 * * * *` | Cross-market lines aggregation ETL: fetch OddsAPI + 4 DFS apps, normalize, stage, union/dedupe into bg_unified_lines. |
| bg_weather | airflow/dags/bg_weather.py | `0 */3 * * *` | First-pitch weather enrichment for outdoor MLB games into fact_weather (Open-Meteo). Read-only. |
| cleanup_logs | airflow/dags/cleanup_logs.py | `@daily` | Infra housekeeping: delete Airflow task logs older than 7 days. No DB/market interaction. |
| update_underdog_outlier_analysis | airflow/dags/update_underdog_outlier_analysis.py | `*/15 * * * *` | Underdog vs OddsAPI outlier/edge regression analytics + spread trend; writes ud_* / hd_ud_* analytics tables. |

## Repo: kalshi

### keep-rewrite

| dag_id | file | schedule | purpose (short) |
|---|---|---|---|
| kalshi_arb_explorer | dags/kalshi_arb_explorer_dag.py | `*/10 * * * *` | Paper-only cross-venue + intra-Kalshi arb detection. Writes JSONL run_summary (strategy=arb_explore). Places no orders. |
| kalshi_snapshot | dags/kalshi_snapshot_dag.py | `*/30 * * * *` | Read-only observability: top-of-book Kalshi vs OddsAPI per market, price trajectory for CLV. JSONL market_snapshot. |

### archive

| dag_id | file | schedule | purpose (short) |
|---|---|---|---|
| kalshi_cross_poll_bot | dags/kalshi_cross_poll_dag.py | `*/10 * * * *` | LIVE maker +EV bot (cross-game pollution variant). Places/cancels live Kalshi orders. Execution-only. |
| kalshi_maker_ev_bot | dags/kalshi_maker_ev_dag.py | `*/10 * * * *` | LIVE maker +EV bot. Places/cancels live Kalshi maker limit orders sized by Kelly. Execution-only. |

## Counts

| repo | keep-rewrite | archive | total |
|---|---|---|---|
| bountygate | 17 | 0 | 17 |
| kalshi | 2 | 2 | 4 |
| **all** | **19** | **2** | **21** |
