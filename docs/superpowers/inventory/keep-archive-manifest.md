# Decommission / Pivot Manifest

Disposition buckets for the read-only analytics aggregator pivot. The ARCHIVE bucket is directly actionable for a decommission step.

## KEEP (keep-as-is)

None. Every retained DAG is classified `keep-rewrite` (see REWRITE bucket). No DAG carries over unchanged.

## REWRITE (keep-rewrite — retain, but clean up before production)

All paths are repo-relative unless an absolute path is shown.

### Repo: bountygate (root: `C:/Users/tkmer/bountygate`)

| dag_id | file | key rewrite items |
|---|---|---|
| bg_analysis_sheets | airflow/dags/bg_analysis_sheets_dag.py | Re-point Google Sheets sink to product read API/data layer; formalize ad-hoc filters (test_score_1, price<=2.5, magic risk thresholds). |
| bg_arb_pipeline | airflow/dags/bg_arb_pipeline.py | Drop enqueue_opportunities_task + bot_execution_queue insert + bg_executed_opportunities read + enqueue-gate constants. Keep ingest/builder/history. |
| bg_arbitrage_player_props | airflow/dags/bg_arbitrage_player_props.py | Strip trigger_bot_execution + bot_execution_queue insert + high-value gating + MARKET_BLACKLIST; index opportunity_key. |
| bg_arbitrage_sheets | airflow/dags/bg_arbitrage_sheets_dag.py | Relabel/drop bankroll/execution columns (wager_under/over, payout); reframe as historical/backtest analytics. |
| bg_closing_line | airflow/dags/bg_closing_line.py | Re-home bg_arb_pipeline_lib.db helper; reframe fact_ev_opportunity as tracked analytics signals (not EV bets). |
| bg_dimensional_model | airflow/dags/bg_dimensional_model.py | Drop bg_arb_pipeline_lib import + bankroll/soft_legal_books helpers; batch dim upserts. |
| bg_game_arb_pipeline | airflow/dags/bg_game_arb_pipeline.py | Scrub executor framing; move hardcoded OddsAPI key to secret; generalize Discord notifier. |
| bg_injuries | airflow/dags/bg_injuries.py | Re-home bg_arb_pipeline_lib.db / db_connection off arb naming. |
| bg_marts | airflow/dags/bg_marts.py | Drop arb-lib import lineage; share consensus/arbitrage transforms via common module (remove copy-paste drift). |
| bg_methodology | airflow/dags/bg_methodology.py | Drop live-bankroll Kelly stake sizing (get_bankroll/account_stats); reframe soft_legal_books as comparison filter. |
| bg_normalization | airflow/dags/bg_normalization.py | Move ad-hoc DDL/CSV image paths into managed migrations; share engine/pooling. |
| bg_results | airflow/dags/bg_results.py | Relocate/rename bulk_append_new out of bg_arb_pipeline_lib namespace. |
| bg_unified_analysis | airflow/dags/bg_unified_analysis_dag.py | Harden schema-on-the-fly + TRUNCATE-replace; dedupe per-task UUID5 event-id logic. |
| bg_unified | airflow/dags/bg_unified_dag.py | Move hardcoded OddsAPI key to secret; reframe sharp-book gating as analytics-only; consider history retention. |
| bg_weather | airflow/dags/bg_weather.py | Externalize MLB venue map; re-home bg_arb_pipeline_lib.db; clean 'the bot' comments. |
| cleanup_logs | airflow/dags/cleanup_logs.py | Parameterize log path + retention; move to shared maintenance module. |
| update_underdog_outlier_analysis | airflow/dags/update_underdog_outlier_analysis.py | Move OddsAPI key to secret; fix fake r2_exp placeholder; fix if_exists=replace truncation of history; prune dead market_lines code. |

### Repo: kalshi (root: `C:/Users/tkmer/kalshi`)

| dag_id | file | key rewrite items |
|---|---|---|
| kalshi_arb_explorer | C:/Users/tkmer/kalshi/dags/kalshi_arb_explorer_dag.py | Persist outputs to analytics Postgres instead of per-day JSONL; consolidate Kalshi/Odds fan-out into shared ingestion. |
| kalshi_snapshot | C:/Users/tkmer/kalshi/dags/kalshi_snapshot_dag.py | Persist market_snapshot to Postgres odds-comparison table; decouple from arb-era JSONL logging. |

## ARCHIVE (decommission — places/cancels live bets)

Direct action targets. Both place and cancel LIVE Kalshi orders; remove from the active deployment.

| dag_id | file | reason |
|---|---|---|
| kalshi_maker_ev_bot | C:/Users/tkmer/kalshi/dags/kalshi_maker_ev_dag.py | Live maker +EV bot; place_order/cancel_order each cycle. Execution-only. Analytics covered by kalshi_arb_explorer + kalshi_snapshot. Salvage utils.trading_logic.find_plus_ev_trades separately if needed. |
| kalshi_cross_poll_bot | C:/Users/tkmer/kalshi/dags/kalshi_cross_poll_dag.py | Live maker +EV bot (cross-game pollution variant, near-clone of maker_ev). place_order/cancel_order each cycle. Execution-only. |

### Associated execution-coupling cleanup (in bountygate, during REWRITE)

These are not standalone DAGs to archive but execution couplings to remove from kept DAGs:

- Table `bot_execution_queue` — drop PENDING inserts in bg_arb_pipeline and bg_arbitrage_player_props.
- Table `bg_executed_opportunities` — drop the execution-dedup read in bg_arb_pipeline.
- Drop `enqueue_opportunities_task` (bg_arb_pipeline) and `trigger_bot_execution` (bg_arbitrage_player_props) and their gate constants (MIN_QUALIFYING_ROI, EXECUTABLE_BOOKS, EXECUTABLE_SPORTS, BOT_EXECUTION_TASK_COUNT, MARKET_BLACKLIST).

## Bucket summary

| Bucket | Count | dag_ids |
|---|---|---|
| KEEP (as-is) | 0 | — |
| REWRITE | 19 | bountygate: bg_analysis_sheets, bg_arb_pipeline, bg_arbitrage_player_props, bg_arbitrage_sheets, bg_closing_line, bg_dimensional_model, bg_game_arb_pipeline, bg_injuries, bg_marts, bg_methodology, bg_normalization, bg_results, bg_unified_analysis, bg_unified, bg_weather, cleanup_logs, update_underdog_outlier_analysis; kalshi: kalshi_arb_explorer, kalshi_snapshot |
| ARCHIVE | 2 | kalshi: kalshi_maker_ev_bot, kalshi_cross_poll_bot |
