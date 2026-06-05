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

// Skeletons inlined from docs/superpowers/inventory/prescan.json (args passing is
// unreliable across the Bun workflow boundary, so the data lives in the script).
const skeletons = [
  { file: "airflow/dags/bg_analysis_sheets_dag.py", repo: "bountygate", dag_id: "bg_analysis_sheets", schedule: null, imports: ["bountygate.utils.db_connection", "gspread", "pandas"], tables: ["bg_unified_analysis", "bg_unified_arbitrage"] },
  { file: "airflow/dags/bg_arb_pipeline.py", repo: "bountygate", dag_id: "bg_arb_pipeline", schedule: "*/5 * * * *", imports: ["bg_arb_pipeline_lib.builder", "bg_arb_pipeline_lib.db", "bg_arb_pipeline_lib.ingest", "bountygate.utils.db_connection", "bountygate.utils.etl_assets", "sqlalchemy"], tables: ["bg_executed_opportunities", "bot_execution_queue"] },
  { file: "airflow/dags/bg_arbitrage_player_props.py", repo: "bountygate", dag_id: "bg_arbitrage_player_props", schedule: null, imports: ["bountygate.utils.db_connection", "bountygate.utils.discord_notify", "pandas"], tables: ["bot_execution_queue"] },
  { file: "airflow/dags/bg_arbitrage_sheets_dag.py", repo: "bountygate", dag_id: "bg_arbitrage_sheets", schedule: null, imports: ["bountygate.utils.db_connection", "gspread", "pandas"], tables: ["bg_arbitrage_player_props", "bg_arbitrage_player_props_alt"] },
  { file: "airflow/dags/bg_closing_line.py", repo: "bountygate", dag_id: "bg_closing_line", schedule: "*/5 * * * *", imports: ["bg_arb_pipeline_lib.db", "bountygate.analytics", "bountygate.utils.db_connection"], tables: ["closing_fair_prob", "fact_closing_line", "fact_clv", "fact_ev_opportunity", "fact_odds_snapshot"] },
  { file: "airflow/dags/bg_dimensional_model.py", repo: "bountygate", dag_id: "bg_dimensional_model", schedule: null, imports: ["bg_analytics_lib.common", "bg_arb_pipeline_lib.db", "sqlalchemy"], tables: ["dim_event", "dim_market", "dim_player", "fact_odds_snapshot"] },
  { file: "airflow/dags/bg_game_arb_pipeline.py", repo: "bountygate", dag_id: "bg_game_arb_pipeline", schedule: "*/10 * * * *", imports: ["bg_game_arb_pipeline_lib.alerts", "bg_game_arb_pipeline_lib.builder", "bg_game_arb_pipeline_lib.db", "bg_game_arb_pipeline_lib.ingest", "bountygate.utils.db_connection", "bountygate.utils.etl_assets"], tables: ["bg_arb_pipeline"] },
  { file: "airflow/dags/bg_injuries.py", repo: "bountygate", dag_id: "bg_injuries", schedule: "@hourly", imports: ["bg_arb_pipeline_lib.db", "bountygate.enrichment", "bountygate.utils.db_connection"], tables: ["dim_event"] },
  { file: "airflow/dags/bg_marts.py", repo: "bountygate", dag_id: "bg_marts", schedule: null, imports: ["bg_analytics_lib.common", "bg_arb_pipeline_lib.db", "bountygate.utils.db_connection"], tables: ["dim_event", "dim_sport", "fact_clv", "fact_ev_opportunity", "fact_fair_prob", "fact_odds_snapshot", "mart_arbitrage"] },
  { file: "airflow/dags/bg_methodology.py", repo: "bountygate", dag_id: "bg_methodology", schedule: null, imports: ["bg_analytics_lib.common", "bg_arb_pipeline_lib.db", "bountygate.analytics", "bountygate.utils.db_connection"], tables: ["fact_fair_prob", "fact_odds_snapshot"] },
  { file: "airflow/dags/bg_normalization.py", repo: "bountygate", dag_id: "bg_normalization", schedule: null, imports: ["bountygate.utils", "sqlalchemy"], tables: ["bg_unified_lines", "bg_unified_lines_normalized", "dq_metrics", "market_aliases", "team_aliases", "team_reference"] },
  { file: "airflow/dags/bg_results.py", repo: "bountygate", dag_id: "bg_results", schedule: "*/30 * * * *", imports: ["bg_arb_pipeline_lib.db", "bountygate.enrichment", "bountygate.utils.db_connection"], tables: ["dim_event"] },
  { file: "airflow/dags/bg_unified_analysis_dag.py", repo: "bountygate", dag_id: "bg_unified_analysis", schedule: null, imports: ["bountygate.utils.db_connection"], tables: ["bg_unified_lines_normalized_mv"] },
  { file: "airflow/dags/bg_unified_dag.py", repo: "bountygate", dag_id: "bg_unified", schedule: "*/5 * * * *", imports: ["bountygate.data_loaders.prizepicks", "bountygate.transformers.underdog", "bountygate.utils", "bountygate.utils.db_connection", "bountygate.utils.etl_assets", "requests", "sqlalchemy"], tables: ["market_aliases", "unified"] },
  { file: "airflow/dags/bg_weather.py", repo: "bountygate", dag_id: "bg_weather", schedule: "0 */3 * * *", imports: ["bg_arb_pipeline_lib.db", "bountygate.enrichment", "bountygate.utils.db_connection"], tables: ["dim_event"] },
  { file: "airflow/dags/cleanup_logs.py", repo: "bountygate", dag_id: "cleanup_logs", schedule: "@daily", imports: ["airflow.models.dag", "airflow.operators.bash", "pendulum"], tables: [] },
  { file: "airflow/dags/update_underdog_outlier_analysis.py", repo: "bountygate", dag_id: null, schedule: "*/15 * * * *", imports: ["bountygate.utils.db_connection", "bountygate.utils.etl_assets", "requests"], tables: ["first_spread"] },
  { file: "C:/Users/tkmer/kalshi/dags/kalshi_arb_explorer_dag.py", repo: "kalshi", dag_id: null, schedule: "*/10 * * * *", imports: ["utils.arb_logic", "utils.kalshi_client", "utils.odds_client", "utils.run_log"], tables: [] },
  { file: "C:/Users/tkmer/kalshi/dags/kalshi_cross_poll_dag.py", repo: "kalshi", dag_id: null, schedule: "*/10 * * * *", imports: ["utils.kalshi_client", "utils.odds_client", "utils.risk_gates", "utils.run_log", "utils.trading_logic"], tables: [] },
  { file: "C:/Users/tkmer/kalshi/dags/kalshi_maker_ev_dag.py", repo: "kalshi", dag_id: null, schedule: "*/10 * * * *", imports: ["utils.kalshi_client", "utils.odds_client", "utils.risk_gates", "utils.run_log", "utils.trading_logic"], tables: [] },
  { file: "C:/Users/tkmer/kalshi/dags/kalshi_snapshot_dag.py", repo: "kalshi", dag_id: null, schedule: "*/30 * * * *", imports: ["utils.event_match", "utils.kalshi_client", "utils.odds_client", "utils.run_log", "utils.team_names"], tables: ["fills"] },
]

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
