# Postgres Table Catalog

Producer = DAG(s) that write the table. Consumer = DAG(s) that read it (in-scope DAGs only; external/non-DAG consumers noted in parentheses). All tables are in repo `bountygate` unless noted. The `kalshi` DAGs use no Postgres application tables (JSONL only).

## Core odds / lines pipeline

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| bg_unified_lines_stage_odds | bg_unified | bg_arbitrage_player_props |
| bg_unified_lines_stage_splash | bg_unified | (internal to bg_unified union) |
| bg_unified_lines_stage_sleeper | bg_unified | (internal to bg_unified union) |
| bg_unified_lines_stage_underdog | bg_unified | (internal to bg_unified union) |
| bg_unified_lines_stage_prizepicks | bg_unified | (internal to bg_unified union) |
| bg_unified_lines | bg_unified, bg_normalization | bg_normalization |
| bg_unified_lines_normalized | (legacy/read in bg_normalization) | bg_normalization |
| bg_unified_lines_normalized_mv | bg_normalization | bg_unified_analysis, bg_dimensional_model, bg_marts |

## Reference / normalization

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| team_reference | bg_normalization | bg_normalization |
| team_aliases | bg_normalization | bg_normalization |
| market_aliases | bg_normalization | bg_normalization, bg_unified, update_underdog_outlier_analysis |
| dq_metrics | bg_normalization | (analytics/monitoring) |
| dim_bookmaker | (external seed) | bg_dimensional_model, bg_methodology |

## Dimensional model

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| dim_event | bg_dimensional_model (also populated by bg_events) | bg_injuries, bg_results, bg_weather |
| dim_player | bg_dimensional_model | (joins in analytics) |
| dim_market | bg_dimensional_model | (joins in analytics) |
| dim_sport | (external/dim seed) | bg_marts |
| dim_player_status | bg_injuries | (vw_player_current_status; props analytics) |

## Fact tables (analytics)

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| fact_odds_snapshot | bg_dimensional_model | bg_methodology, bg_marts, bg_closing_line |
| fact_fair_prob | bg_methodology | bg_methodology, bg_marts |
| fact_ev_opportunity | bg_methodology | bg_marts, bg_closing_line |
| fact_ev_opportunity_history | bg_methodology | (backtest analytics) |
| fact_line_movement | bg_methodology | (line-movement analytics) |
| fact_closing_line | bg_closing_line | bg_closing_line |
| fact_clv | bg_closing_line | bg_marts |
| fact_game_result | bg_results | bg_results (mart_bet_performance view) |
| fact_player_stat_result | bg_results | bg_results (mart_bet_performance view) |
| fact_weather | bg_weather | (MLB totals/props edge + backtest analytics) |

## Marts

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| mart_market_consensus | bg_marts | (dashboards / good-bets board) |
| mart_arbitrage | bg_marts | bg_marts (build_good_bets) |
| mart_good_bets | bg_marts | (bg_good_bets_ready consumers / dashboard) |

## Unified analysis / arbitrage

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| bg_unified_analysis | bg_unified_analysis | bg_analysis_sheets |
| bg_unified_arbitrage | bg_unified_analysis | bg_analysis_sheets |

## Player-prop arbitrage

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| bg_arbitrage_player_props | bg_arbitrage_player_props | bg_arbitrage_sheets |
| bg_arbitrage_player_props_alt | bg_arbitrage_player_props | bg_arbitrage_sheets |
| bg_arbitrage_player_props_history | bg_arbitrage_player_props | bg_arbitrage_player_props (dedup) |
| bg_arbitrage_player_props_alt_history | bg_arbitrage_player_props | bg_arbitrage_player_props (dedup) |

## Standalone arb pipelines

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| bg_arb_stage_lines | bg_arb_pipeline | bg_arb_pipeline |
| bg_arbitrage_opportunities | bg_arb_pipeline | bg_arb_pipeline |
| bg_arb_opportunities_history | bg_arb_pipeline | (backtest analytics) |
| bg_game_arb_stage_lines | bg_game_arb_pipeline | bg_game_arb_pipeline |
| bg_arbitrage_game_opportunities | bg_game_arb_pipeline | bg_game_arb_pipeline |
| bg_arb_game_opportunities_history | bg_game_arb_pipeline | (backtest analytics) |

## Underdog outlier analytics

| Table | Producer DAG(s) | Consumer DAG(s) |
|---|---|---|
| ud_games | update_underdog_outlier_analysis | update_underdog_outlier_analysis |
| ud_appearances | update_underdog_outlier_analysis | update_underdog_outlier_analysis |
| ud_over_under_lines | update_underdog_outlier_analysis | update_underdog_outlier_analysis |
| ud_options | update_underdog_outlier_analysis | update_underdog_outlier_analysis |
| ud_players | update_underdog_outlier_analysis | update_underdog_outlier_analysis |
| odds_player_props | update_underdog_outlier_analysis | update_underdog_outlier_analysis |
| odds_events | update_underdog_outlier_analysis | (analytics) |
| ud_analysis | update_underdog_outlier_analysis | (analytics) |
| ud_ou_lines_details | update_underdog_outlier_analysis | (analytics) |
| hd_ud_analysis | update_underdog_outlier_analysis | update_underdog_outlier_analysis (history read-back) |
| hd_ud_trend | update_underdog_outlier_analysis | (trend analytics) |
| bg_reference | update_underdog_outlier_analysis | (analytics) |

## Execution-coupled tables (ARCHIVE on rewrite)

| Table | Producer DAG(s) | Consumer DAG(s) | Disposition |
|---|---|---|---|
| bot_execution_queue | bg_arb_pipeline, bg_arbitrage_player_props | bg_arb_pipeline (PENDING backlog check) + external execution bot | DROP the inserts/reads on rewrite; table feeds the live execution bot |
| bg_executed_opportunities | (execution bot) | bg_arb_pipeline (execution dedup) | DROP the read on rewrite |

## Views (non-table consumers)

| View | Backed by | Notes |
|---|---|---|
| vw_player_current_status | dim_player_status | latest report per player |
| mart_bet_performance | fact_game_result, fact_player_stat_result, fact_ev_opportunity | grades prior +EV picks |
