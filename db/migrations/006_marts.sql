-- Read-only product marts (populated later by Airflow transforms).
CREATE TABLE mart_cross_market_prices (
  question_key              text,
  captured_at               timestamptz,
  kalshi_prob               numeric,
  polymarket_prob           numeric,
  sportsbook_consensus_prob numeric,
  max_spread                numeric
);
CREATE INDEX ix_mart_xmkt_question ON mart_cross_market_prices (question_key, captured_at);

CREATE TABLE mart_edge_signals (
  signal_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  detected_at    timestamptz,
  venue_key      text,
  market_id      uuid,
  outcome_id     uuid,
  signal_type    text,             -- 'arb' | 'ev'
  fair_prob      numeric,
  venue_price    numeric,
  edge           numeric,
  kelly_fraction numeric
);
CREATE INDEX ix_mart_edge_detected ON mart_edge_signals (detected_at);

CREATE TABLE mart_market_history (
  market_id       uuid,
  resolved_outcome text,
  resolution_time timestamptz,
  predicted_prob  numeric,
  realized        boolean,
  clv             numeric
);
CREATE INDEX ix_mart_hist_market ON mart_market_history (market_id);
