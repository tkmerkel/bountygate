-- Unique keys backing the idempotent ON CONFLICT appends in normalize.
-- On partitioned tables the unique index must include the partition key (captured_at).
CREATE UNIQUE INDEX IF NOT EXISTS uq_price_hist_outcome_time
  ON price_history (outcome_id, captured_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sb_odds_natural
  ON sportsbook_odds_history (event_id, market_type, bookmaker, outcome_name, captured_at);
