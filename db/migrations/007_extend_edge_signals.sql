-- Sportsbook signals need event/bookmaker dimensions the Spec-2 contract lacked.
-- Additive + nullable: market_id/outcome_id stay for future prediction-market signals.
ALTER TABLE mart_edge_signals ADD COLUMN IF NOT EXISTS event_id     uuid;
ALTER TABLE mart_edge_signals ADD COLUMN IF NOT EXISTS bookmaker    text;
ALTER TABLE mart_edge_signals ADD COLUMN IF NOT EXISTS market_type  text;
ALTER TABLE mart_edge_signals ADD COLUMN IF NOT EXISTS outcome_name text;
CREATE INDEX IF NOT EXISTS ix_mart_edge_event ON mart_edge_signals (event_id);
