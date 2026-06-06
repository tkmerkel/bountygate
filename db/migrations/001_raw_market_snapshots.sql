-- Fresh baseline for the analytics-aggregator pivot.
-- Append-only raw landing for all marketplace connectors (Kalshi/Polymarket/Odds).
CREATE TABLE IF NOT EXISTS raw_market_snapshots (
  id           bigserial   PRIMARY KEY,
  source       text        NOT NULL,   -- 'kalshi' | 'polymarket' | 'the_odds_api'
  source_key   text        NOT NULL,   -- ticker / condition_id / event+market+book
  record_type  text        NOT NULL,   -- 'market' | 'orderbook' | 'odds_line'
  captured_at  timestamptz NOT NULL,
  payload      jsonb       NOT NULL,
  ingested_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_snap_source_time ON raw_market_snapshots (source, captured_at);
CREATE INDEX IF NOT EXISTS ix_raw_snap_source_key  ON raw_market_snapshots (source, source_key, captured_at);
