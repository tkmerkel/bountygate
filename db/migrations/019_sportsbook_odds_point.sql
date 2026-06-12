-- Spreads/totals ingestion needs a per-outcome line value; h2h rows leave it NULL.
-- The natural unique key (009) must now include point so e.g. Over 8.5 and Over 9.5
-- from the same book at the same capture are distinct rows.
ALTER TABLE sportsbook_odds_history ADD COLUMN IF NOT EXISTS point numeric;
DROP INDEX IF EXISTS uq_sb_odds_natural;
-- NULLS NOT DISTINCT requires PG>=15 (verify SELECT version() at deploy; fallback for <15:
-- unique expression index on COALESCE(point, 0)).
CREATE UNIQUE INDEX uq_sb_odds_natural
  ON sportsbook_odds_history (event_id, market_type, bookmaker, outcome_name, point, captured_at)
  NULLS NOT DISTINCT;
