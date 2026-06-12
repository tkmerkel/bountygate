-- DEPLOY ORDER (required): ON CONFLICT arbiter inference needs an exact unique-index match,
-- so old code (5-col conflict target) fails against the new 6-col index and vice versa.
-- A dual-index overlap is NOT safe either: the old 5-col index would reject multi-line
-- totals rows (Over 8.5 vs 9.5, same captured_at) as duplicates on a non-arbiter index.
-- Procedure: pause ingest_* + normalize DAGs -> apply this migration -> rebuild/recreate
-- Airflow containers with the new image -> unpause. (Index rebuild on the partitioned
-- parent takes ACCESS EXCLUSIVE across all partitions; do not add CONCURRENTLY - it
-- cannot run inside the migration transaction.)

-- Spreads/totals ingestion needs a per-outcome line value; h2h rows leave it NULL.
-- The natural unique key (009) must now include point so e.g. Over 8.5 and Over 9.5
-- from the same book at the same capture are distinct rows.
ALTER TABLE sportsbook_odds_history ADD COLUMN IF NOT EXISTS point numeric;
DROP INDEX IF EXISTS uq_sb_odds_natural;
-- NULLS NOT DISTINCT requires PG>=15 (verify SELECT version() at deploy; fallback for <15:
-- unique expression index on COALESCE(point, -99999) -- 0 is a legal totals/spread line).
CREATE UNIQUE INDEX uq_sb_odds_natural
  ON sportsbook_odds_history (event_id, market_type, bookmaker, outcome_name, point, captured_at)
  NULLS NOT DISTINCT;
