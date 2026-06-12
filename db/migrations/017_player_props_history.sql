-- Player-prop odds time series (The Odds API), partitioned like sportsbook_odds_history.
CREATE TABLE IF NOT EXISTS player_props_odds_history (
  event_id      uuid        NOT NULL,
  market_key    text        NOT NULL,  -- raw odds-api key incl. _alternate
  player_name   text        NOT NULL,
  line          numeric     NOT NULL,
  side          text        NOT NULL,  -- 'over' | 'under'
  bookmaker     text        NOT NULL,
  decimal_price numeric,
  captured_at   timestamptz NOT NULL
) PARTITION BY RANGE (captured_at);

SELECT partman.create_parent(p_parent_table := 'public.player_props_odds_history',
  p_control := 'captured_at', p_interval := '1 day', p_type := 'range', p_premake := 4);

UPDATE partman.part_config SET retention = '2 years', retention_keep_table = false
WHERE parent_table = 'public.player_props_odds_history';

CREATE INDEX IF NOT EXISTS ix_props_event ON player_props_odds_history (event_id, captured_at);
CREATE INDEX IF NOT EXISTS brin_props_captured ON player_props_odds_history USING brin (captured_at);
-- Partition key must be included in the unique index (see 009); backs ON CONFLICT appends.
CREATE UNIQUE INDEX IF NOT EXISTS uq_props_natural ON player_props_odds_history
  (event_id, market_key, player_name, line, side, bookmaker, captured_at);
