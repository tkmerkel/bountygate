-- Append-only analytical time-series, partitioned by captured_at (2y retention).
CREATE TABLE price_history (
  market_id   uuid        NOT NULL,
  outcome_id  uuid        NOT NULL,
  captured_at timestamptz NOT NULL,
  price       numeric,
  bid         numeric,
  ask         numeric,
  volume      numeric,
  liquidity   numeric
) PARTITION BY RANGE (captured_at);
CREATE INDEX ix_price_hist_outcome ON price_history (outcome_id, captured_at);
CREATE INDEX brin_price_hist_captured ON price_history USING brin (captured_at);

CREATE TABLE sportsbook_odds_history (
  event_id      uuid        NOT NULL,
  market_type   text        NOT NULL,
  bookmaker     text        NOT NULL,
  outcome_name  text        NOT NULL,
  captured_at   timestamptz NOT NULL,
  decimal_price numeric
) PARTITION BY RANGE (captured_at);
CREATE INDEX ix_sb_odds_event ON sportsbook_odds_history (event_id, captured_at);
CREATE INDEX brin_sb_odds_captured ON sportsbook_odds_history USING brin (captured_at);

SELECT partman.create_parent(p_parent_table := 'public.price_history',
  p_control := 'captured_at', p_interval := '1 day', p_type := 'range', p_premake := 4);
SELECT partman.create_parent(p_parent_table := 'public.sportsbook_odds_history',
  p_control := 'captured_at', p_interval := '1 day', p_type := 'range', p_premake := 4);

UPDATE partman.part_config SET retention = '2 years', retention_keep_table = false
WHERE parent_table IN ('public.price_history', 'public.sportsbook_odds_history');
