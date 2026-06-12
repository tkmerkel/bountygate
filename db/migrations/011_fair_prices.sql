CREATE TABLE fair_prices (
  event_id     uuid        NOT NULL,
  market_type  text        NOT NULL,
  bookmaker    text        NOT NULL,
  outcome_name text        NOT NULL,
  method       text        NOT NULL,
  fair_prob    numeric     NOT NULL,
  captured_at  timestamptz NOT NULL
) PARTITION BY RANGE (captured_at);
CREATE INDEX ix_fair_prices_event ON fair_prices (event_id, captured_at);
CREATE INDEX brin_fair_prices_captured ON fair_prices USING brin (captured_at);

SELECT partman.create_parent(p_parent_table := 'public.fair_prices',
  p_control := 'captured_at', p_interval := '1 day', p_type := 'range', p_premake := 4);

UPDATE partman.part_config SET retention = '2 years', retention_keep_table = false
WHERE parent_table = 'public.fair_prices';

CREATE TABLE mart_fair_odds (
  event_id       uuid NOT NULL,
  sport_key      text,
  commence_time  timestamptz,
  home_team      text,
  away_team      text,
  market_type    text NOT NULL,
  outcome_name   text NOT NULL,
  consensus_prob numeric,
  best_price     numeric,
  best_bookmaker text,
  edge           numeric,
  computed_at    timestamptz
);
CREATE INDEX ix_mart_fair_odds_event ON mart_fair_odds (event_id);
