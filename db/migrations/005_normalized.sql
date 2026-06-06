-- Normalized cross-venue contract (populated later by Airflow transforms).
CREATE TABLE venues (
  venue_key text PRIMARY KEY,
  kind      text NOT NULL          -- 'prediction' | 'sportsbook'
);
INSERT INTO venues (venue_key, kind) VALUES
  ('kalshi', 'prediction'),
  ('polymarket', 'prediction'),
  ('the_odds_api', 'sportsbook')
ON CONFLICT DO NOTHING;

CREATE TABLE markets (
  market_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  venue_key       text NOT NULL REFERENCES venues(venue_key),
  external_id     text NOT NULL,
  title           text,
  category        text,
  status          text,
  open_time       timestamptz,
  close_time      timestamptz,
  resolved_outcome text,
  resolution_time timestamptz,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (venue_key, external_id)
);

CREATE TABLE market_outcomes (
  outcome_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  market_id    uuid NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
  outcome_name text NOT NULL,
  outcome_index int,
  last_price   numeric,
  last_seen    timestamptz,
  UNIQUE (market_id, outcome_name)
);

CREATE TABLE sports_events (
  event_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_event_id text UNIQUE NOT NULL,
  sport_key       text,
  commence_time   timestamptz,
  home_team       text,
  away_team       text
);

CREATE TABLE market_event_links (
  market_id  uuid NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
  event_id   uuid NOT NULL REFERENCES sports_events(event_id) ON DELETE CASCADE,
  confidence numeric,
  method     text,
  UNIQUE (market_id, event_id)
);
