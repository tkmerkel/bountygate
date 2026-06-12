-- Detected arbitrage opportunities (book x book and book x venue; game lines + props).
-- Upsert lifecycle: hash PK, ON CONFLICT bumps last_seen_at; price moves create new rows
-- (hash includes prices), so the table doubles as history. first==last seen => new.
CREATE TABLE IF NOT EXISTS arb_opportunities (
  opportunity_hash  text PRIMARY KEY,
  first_detected_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at      timestamptz NOT NULL DEFAULT now(),
  kind              text NOT NULL,      -- 'game' | 'prop'
  pairing           text NOT NULL,      -- 'book_book' | 'book_venue'
  event_id          uuid REFERENCES sports_events(event_id),
  sport_key         text,
  home_team         text,
  away_team         text,
  commence_time     timestamptz,
  market_segment    text NOT NULL,      -- 'h2h'|'spreads'|'totals'|canonical prop key
  player_name       text,               -- props only
  line              numeric,            -- prop line / totals point
  pairing_type      text,               -- std_std|std_alt|alt_std|alt_alt (book_book props only)
  leg_a_kind        text NOT NULL,      -- 'book' | 'venue'
  leg_a_source      text NOT NULL,      -- bookmaker key or venue_key
  leg_a_outcome     text NOT NULL,
  leg_a_point       numeric,
  leg_a_price       numeric NOT NULL,   -- decimal odds (book) or contract ask in $ (venue)
  leg_a_stake       numeric,
  leg_b_kind        text NOT NULL,
  leg_b_source      text NOT NULL,
  leg_b_outcome     text NOT NULL,
  leg_b_point       numeric,
  leg_b_price       numeric NOT NULL,
  leg_b_stake       numeric,
  payout            numeric,
  arb_ev            numeric,
  roi               numeric NOT NULL,
  fee_adjusted_roi  numeric NOT NULL,   -- == roi for book_book
  hours_until_commence numeric,
  details           jsonb               -- market_id/outcome_id/ticker/fee inputs/leg captured_ats
);
CREATE INDEX IF NOT EXISTS ix_arb_live ON arb_opportunities (last_seen_at DESC);
CREATE INDEX IF NOT EXISTS ix_arb_browse ON arb_opportunities (kind, pairing, fee_adjusted_roi DESC, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS ix_arb_event ON arb_opportunities (event_id);
