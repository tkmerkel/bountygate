-- Derived closing line per event/market/book/outcome (last pre-commence snapshot).
CREATE TABLE closing_lines (
  event_id          uuid NOT NULL,
  market_type       text NOT NULL,
  bookmaker         text NOT NULL,
  outcome_name      text NOT NULL,
  decimal_price     numeric,
  fair_prob         numeric,
  captured_at       timestamptz,
  staleness_minutes numeric,
  UNIQUE (event_id, market_type, bookmaker, outcome_name)
);
CREATE INDEX ix_closing_lines_event ON closing_lines (event_id);
