-- Stage table: raw line-level rows from the-odds-api after JSON normalization.
-- One row per (book, market_key, line, side). Replaced every ingest run.
CREATE TABLE IF NOT EXISTS bg_arb_stage_lines (
    event_id              text         NOT NULL,
    sport_title           text         NOT NULL,
    home_team             text         NOT NULL,
    away_team             text         NOT NULL,
    commence_time_utc     timestamp    NOT NULL,
    player_name           text         NOT NULL,
    bookmaker_key         text         NOT NULL,
    market_key            text         NOT NULL,
    line                  numeric      NOT NULL,
    side                  text         NOT NULL CHECK (side IN ('under', 'over')),
    price                 numeric      NOT NULL,
    fetched_at_utc        timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_arb_stage_event
    ON bg_arb_stage_lines (event_id, player_name);
CREATE INDEX IF NOT EXISTS idx_arb_stage_market
    ON bg_arb_stage_lines (market_key);

-- Current arb-able opportunities: what the bot reads.
CREATE TABLE IF NOT EXISTS bg_arbitrage_opportunities (
    opportunity_hash       text PRIMARY KEY,
    event_id               text         NOT NULL,
    sport_title            text         NOT NULL,
    home_team              text         NOT NULL,
    away_team              text         NOT NULL,
    player_name            text         NOT NULL,
    canonical_market       text         NOT NULL,
    pairing_type           text         NOT NULL CHECK (pairing_type IN ('std_std','std_alt','alt_std','alt_alt')),
    under_book             text         NOT NULL,
    under_market_key       text         NOT NULL,
    under_line             numeric      NOT NULL,
    under_price            numeric      NOT NULL,
    over_book              text         NOT NULL,
    over_market_key        text         NOT NULL,
    over_line              numeric      NOT NULL,
    over_price             numeric      NOT NULL,
    wager_under            numeric      NOT NULL,
    wager_over             numeric      NOT NULL,
    payout                 numeric      NOT NULL,
    arb_ev                 numeric      NOT NULL,
    roi                    numeric      NOT NULL,
    hours_until_commence   numeric      NOT NULL,
    fetched_at_utc         timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_arb_opp_fetched
    ON bg_arbitrage_opportunities (fetched_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_arb_opp_roi
    ON bg_arbitrage_opportunities (roi DESC);
CREATE INDEX IF NOT EXISTS idx_arb_opp_pairing
    ON bg_arbitrage_opportunities (pairing_type);
CREATE INDEX IF NOT EXISTS idx_arb_opp_player
    ON bg_arbitrage_opportunities (player_name);

-- Append-only history of every opportunity ever produced. For analysis only.
CREATE TABLE IF NOT EXISTS bg_arb_opportunities_history (
    opportunity_hash       text PRIMARY KEY,
    event_id               text         NOT NULL,
    sport_title            text         NOT NULL,
    home_team              text         NOT NULL,
    away_team              text         NOT NULL,
    player_name            text         NOT NULL,
    canonical_market       text         NOT NULL,
    pairing_type           text         NOT NULL,
    under_book             text         NOT NULL,
    under_market_key       text         NOT NULL,
    under_line             numeric      NOT NULL,
    under_price            numeric      NOT NULL,
    over_book              text         NOT NULL,
    over_market_key        text         NOT NULL,
    over_line              numeric      NOT NULL,
    over_price             numeric      NOT NULL,
    wager_under            numeric      NOT NULL,
    wager_over             numeric      NOT NULL,
    payout                 numeric      NOT NULL,
    arb_ev                 numeric      NOT NULL,
    roi                    numeric      NOT NULL,
    hours_until_commence   numeric      NOT NULL,
    fetched_at_utc         timestamp    NOT NULL,
    first_seen_at_utc      timestamp    NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX IF NOT EXISTS idx_arb_history_pairing_fetched
    ON bg_arb_opportunities_history (pairing_type, fetched_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_arb_history_player_market
    ON bg_arb_opportunities_history (player_name, canonical_market);
