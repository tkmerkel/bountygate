-- 007_bg_game_arb_pipeline_tables.sql
-- Game-line arbitrage pipeline: separate from bg_arb_pipeline (player props).
-- Detection-only v1; bot executor integration is a Phase 2 follow-up.

-- Stage table: per-(book, market_key, outcome, point) rows from the-odds-api.
-- Replaced every ingest run.
CREATE TABLE IF NOT EXISTS bg_game_arb_stage_lines (
    event_id              text         NOT NULL,
    sport_key             text         NOT NULL,
    sport_title           text         NOT NULL,
    home_team             text         NOT NULL,
    away_team             text         NOT NULL,
    commence_time_utc     timestamp    NOT NULL,
    bookmaker_key         text         NOT NULL,
    market_key            text         NOT NULL CHECK (market_key IN ('h2h','spreads','totals')),
    outcome               text         NOT NULL,
    point                 numeric      NULL,
    price                 numeric      NOT NULL,
    fetched_at_utc        timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_game_arb_stage_event
    ON bg_game_arb_stage_lines (event_id, market_key);
CREATE INDEX IF NOT EXISTS idx_game_arb_stage_book
    ON bg_game_arb_stage_lines (bookmaker_key);

-- Current arb-able opportunities. Replaced every build run.
-- Per-leg columns mirror the discipline of bg_arbitrage_opportunities but
-- carry game-line semantics: leg_a/leg_b instead of under/over.
CREATE TABLE IF NOT EXISTS bg_arbitrage_game_opportunities (
    opportunity_hash       text PRIMARY KEY,
    event_id               text         NOT NULL,
    sport_key              text         NOT NULL,
    sport_title            text         NOT NULL,
    home_team              text         NOT NULL,
    away_team              text         NOT NULL,
    commence_time_utc      timestamp    NOT NULL,
    market_key             text         NOT NULL CHECK (market_key IN ('h2h','spreads','totals')),

    leg_a_book             text         NOT NULL,
    leg_a_outcome          text         NOT NULL,
    leg_a_point            numeric      NULL,
    leg_a_price            numeric      NOT NULL,

    leg_b_book             text         NOT NULL,
    leg_b_outcome          text         NOT NULL,
    leg_b_point            numeric      NULL,
    leg_b_price            numeric      NOT NULL,

    wager_leg_a            numeric      NOT NULL,
    wager_leg_b            numeric      NOT NULL,
    payout                 numeric      NOT NULL,
    arb_ev                 numeric      NOT NULL,
    roi                    numeric      NOT NULL,

    hours_until_commence   numeric      NOT NULL,
    fetched_at_utc         timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_game_opp_fetched
    ON bg_arbitrage_game_opportunities (fetched_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_game_opp_roi
    ON bg_arbitrage_game_opportunities (roi DESC);
CREATE INDEX IF NOT EXISTS idx_game_opp_market
    ON bg_arbitrage_game_opportunities (market_key);
CREATE INDEX IF NOT EXISTS idx_game_opp_event
    ON bg_arbitrage_game_opportunities (event_id);

-- Append-only history. opportunity_hash dedups across runs.
CREATE TABLE IF NOT EXISTS bg_arb_game_opportunities_history (
    opportunity_hash       text PRIMARY KEY,
    event_id               text         NOT NULL,
    sport_key              text         NOT NULL,
    sport_title            text         NOT NULL,
    home_team              text         NOT NULL,
    away_team              text         NOT NULL,
    commence_time_utc      timestamp    NOT NULL,
    market_key             text         NOT NULL,

    leg_a_book             text         NOT NULL,
    leg_a_outcome          text         NOT NULL,
    leg_a_point            numeric      NULL,
    leg_a_price            numeric      NOT NULL,

    leg_b_book             text         NOT NULL,
    leg_b_outcome          text         NOT NULL,
    leg_b_point            numeric      NULL,
    leg_b_price            numeric      NOT NULL,

    wager_leg_a            numeric      NOT NULL,
    wager_leg_b            numeric      NOT NULL,
    payout                 numeric      NOT NULL,
    arb_ev                 numeric      NOT NULL,
    roi                    numeric      NOT NULL,

    hours_until_commence   numeric      NOT NULL,
    fetched_at_utc         timestamp    NOT NULL,
    first_seen_at_utc      timestamp    NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX IF NOT EXISTS idx_game_arb_history_market_fetched
    ON bg_arb_game_opportunities_history (market_key, fetched_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_game_arb_history_event
    ON bg_arb_game_opportunities_history (event_id);
