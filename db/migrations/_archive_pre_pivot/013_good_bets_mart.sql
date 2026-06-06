-- Good-bets mart tables: ranked board, market consensus, arbitrage.
-- mart_* tables are replace-every-run (no PK except mart_good_bets).
-- mart_market_consensus mirrors bg_unified_analysis column shape.
-- mart_arbitrage mirrors bg_unified_arbitrage column shape.
-- All statements are idempotent (IF NOT EXISTS / ON CONFLICT DO NOTHING).
-- No PL/pgSQL; no DO $$ blocks; no CREATE FUNCTION/TRIGGER.

CREATE TABLE IF NOT EXISTS mart_good_bets (
    good_bet_hash             text         PRIMARY KEY,
    bg_event_id               text         NOT NULL,
    sport_key                 text,
    sport_title               text,
    home_team_name            text,
    away_team_name            text,
    commence_at_utc           timestamp,
    hours_until_commence      numeric,
    market_key                text,
    player_name               text,
    side                      text,
    line                      numeric,
    opportunity_type          text,
    soft_book                 text,
    soft_price_decimal        numeric,
    fair_prob                 numeric,
    fair_method               text,
    edge_pct                  numeric,
    roi                       numeric,
    kelly_quarter             numeric,
    stake_capped              numeric,
    disagreement_flag         boolean,
    rank_score                numeric,
    fetched_at_utc            timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mgb_rank_score
    ON mart_good_bets (rank_score DESC);

CREATE INDEX IF NOT EXISTS idx_mgb_opportunity_type
    ON mart_good_bets (opportunity_type);

CREATE INDEX IF NOT EXISTS idx_mgb_sport_key
    ON mart_good_bets (sport_key);

CREATE INDEX IF NOT EXISTS idx_mgb_commence
    ON mart_good_bets (commence_at_utc);

CREATE TABLE IF NOT EXISTS mart_market_consensus (
    bg_event_id               text,
    player_name               text,
    outcome                   text,
    line                      numeric,
    market_key                text,
    price_mean                numeric,
    price_std                 numeric,
    price_count               integer,
    price_delta               numeric,
    multiplier_mean           numeric,
    multiplier_std            numeric,
    multiplier_count          integer,
    multiplier_delta          numeric,
    pinnacle_fair_prob        numeric,
    bookmaker_key             text,
    price                     numeric,
    fetched_at_utc            timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mmc_event_market
    ON mart_market_consensus (bg_event_id, market_key);

CREATE TABLE IF NOT EXISTS mart_arbitrage (
    bg_event_id               text,
    player_name               text,
    market_key                text,
    line                      numeric,
    under_bookmaker_key       text,
    under_price               numeric,
    under_line                numeric,
    over_bookmaker_key        text,
    over_price                numeric,
    over_line                 numeric,
    wager_under               numeric,
    wager_over                numeric,
    payout                    numeric,
    arb_ev                    numeric,
    roi                       numeric,
    fetched_at_utc            timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_marb_roi
    ON mart_arbitrage (roi DESC);
