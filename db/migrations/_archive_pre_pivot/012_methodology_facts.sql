-- Methodology fact tables: devig fair probs, EV opportunities, CLV, line movement.
-- All statements are idempotent (IF NOT EXISTS / ON CONFLICT DO NOTHING).
-- No PL/pgSQL; no DO $$ blocks; no CREATE FUNCTION/TRIGGER.
-- COPY-loaded time columns are naive-UTC timestamp; audit defaults use timestamptz DEFAULT now().

CREATE TABLE IF NOT EXISTS fact_fair_prob (
    fair_hash                 text         PRIMARY KEY,
    bg_event_id               text         NOT NULL,
    sport_key                 text,
    market_key                text         NOT NULL,
    base_market_key           text,
    player_name               text,
    player_id                 uuid,
    line                      numeric,
    pinnacle_over_price       numeric,
    pinnacle_under_price      numeric,
    fair_prob_over_mult       numeric,
    fair_prob_under_mult      numeric,
    fair_prob_over_power      numeric,
    fair_prob_under_power     numeric,
    fair_prob_over_shin       numeric,
    fair_prob_under_shin      numeric,
    consensus_fair_prob_over  numeric,
    consensus_fair_prob_under numeric,
    method_disagreement       numeric,
    disagreement_flag         boolean,
    overround                 numeric,
    fetched_at_utc            timestamp    NOT NULL,
    commence_at_utc           timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ffp_event_market_player_fetched
    ON fact_fair_prob (bg_event_id, market_key, player_name, fetched_at_utc DESC);

CREATE TABLE IF NOT EXISTS fact_ev_opportunity (
    ev_hash                   text         PRIMARY KEY,
    bg_event_id               text         NOT NULL,
    sport_key                 text,
    market_key                text         NOT NULL,
    player_name               text,
    player_id                 uuid,
    side                      text,
    line                      numeric,
    soft_book                 text,
    soft_price_decimal        numeric,
    fair_prob                 numeric,
    fair_method               text,
    edge                      numeric,
    edge_pct                  numeric,
    kelly_full                numeric,
    kelly_quarter             numeric,
    stake_capped              numeric,
    pinnacle_no_vig_price     numeric,
    disagreement_flag         boolean,
    commence_at_utc           timestamp,
    hours_until_commence      numeric,
    fetched_at_utc            timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fevo_edge_pct
    ON fact_ev_opportunity (edge_pct DESC);

CREATE INDEX IF NOT EXISTS idx_fevo_event
    ON fact_ev_opportunity (bg_event_id);

CREATE INDEX IF NOT EXISTS idx_fevo_fetched
    ON fact_ev_opportunity (fetched_at_utc DESC);

CREATE TABLE IF NOT EXISTS fact_ev_opportunity_history (
    ev_history_hash           text         PRIMARY KEY,
    ev_hash                   text         NOT NULL,
    bg_event_id               text         NOT NULL,
    sport_key                 text,
    market_key                text         NOT NULL,
    player_name               text,
    player_id                 uuid,
    side                      text,
    line                      numeric,
    soft_book                 text,
    soft_price_decimal        numeric,
    fair_prob                 numeric,
    fair_method               text,
    edge                      numeric,
    edge_pct                  numeric,
    kelly_full                numeric,
    kelly_quarter             numeric,
    stake_capped              numeric,
    pinnacle_no_vig_price     numeric,
    disagreement_flag         boolean,
    commence_at_utc           timestamp,
    hours_until_commence      numeric,
    fetched_at_utc            timestamp    NOT NULL,
    first_seen_at_utc         timestamp    DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX IF NOT EXISTS idx_fevoh_player_market
    ON fact_ev_opportunity_history (player_name, market_key);

CREATE TABLE IF NOT EXISTS fact_clv (
    clv_hash                  text         PRIMARY KEY,
    bg_event_id               text         NOT NULL,
    market_key                text         NOT NULL,
    player_name               text,
    side                      text,
    line                      numeric,
    bet_price_decimal         numeric,
    bet_fair_prob             numeric,
    closing_price_decimal     numeric,
    closing_fair_prob         numeric,
    clv_pct                   numeric,
    beat_close                boolean,
    recorded_at_utc           timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fclv_event_market_player
    ON fact_clv (bg_event_id, market_key, player_name);

CREATE TABLE IF NOT EXISTS fact_line_movement (
    movement_hash             text         PRIMARY KEY,
    bg_event_id               text         NOT NULL,
    market_key                text         NOT NULL,
    player_name               text,
    side                      text,
    line                      numeric,
    window_start_utc          timestamp    NOT NULL,
    window_end_utc            timestamp    NOT NULL,
    soft_open_price           numeric,
    soft_close_price          numeric,
    pinnacle_open_price       numeric,
    pinnacle_close_price      numeric,
    soft_vs_sharp_gap         numeric,
    reverse_line_movement     boolean,
    computed_at_utc           timestamp    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_flm_event_market_player
    ON fact_line_movement (bg_event_id, market_key, player_name);
