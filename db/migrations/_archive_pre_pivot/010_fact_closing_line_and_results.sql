-- Sharp closing snapshot captured at/just before event commence.
-- closing_hash is a content hash keyed on (bg_event_id, bookmaker_key, market_key, player_name, side).
-- Time columns are naive UTC (timestamp) because bulk_append_new COPY strips tz.
CREATE TABLE IF NOT EXISTS fact_closing_line (
    closing_hash             text         PRIMARY KEY,
    bg_event_id              text,
    sport_key                text,
    bookmaker_key            text,
    market_key               text,
    base_market_key          text,
    player_name              text,
    player_id                uuid,
    side                     text,
    line                     numeric,
    closing_price_decimal    numeric,
    closing_fair_prob        numeric,
    captured_at_utc          timestamp,
    commence_at_utc          timestamp
);

CREATE INDEX IF NOT EXISTS idx_fcl_event_market_player_side
    ON fact_closing_line (bg_event_id, market_key, player_name, side);

-- Game-level result. Filled in Phase 2 by the bg_results DAG.
-- bg_event_id is the PK (one result row per event).
CREATE TABLE IF NOT EXISTS fact_game_result (
    bg_event_id              text         PRIMARY KEY,
    sport_key                text,
    home_score               integer,
    away_score               integer,
    winner_team_id           uuid,
    status                   text,
    result_source            text,
    settled_at_utc           timestamp,
    raw                      jsonb
);

CREATE INDEX IF NOT EXISTS idx_fgr_sport_settled
    ON fact_game_result (sport_key, settled_at_utc);

-- Player stat result for prop grading. Filled in Phase 2 by the bg_results DAG.
-- result_hash is a content hash keyed on (bg_event_id, player_id, stat_key).
CREATE TABLE IF NOT EXISTS fact_player_stat_result (
    result_hash              text         PRIMARY KEY,
    bg_event_id              text,
    player_id                uuid,
    player_name              text,
    sport_key                text,
    stat_key                 text,
    stat_value               numeric,
    result_source            text,
    settled_at_utc           timestamp
);

CREATE INDEX IF NOT EXISTS idx_fpsr_event_player_stat
    ON fact_player_stat_result (bg_event_id, player_id, stat_key);
