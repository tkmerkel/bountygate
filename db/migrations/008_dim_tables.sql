-- Kimball dimension tables for the BountyGate analytics layer.
-- All statements are idempotent (IF NOT EXISTS / ON CONFLICT DO NOTHING).
-- No PL/pgSQL; no DO $$ blocks; no CREATE FUNCTION/TRIGGER.
-- Audit-only timestamptz columns use DEFAULT now().
-- COPY-loaded fact time columns are naive-UTC timestamp (see 009+).

CREATE TABLE IF NOT EXISTS dim_sport (
    sport_key             text         PRIMARY KEY,
    sport_title           text,
    league                text,
    is_outdoor            boolean      DEFAULT false,
    active                boolean      DEFAULT true
);

CREATE TABLE IF NOT EXISTS dim_bookmaker (
    bookmaker_key         text         PRIMARY KEY,
    bookmaker_title       text,
    is_sharp              boolean      DEFAULT false,
    is_soft               boolean      DEFAULT false,
    is_legal_for_betting  boolean      DEFAULT false,
    region                text
);

CREATE TABLE IF NOT EXISTS dim_team (
    team_id               uuid         PRIMARY KEY,
    sport_key             text         NOT NULL,
    display_name          text         NOT NULL,
    abbreviation          text,
    espn_team_id          text,
    mlb_team_id           integer,
    nhl_team_id           integer,
    balldontlie_team_id   integer,
    aliases               jsonb        DEFAULT '[]'::jsonb,
    active                boolean      DEFAULT true
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_team_sport_name
    ON dim_team (sport_key, display_name);

CREATE TABLE IF NOT EXISTS dim_player (
    player_id             uuid         PRIMARY KEY,
    sport_key             text         NOT NULL,
    full_name             text         NOT NULL,
    normalized_name       text         NOT NULL,
    team_id               uuid,
    position              text,
    espn_player_id        text,
    mlb_player_id         integer,
    nhl_player_id         integer,
    balldontlie_player_id integer,
    created_at_utc        timestamptz  DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_player_sport_name
    ON dim_player (sport_key, normalized_name);

CREATE INDEX IF NOT EXISTS idx_dim_player_sport_normalized
    ON dim_player (sport_key, normalized_name);

CREATE TABLE IF NOT EXISTS dim_market (
    market_key            text         PRIMARY KEY,
    sport_key             text,
    market_group          text,
    is_player_prop        boolean      DEFAULT false,
    is_alternate          boolean      DEFAULT false,
    base_market_key       text
);

CREATE INDEX IF NOT EXISTS idx_dim_market_sport
    ON dim_market (sport_key);

CREATE TABLE IF NOT EXISTS dim_event (
    bg_event_id           text         PRIMARY KEY,
    event_id_source       text,
    sport_key             text,
    home_team_id          uuid,
    away_team_id          uuid,
    home_team_name        text,
    away_team_name        text,
    commence_at_utc       timestamptz,
    venue                 text,
    created_at_utc        timestamptz  DEFAULT now(),
    updated_at_utc        timestamptz
);

CREATE INDEX IF NOT EXISTS idx_dim_event_sport_commence
    ON dim_event (sport_key, commence_at_utc);

CREATE INDEX IF NOT EXISTS idx_dim_event_commence
    ON dim_event (commence_at_utc);
