-- Weather forecast snapshot per event. Filled in Phase 2 by the bg_weather DAG.
-- weather_hash is a content hash keyed on (bg_event_id, forecast_at_utc).
-- Time columns are naive UTC (timestamp) because bulk_append_new COPY strips tz.
CREATE TABLE IF NOT EXISTS fact_weather (
    weather_hash             text         PRIMARY KEY,
    bg_event_id              text,
    sport_key                text,
    forecast_at_utc          timestamp,
    commence_at_utc          timestamp,
    temp_f                   numeric,
    wind_mph                 numeric,
    wind_dir_deg             numeric,
    precip_prob              numeric,
    humidity                 numeric,
    source                   text         DEFAULT 'open-meteo',
    lat                      numeric,
    lon                      numeric
);

CREATE INDEX IF NOT EXISTS idx_fw_event
    ON fact_weather (bg_event_id);

-- Player injury/availability status. Filled in Phase 2 by the bg_injuries DAG.
-- status_hash is a content hash keyed on (player_id, report_at_utc, source).
CREATE TABLE IF NOT EXISTS dim_player_status (
    status_hash              text         PRIMARY KEY,
    player_id                uuid,
    player_name              text,
    sport_key                text,
    team_id                  uuid,
    status                   text,
    description              text,
    report_at_utc            timestamp,
    source                   text
);

CREATE INDEX IF NOT EXISTS idx_dps_player_reported
    ON dim_player_status (player_id, report_at_utc DESC);

-- Latest status row per player, resolved by most recent report_at_utc.
CREATE OR REPLACE VIEW vw_player_current_status AS
SELECT DISTINCT ON (player_id)
    status_hash,
    player_id,
    player_name,
    sport_key,
    team_id,
    status,
    description,
    report_at_utc,
    source
FROM dim_player_status
ORDER BY player_id, report_at_utc DESC;
