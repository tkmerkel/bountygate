-- Append-only every-quote time-series. One row per unique (book, market, side, line, price, fetch window).
-- PK quote_hash is a content hash computed by the loader (bulk_append_new deduplicates on it).
-- Time columns are naive UTC (timestamp) because bulk_append_new COPY strips tz.
CREATE TABLE IF NOT EXISTS fact_odds_snapshot (
    quote_hash               text         PRIMARY KEY,
    bg_event_id              text         NOT NULL,
    sport_key                text         NOT NULL,
    bookmaker_key            text         NOT NULL,
    market_key               text         NOT NULL,
    base_market_key          text         NOT NULL,
    player_name              text,
    player_id                uuid,
    side                     text         NOT NULL,
    line                     numeric,
    price_decimal            numeric      NOT NULL,
    implied_prob             numeric,
    commence_at_utc          timestamp    NOT NULL,
    fetched_at_utc           timestamp    NOT NULL,
    is_sharp                 boolean      NOT NULL DEFAULT false,
    is_consensus_source      boolean      NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_fos_event_market_player_fetched
    ON fact_odds_snapshot (bg_event_id, market_key, player_name, fetched_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_fos_fetched
    ON fact_odds_snapshot (fetched_at_utc);

CREATE INDEX IF NOT EXISTS idx_fos_book_fetched
    ON fact_odds_snapshot (bookmaker_key, fetched_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_fos_sharp_event_market
    ON fact_odds_snapshot (bg_event_id, market_key)
    WHERE is_sharp;
