-- 006_dashboard_state.sql
-- Tables backing the new dashboard endpoints and the watcher heartbeat system.

CREATE TABLE dashboard_runs (
    run_id              text PRIMARY KEY,
    occurred_at         timestamptz NOT NULL,
    player              text NOT NULL,
    market              text NOT NULL,
    outcome             text NOT NULL CHECK (outcome IN ('success','failure','skipped')),
    duration_s          numeric,
    issues              jsonb NOT NULL DEFAULT '{}'::jsonb,
    top_finding         text,
    video_url           text,
    review_url          text,
    inserted_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX dashboard_runs_occurred_at_idx ON dashboard_runs (occurred_at DESC);

CREATE TABLE account_stats (
    book                text PRIMARY KEY,
    balance             numeric,
    pending_wagers      numeric,
    available_liquidity numeric,
    pnl_7d              numeric,
    scrape_status       text NOT NULL,
    last_error          text,
    scraped_at          timestamptz NOT NULL
);

CREATE TABLE account_stats_history (
    book        text NOT NULL,
    scraped_at  timestamptz NOT NULL,
    balance     numeric,
    pnl_7d      numeric,
    PRIMARY KEY (book, scraped_at)
);

CREATE TABLE watcher_heartbeats (
    name                  text PRIMARY KEY,
    is_running            boolean NOT NULL,
    last_tick_at          timestamptz NOT NULL,
    pending_count         int NOT NULL DEFAULT 0,
    oldest_pending_age_s  int,
    completed_24h         int NOT NULL DEFAULT 0,
    errors_24h            int NOT NULL DEFAULT 0,
    last_error            text,
    expected_interval_s   int NOT NULL
);
