-- Extensions for the analytics-aggregator backend.
-- pg_cron is intentionally absent: Heroku Postgres's rds.allowed_extensions blocks it;
-- partition maintenance runs from an Airflow DAG instead.
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
