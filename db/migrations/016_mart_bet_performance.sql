-- Phase 2 settlement / track-record mart.
-- Grades every +EV pick we ever flagged against the actual player-stat result.
--
-- Design notes:
--   * Reads fact_ev_opportunity_history (accumulates picks) not
--     fact_ev_opportunity (replace-every-run; drops finished events).
--   * fact_ev_opportunity_history.player_id is NULL (the methodology DAG does
--     not populate it), so the join is NAME-BASED, replicating the Python
--     _normalize_player_name contract in SQL: lower -> strip non [a-z0-9 ] ->
--     collapse whitespace -> trim. This matches bountygate.enrichment.match.
--   * stat_key uses BASE market keys, so we strip a trailing '_alternate' from
--     the pick's market_key before joining (mirrors _base_market_key).
--   * outcome mirrors statmap.settle (over/under/push); profit is per-unit and
--     per-staked at the soft price taken.
-- Idempotent: CREATE OR REPLACE VIEW. No PL/pgSQL, no DO blocks.

CREATE OR REPLACE VIEW mart_bet_performance AS
WITH picks AS (
    SELECT DISTINCT ON (bg_event_id, market_key, player_name, side, line)
        ev_hash,
        bg_event_id,
        sport_key,
        market_key,
        regexp_replace(market_key, '_alternate$', '') AS base_market_key,
        player_name,
        trim(regexp_replace(regexp_replace(lower(player_name), '[^a-z0-9 ]+', '', 'g'), '\s+', ' ', 'g')) AS normalized_name,
        side,
        line,
        soft_book,
        soft_price_decimal,
        fair_prob,
        edge_pct,
        stake_capped,
        commence_at_utc,
        fetched_at_utc
    FROM fact_ev_opportunity_history
    ORDER BY bg_event_id, market_key, player_name, side, line, fetched_at_utc DESC
),
graded AS (
    SELECT
        p.ev_hash,
        p.bg_event_id,
        p.sport_key,
        p.market_key,
        p.base_market_key,
        p.player_name,
        p.side,
        p.line,
        p.soft_book,
        p.soft_price_decimal,
        p.fair_prob,
        p.edge_pct,
        p.stake_capped,
        p.commence_at_utc,
        p.fetched_at_utc,
        psr.stat_value,
        psr.result_source,
        psr.settled_at_utc,
        CASE
            WHEN psr.stat_value IS NULL OR p.line IS NULL OR p.side IS NULL THEN 'pending'
            WHEN lower(p.side) = 'over'  AND psr.stat_value > p.line THEN 'win'
            WHEN lower(p.side) = 'over'  AND psr.stat_value < p.line THEN 'loss'
            WHEN lower(p.side) = 'over'  THEN 'push'
            WHEN lower(p.side) = 'under' AND psr.stat_value < p.line THEN 'win'
            WHEN lower(p.side) = 'under' AND psr.stat_value > p.line THEN 'loss'
            WHEN lower(p.side) = 'under' THEN 'push'
            ELSE 'pending'
        END AS outcome
    FROM picks p
    LEFT JOIN fact_player_stat_result psr
        ON psr.bg_event_id = p.bg_event_id
       AND psr.sport_key = p.sport_key
       AND psr.stat_key = p.base_market_key
       AND trim(regexp_replace(regexp_replace(lower(psr.player_name), '[^a-z0-9 ]+', '', 'g'), '\s+', ' ', 'g')) = p.normalized_name
)
SELECT
    graded.*,
    CASE outcome
        WHEN 'win'  THEN COALESCE(soft_price_decimal, 1) - 1
        WHEN 'loss' THEN -1
        WHEN 'push' THEN 0
        ELSE NULL
    END AS profit_units,
    CASE outcome
        WHEN 'win'  THEN COALESCE(stake_capped, 0) * (COALESCE(soft_price_decimal, 1) - 1)
        WHEN 'loss' THEN -COALESCE(stake_capped, 0)
        WHEN 'push' THEN 0
        ELSE NULL
    END AS profit_staked
FROM graded;

-- Rollup track record by sport + market.
CREATE OR REPLACE VIEW mart_bet_performance_summary AS
SELECT
    sport_key,
    base_market_key,
    count(*)                                            AS total_picks,
    count(*) FILTER (WHERE outcome = 'win')             AS wins,
    count(*) FILTER (WHERE outcome = 'loss')            AS losses,
    count(*) FILTER (WHERE outcome = 'push')            AS pushes,
    count(*) FILTER (WHERE outcome = 'pending')         AS pending,
    round(avg(edge_pct), 4)                             AS avg_edge_pct,
    round(sum(profit_units), 4)                         AS total_profit_units,
    CASE
        WHEN count(*) FILTER (WHERE outcome IN ('win', 'loss', 'push')) > 0
        THEN round(
            sum(profit_units)
            / count(*) FILTER (WHERE outcome IN ('win', 'loss', 'push')),
            4)
        ELSE NULL
    END                                                 AS roi_per_settled_bet
FROM mart_bet_performance
GROUP BY sport_key, base_market_key;
