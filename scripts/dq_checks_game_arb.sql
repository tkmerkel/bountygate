-- dq_checks_game_arb.sql
-- One-off spot-check queries for the 48-hour shake-out of bg_game_arb_pipeline.
-- Run these manually against the production DB during the first soak period.

-- Q1. Row counts by market_key. Sanity: all three should appear (h2h, spreads, totals).
SELECT market_key, COUNT(*) AS opp_count
FROM bg_arbitrage_game_opportunities
GROUP BY market_key
ORDER BY market_key;

-- Q2. Books represented in pairings. Sanity: all four configured books should
-- appear on each side. Pairs where one book dominates the alert feed may
-- indicate a stale price or a books-coverage gap.
SELECT
    leg_a_book,
    leg_b_book,
    COUNT(*) AS pair_count,
    ROUND(AVG(roi) * 100, 3) AS avg_roi_pct,
    ROUND(MAX(roi) * 100, 3) AS max_roi_pct
FROM bg_arbitrage_game_opportunities
GROUP BY leg_a_book, leg_b_book
ORDER BY pair_count DESC;

-- Q3. ROI distribution. Anything > 5% is suspicious (stale line or input error).
SELECT
    CASE
        WHEN roi < 0.005             THEN '0  - 0.5%'
        WHEN roi < 0.0075            THEN '0.5- 0.75%'
        WHEN roi < 0.015             THEN '0.75-1.5%'
        WHEN roi < 0.03              THEN '1.5- 3.0%'
        WHEN roi < 0.05              THEN '3.0- 5.0%'
        ELSE                              '5.0%+  (verify by hand)'
    END AS roi_bucket,
    COUNT(*) AS opp_count
FROM bg_arbitrage_game_opportunities
GROUP BY 1
ORDER BY 1;

-- Q4. Null/zero scan. None of these should appear in a healthy run.
SELECT
    SUM(CASE WHEN payout <= 0           THEN 1 ELSE 0 END) AS zero_payout,
    SUM(CASE WHEN wager_leg_a <= 0      THEN 1 ELSE 0 END) AS zero_wager_a,
    SUM(CASE WHEN wager_leg_b <= 0      THEN 1 ELSE 0 END) AS zero_wager_b,
    SUM(CASE WHEN leg_a_book = leg_b_book THEN 1 ELSE 0 END) AS same_book_pairs,
    SUM(CASE WHEN leg_a_book IS NULL OR leg_b_book IS NULL THEN 1 ELSE 0 END) AS null_books
FROM bg_arbitrage_game_opportunities;

-- Q5. History growth rate. Used to size the table over time and confirm the
-- append-only path is working.
SELECT
    DATE(first_seen_at_utc) AS day,
    COUNT(*)                AS new_opps_first_seen,
    ROUND(AVG(roi) * 100, 3) AS avg_roi_pct
FROM bg_arb_game_opportunities_history
WHERE first_seen_at_utc >= now() AT TIME ZONE 'utc' - INTERVAL '14 days'
GROUP BY 1
ORDER BY 1 DESC;

-- Q6. Recency. Anything older than ~15min in the live opps table is stale
-- (DAG runs every 10min; allow one missed run).
SELECT
    MIN(fetched_at_utc)                                              AS oldest_row,
    MAX(fetched_at_utc)                                              AS newest_row,
    EXTRACT(EPOCH FROM (now() AT TIME ZONE 'utc' - MAX(fetched_at_utc))) / 60.0 AS age_minutes
FROM bg_arbitrage_game_opportunities;

-- Q7. Mirrored-spread sanity. Every spreads row should have leg_a_point = -leg_b_point.
SELECT COUNT(*) AS spreads_with_unmirrored_points
FROM bg_arbitrage_game_opportunities
WHERE market_key = 'spreads'
  AND leg_a_point != -leg_b_point;

-- Q8. Totals same-point sanity. Every totals row should have leg_a_point = leg_b_point.
SELECT COUNT(*) AS totals_with_mismatched_points
FROM bg_arbitrage_game_opportunities
WHERE market_key = 'totals'
  AND leg_a_point != leg_b_point;

-- Q9. h2h null-point sanity. Every h2h row should have NULL on both points.
SELECT COUNT(*) AS h2h_with_non_null_points
FROM bg_arbitrage_game_opportunities
WHERE market_key = 'h2h'
  AND (leg_a_point IS NOT NULL OR leg_b_point IS NOT NULL);
