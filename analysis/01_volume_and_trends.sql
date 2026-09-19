-- =====================================================================
-- 01 — Volume & trend analysis
-- =====================================================================
-- Operational reporting: how much is flowing through the platform, when,
-- and how that changes over time. These are the queries a data analyst
-- runs first to understand the shape of the business before digging into
-- anything specific.
--
-- Techniques on show: date-dimension joins, LAG() for period-over-period
-- change, a windowed moving average, and share-of-total with a window
-- SUM. Run each query block on its own.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Q1.1 — Daily transaction volume and value, with day-over-day change
-- ---------------------------------------------------------------------
-- Business question: how does daily activity trend across the month, and
-- which days jump or drop versus the day before?
--
-- LAG() reaches back one row (one day) to compute the delta without a
-- self-join. This is the single most common "trend" pattern in analytics.
WITH daily AS (
    SELECT
        d.calendar_date,
        COUNT(*)                        AS txn_count,
        ROUND(SUM(f.amount), 2)         AS total_value
    FROM fact_transactions f
    JOIN dim_date d ON d.date_key = f.date_key
    GROUP BY d.calendar_date
)
SELECT
    calendar_date,
    txn_count,
    total_value,
    txn_count - LAG(txn_count) OVER (ORDER BY calendar_date)          AS count_change,
    ROUND(
        100.0 * (txn_count - LAG(txn_count) OVER (ORDER BY calendar_date))
        / NULLIF(LAG(txn_count) OVER (ORDER BY calendar_date), 0)
    , 1)                                                              AS count_pct_change
FROM daily
ORDER BY calendar_date;


-- ---------------------------------------------------------------------
-- Q1.2 — 7-day moving average of transaction volume
-- ---------------------------------------------------------------------
-- Business question: stripping out day-to-day noise, is volume trending
-- up or down?
--
-- A framed window (ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) computes the
-- trailing 7-day average — the standard way to smooth a noisy daily series.
WITH daily AS (
    SELECT d.calendar_date, COUNT(*) AS txn_count
    FROM fact_transactions f
    JOIN dim_date d ON d.date_key = f.date_key
    GROUP BY d.calendar_date
)
SELECT
    calendar_date,
    txn_count,
    ROUND(AVG(txn_count) OVER (
        ORDER BY calendar_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1) AS moving_avg_7d
FROM daily
ORDER BY calendar_date;


-- ---------------------------------------------------------------------
-- Q1.3 — Transaction mix by type and channel
-- ---------------------------------------------------------------------
-- Business question: what share of volume and value does each transaction
-- type carry, and through which channel?
--
-- A window SUM with no ORDER BY gives the grand total on every row, so
-- each type's share is one division — no subquery for the denominator.
SELECT
    tt.type_code,
    tt.channel_category,
    COUNT(*)                                                    AS txn_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)          AS pct_of_txns,
    ROUND(SUM(f.amount), 2)                                     AS total_value,
    ROUND(100.0 * SUM(f.amount) / SUM(SUM(f.amount)) OVER (), 1) AS pct_of_value,
    ROUND(AVG(f.amount), 2)                                     AS avg_txn_value
FROM fact_transactions f
JOIN dim_transaction_type tt ON tt.transaction_type_key = f.transaction_type_key
GROUP BY tt.type_code, tt.channel_category
ORDER BY total_value DESC;


-- ---------------------------------------------------------------------
-- Q1.4 — Hourly activity pattern (peak hours)
-- ---------------------------------------------------------------------
-- Business question: what are the platform's busy and quiet hours? Feeds
-- capacity planning and staffing, and gives fraud analysis a baseline for
-- what "normal" hourly volume looks like.
SELECT
    d.hour_of_day,
    COUNT(*)                                                   AS txn_count,
    ROUND(AVG(f.amount), 2)                                    AS avg_value,
    RANK() OVER (ORDER BY COUNT(*) DESC)                       AS busiest_rank
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.hour_of_day
ORDER BY d.hour_of_day;


-- ---------------------------------------------------------------------
-- Q1.5 — Weekday vs weekend comparison
-- ---------------------------------------------------------------------
-- Business question: does behaviour differ between weekdays and weekends,
-- in both volume and average ticket size?
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END   AS day_type,
    COUNT(*)                                                   AS txn_count,
    COUNT(DISTINCT d.calendar_date)                           AS num_days,
    ROUND(COUNT(*)::numeric / COUNT(DISTINCT d.calendar_date), 0) AS avg_txns_per_day,
    ROUND(AVG(f.amount), 2)                                    AS avg_txn_value
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.is_weekend
ORDER BY day_type;
