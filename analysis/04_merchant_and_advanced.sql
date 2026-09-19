-- =====================================================================
-- 04 — Merchant analysis & advanced window functions
-- =====================================================================
-- Merchant-side spending patterns plus a set of queries built specifically
-- to demonstrate the window-function toolkit interviewers probe for:
-- running cumulative totals, partitioned ranking, and dense ranking within
-- a group.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Q4.1 — Spending by merchant category
-- ---------------------------------------------------------------------
-- Business question: where do customers spend? Only PAYMENT transactions
-- go to merchants, so this is scoped to merchant destinations.
SELECT
    m.merchant_category,
    COUNT(*)                                                   AS payments,
    COUNT(DISTINCT m.merchant_key)                             AS merchants,
    ROUND(SUM(f.amount), 2)                                    AS total_spend,
    ROUND(AVG(f.amount), 2)                                    AS avg_payment,
    ROUND(100.0 * SUM(f.amount) / SUM(SUM(f.amount)) OVER (), 1) AS pct_of_spend
FROM fact_transactions f
JOIN dim_merchant m ON m.merchant_key = f.dest_merchant_key
GROUP BY m.merchant_category
ORDER BY total_spend DESC;


-- ---------------------------------------------------------------------
-- Q4.2 — Running cumulative transaction value over the month
-- ---------------------------------------------------------------------
-- Business question: how does cumulative value build up across the month?
-- The running total is what feeds a "value-to-date" line on a dashboard.
--
-- SUM(...) OVER (ORDER BY date) with the default running frame gives the
-- cumulative total; a separate window adds each day's share of the
-- month-end total.
WITH daily AS (
    SELECT d.calendar_date, SUM(f.amount) AS daily_value
    FROM fact_transactions f
    JOIN dim_date d ON d.date_key = f.date_key
    GROUP BY d.calendar_date
)
SELECT
    calendar_date,
    ROUND(daily_value, 2)                                         AS daily_value,
    ROUND(SUM(daily_value) OVER (ORDER BY calendar_date), 2)      AS cumulative_value,
    ROUND(100.0 * SUM(daily_value) OVER (ORDER BY calendar_date)
          / SUM(daily_value) OVER (), 1)                          AS pct_of_month_to_date
FROM daily
ORDER BY calendar_date;


-- ---------------------------------------------------------------------
-- Q4.3 — Top accounts within each region (partitioned ranking)
-- ---------------------------------------------------------------------
-- Business question: who are the top 3 accounts by value in each region?
-- A regional leaderboard, not a global one.
--
-- RANK() PARTITION BY region restarts the ranking for each region — the
-- defining use of a partitioned window function.
WITH account_region AS (
    SELECT
        a.region,
        a.account_id,
        SUM(f.amount) AS total_value,
        COUNT(*)      AS txns
    FROM fact_transactions f
    JOIN dim_account a ON a.account_key = f.orig_account_key
    GROUP BY a.region, a.account_id
),
ranked AS (
    SELECT
        region,
        account_id,
        ROUND(total_value, 2) AS total_value,
        txns,
        RANK() OVER (PARTITION BY region ORDER BY total_value DESC) AS rank_in_region
    FROM account_region
)
SELECT region, rank_in_region, account_id, total_value, txns
FROM ranked
WHERE rank_in_region <= 3
ORDER BY region, rank_in_region;


-- ---------------------------------------------------------------------
-- Q4.4 — Busiest day per transaction type (dense ranking within group)
-- ---------------------------------------------------------------------
-- Business question: for each transaction type, which calendar day saw
-- the most activity?
--
-- DENSE_RANK() over (partition by type, order by count desc) picks each
-- type's peak day, with no rank gaps on ties.
WITH type_day AS (
    SELECT
        tt.type_code,
        d.calendar_date,
        COUNT(*) AS txns
    FROM fact_transactions f
    JOIN dim_transaction_type tt ON tt.transaction_type_key = f.transaction_type_key
    JOIN dim_date d              ON d.date_key = f.date_key
    GROUP BY tt.type_code, d.calendar_date
),
ranked AS (
    SELECT
        type_code,
        calendar_date,
        txns,
        DENSE_RANK() OVER (PARTITION BY type_code ORDER BY txns DESC) AS day_rank
    FROM type_day
)
SELECT type_code, calendar_date AS busiest_day, txns
FROM ranked
WHERE day_rank = 1
ORDER BY txns DESC;
