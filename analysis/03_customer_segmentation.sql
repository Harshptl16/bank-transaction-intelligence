-- =====================================================================
-- 03 — Customer segmentation & account behaviour
-- =====================================================================
-- Moving from "what happened" to "who did it": grouping accounts by
-- behaviour so the business can treat high-value, high-frequency, and
-- dormant customers differently.
--
-- Techniques: an RFM-style segmentation with NTILE deciles, share-of-total
-- windows, and the ROW_NUMBER "top-N-per-group" pattern.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Q3.1 — RFM-style account segmentation
-- ---------------------------------------------------------------------
-- Business question: how do we group accounts by Recency (how recently
-- active), Frequency (how many transactions), and Monetary value (total
-- amount)? RFM is the standard customer-value segmentation.
--
-- NTILE(5) splits accounts into five equal-sized buckets on each measure;
-- recency is measured as days since the account's last transaction,
-- relative to the most recent date in the dataset.
WITH bounds AS (
    SELECT MAX(calendar_date) AS as_of FROM dim_date
),
account_rfm AS (
    SELECT
        a.account_id,
        a.region,
        (SELECT as_of FROM bounds) - MAX(d.calendar_date) AS recency_days,
        COUNT(*)                                          AS frequency,
        ROUND(SUM(f.amount), 2)                           AS monetary
    FROM fact_transactions f
    JOIN dim_account a ON a.account_key = f.orig_account_key
    JOIN dim_date d    ON d.date_key    = f.date_key
    GROUP BY a.account_id, a.region
)
SELECT
    account_id,
    region,
    recency_days,
    frequency,
    monetary,
    -- lower recency_days = more recent = higher score, hence DESC on the reverse
    NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
    NTILE(5) OVER (ORDER BY frequency)          AS f_score,
    NTILE(5) OVER (ORDER BY monetary)           AS m_score
FROM account_rfm
ORDER BY monetary DESC
LIMIT 25;


-- ---------------------------------------------------------------------
-- Q3.2 — Account value deciles (where the value concentrates)
-- ---------------------------------------------------------------------
-- Business question: how concentrated is transaction value across
-- accounts? (The classic "top 10% of customers drive X% of value.")
--
-- NTILE(10) buckets accounts into deciles by total value; the window SUM
-- gives each decile's share of the grand total.
WITH account_value AS (
    SELECT
        f.orig_account_key,
        SUM(f.amount) AS total_value,
        COUNT(*)      AS txn_count
    FROM fact_transactions f
    GROUP BY f.orig_account_key
),
deciled AS (
    SELECT
        NTILE(10) OVER (ORDER BY total_value DESC) AS value_decile,
        total_value,
        txn_count
    FROM account_value
)
SELECT
    value_decile,
    COUNT(*)                                                       AS accounts,
    ROUND(SUM(total_value), 2)                                     AS decile_value,
    ROUND(100.0 * SUM(total_value) / SUM(SUM(total_value)) OVER (), 1) AS pct_of_total_value,
    ROUND(AVG(txn_count), 1)                                       AS avg_txns_per_account
FROM deciled
GROUP BY value_decile
ORDER BY value_decile;


-- ---------------------------------------------------------------------
-- Q3.3 — Transaction distribution by region
-- ---------------------------------------------------------------------
-- Business question: how does activity split across regions, by both
-- account base and transaction value?
SELECT
    a.region,
    COUNT(DISTINCT a.account_key)                              AS accounts,
    COUNT(*)                                                   AS txns,
    ROUND(SUM(f.amount), 2)                                    AS total_value,
    ROUND(SUM(f.amount) / COUNT(DISTINCT a.account_key), 2)    AS value_per_account,
    ROUND(100.0 * SUM(f.amount) / SUM(SUM(f.amount)) OVER (), 1) AS pct_of_value
FROM fact_transactions f
JOIN dim_account a ON a.account_key = f.orig_account_key
GROUP BY a.region
ORDER BY total_value DESC;


-- ---------------------------------------------------------------------
-- Q3.4 — Largest transaction per account (top-N-per-group)
-- ---------------------------------------------------------------------
-- Business question: what is each account's single biggest transaction,
-- and which accounts have the largest "biggest transactions" overall?
--
-- ROW_NUMBER() PARTITION BY account, ORDER BY amount DESC, then filter to
-- rn = 1 — the canonical way to pick one row per group in SQL.
WITH ranked AS (
    SELECT
        a.account_id,
        a.region,
        f.amount,
        tt.type_code,
        ROW_NUMBER() OVER (
            PARTITION BY f.orig_account_key
            ORDER BY f.amount DESC
        ) AS rn
    FROM fact_transactions f
    JOIN dim_account a           ON a.account_key = f.orig_account_key
    JOIN dim_transaction_type tt ON tt.transaction_type_key = f.transaction_type_key
)
SELECT account_id, region, type_code, ROUND(amount, 2) AS largest_txn
FROM ranked
WHERE rn = 1
ORDER BY largest_txn DESC
LIMIT 20;
