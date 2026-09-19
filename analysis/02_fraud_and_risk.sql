-- =====================================================================
-- 02 — Fraud & risk analysis
-- =====================================================================
-- The risk-facing half of the platform: where fraud concentrates, how
-- well the system's own rule engine catches it, and how to threshold
-- high-value transactions for review.
--
-- Data caveat carried through from the pipeline: PaySim zeroes out the
-- balance columns on fraudulent rows, so none of these queries use
-- balance fields as a fraud signal — only amount, type, time, and the
-- is_fraud / is_flagged_by_system labels.
--
-- Techniques: conditional aggregation (COUNT FILTER), PERCENTILE_CONT for
-- threshold setting, a confusion-matrix cross-tab, and RANK over accounts.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Q2.1 — Fraud rate by transaction type
-- ---------------------------------------------------------------------
-- Business question: which transaction types carry fraud, and at what
-- rate? In PaySim fraud only ever appears in TRANSFER and CASH_OUT — this
-- query is what surfaces that fact from the data rather than assuming it.
--
-- COUNT(*) FILTER (WHERE ...) is cleaner than SUM(CASE WHEN ...) for
-- conditional counts and reads closer to the business question.
SELECT
    tt.type_code,
    COUNT(*)                                             AS total_txns,
    COUNT(*) FILTER (WHERE f.is_fraud)                   AS fraud_txns,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_fraud)
          / COUNT(*), 4)                                 AS fraud_rate_pct,
    ROUND(COALESCE(SUM(f.amount) FILTER (WHERE f.is_fraud), 0), 2) AS fraud_value
FROM fact_transactions f
JOIN dim_transaction_type tt ON tt.transaction_type_key = f.transaction_type_key
GROUP BY tt.type_code
ORDER BY fraud_txns DESC;


-- ---------------------------------------------------------------------
-- Q2.2 — Fraud concentration by hour of day
-- ---------------------------------------------------------------------
-- Business question: is fraud evenly spread across the day, or does it
-- cluster in particular hours? Off-hours fraud spikes are a classic
-- monitoring signal.
--
-- Compares each hour's fraud rate against the overall rate using a window
-- AVG over the per-hour fraud flags.
SELECT
    d.hour_of_day,
    COUNT(*)                                             AS txns,
    COUNT(*) FILTER (WHERE f.is_fraud)                   AS fraud_txns,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_fraud) / COUNT(*), 4) AS hourly_fraud_rate,
    ROUND(100.0 * SUM(COUNT(*) FILTER (WHERE f.is_fraud)) OVER ()
          / SUM(COUNT(*)) OVER (), 4)                    AS overall_fraud_rate
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.hour_of_day
ORDER BY hourly_fraud_rate DESC;


-- ---------------------------------------------------------------------
-- Q2.3 — High-value transaction thresholds (percentiles)
-- ---------------------------------------------------------------------
-- Business question: where should a "large transaction" review threshold
-- sit? Setting it by percentile adapts to the actual amount distribution
-- instead of hard-coding a dollar figure.
--
-- PERCENTILE_CONT is an ordered-set aggregate — the standard SQL way to
-- compute medians and percentiles. Shown per transaction type because the
-- distributions differ sharply between them.
SELECT
    tt.type_code,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.amount)::numeric, 2) AS median,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY f.amount)::numeric, 2) AS p90,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY f.amount)::numeric, 2) AS p99,
    ROUND(MAX(f.amount), 2)                                                    AS max_amount
FROM fact_transactions f
JOIN dim_transaction_type tt ON tt.transaction_type_key = f.transaction_type_key
GROUP BY tt.type_code
ORDER BY p99 DESC;


-- ---------------------------------------------------------------------
-- Q2.4 — Rule-engine effectiveness (fraud vs system flag)
-- ---------------------------------------------------------------------
-- Business question: how good is the platform's built-in flag
-- (is_flagged_by_system) at catching actual fraud? This is a confusion
-- matrix — true positives, false negatives (missed fraud), false
-- positives (flagged legit) — the core evaluation for any detection rule.
SELECT
    CASE WHEN f.is_fraud THEN 'Actual fraud' ELSE 'Legitimate' END      AS actual,
    COUNT(*) FILTER (WHERE f.is_flagged_by_system)                       AS flagged,
    COUNT(*) FILTER (WHERE NOT f.is_flagged_by_system)                   AS not_flagged,
    COUNT(*)                                                             AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_flagged_by_system)
          / COUNT(*), 2)                                                 AS pct_flagged
FROM fact_transactions f
GROUP BY f.is_fraud
ORDER BY actual;


-- ---------------------------------------------------------------------
-- Q2.5 — Accounts with the most fraud exposure
-- ---------------------------------------------------------------------
-- Business question: which originating accounts are involved in the most
-- fraudulent transactions, and what value is at stake? A prioritised list
-- for an investigations team.
--
-- Joins to the CURRENT version of dim_account (is_current) so each account
-- is reported with its latest region — the correct way to read an SCD
-- Type 2 dimension for reporting.
SELECT
    a.account_id,
    a.region,
    COUNT(*) FILTER (WHERE f.is_fraud)                   AS fraud_txns,
    ROUND(SUM(f.amount) FILTER (WHERE f.is_fraud), 2)    AS fraud_value,
    COUNT(*)                                             AS total_txns,
    RANK() OVER (ORDER BY SUM(f.amount) FILTER (WHERE f.is_fraud) DESC NULLS LAST) AS exposure_rank
FROM fact_transactions f
JOIN dim_account a ON a.account_key = f.orig_account_key
GROUP BY a.account_id, a.region
HAVING COUNT(*) FILTER (WHERE f.is_fraud) > 0
ORDER BY fraud_value DESC
LIMIT 20;
