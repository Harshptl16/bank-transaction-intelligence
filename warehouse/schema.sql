-- =====================================================================
-- Bank Transaction Intelligence Platform — Warehouse Schema
-- Target: Azure Database for PostgreSQL (Flexible Server)
-- Source: PaySim synthetic mobile money transaction dataset
-- =====================================================================
-- Design notes:
--   - Star schema, grain of fact_transactions = one row per PaySim
--     transaction record.
--   - dim_account holds customer accounts (PaySim "C..." IDs).
--   - dim_merchant holds merchant accounts (PaySim "M..." IDs).
--   - A transaction's recipient can be either a customer or a merchant,
--     so fact_transactions carries two nullable FK pairs
--     (dest_account_key / dest_merchant_key) — exactly one is populated
--     per row, enforced by a CHECK constraint below.
--   - dim_account is built as SCD Type 2 (region/status can change over
--     time) even though PaySim itself has no account attributes — those
--     attributes are synthesized during the ETL step specifically to
--     give the pipeline a real slowly-changing-dimension case to handle.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- dim_date
-- ---------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key        INTEGER PRIMARY KEY,        -- YYYYMMDDHH surrogate key
    full_timestamp  TIMESTAMP NOT NULL,          -- derived from PaySim "step"
    step             INTEGER NOT NULL,            -- raw PaySim step (1 step = 1 hour)
    calendar_date    DATE NOT NULL,
    hour_of_day      SMALLINT NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    day_of_week      SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7), -- 1=Mon
    day_name         VARCHAR(10) NOT NULL,
    week_of_month    SMALLINT NOT NULL,
    month_number     SMALLINT NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name       VARCHAR(10) NOT NULL,
    quarter          SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year             SMALLINT NOT NULL,
    is_weekend       BOOLEAN NOT NULL
);

CREATE INDEX idx_dim_date_calendar_date ON dim_date (calendar_date);
CREATE INDEX idx_dim_date_step ON dim_date (step);

-- ---------------------------------------------------------------------
-- dim_transaction_type
-- ---------------------------------------------------------------------
CREATE TABLE dim_transaction_type (
    transaction_type_key SERIAL PRIMARY KEY,
    type_code            VARCHAR(20) NOT NULL UNIQUE,   -- CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER
    channel_category      VARCHAR(20) NOT NULL,          -- e.g. 'ATM', 'Online', 'POS', 'Wire'
    description            VARCHAR(255)
);

-- Seed the five known PaySim transaction types
INSERT INTO dim_transaction_type (type_code, channel_category, description) VALUES
    ('CASH_IN',  'ATM',    'Customer deposits cash into account'),
    ('CASH_OUT', 'ATM',    'Customer withdraws cash from account'),
    ('DEBIT',    'POS',    'Point-of-sale debit transaction'),
    ('PAYMENT',  'Online', 'Payment made to a merchant'),
    ('TRANSFER', 'Wire',   'Funds transferred between accounts');

-- ---------------------------------------------------------------------
-- dim_account  (customer accounts — SCD Type 2)
-- ---------------------------------------------------------------------
CREATE TABLE dim_account (
    account_key       SERIAL PRIMARY KEY,             -- surrogate key
    account_id        VARCHAR(64) NOT NULL,            -- masked/hashed PaySim nameOrig/nameDest ("C..." IDs)
    region             VARCHAR(50),                     -- synthesized during ETL
    account_status     VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    effective_start    TIMESTAMP NOT NULL DEFAULT now(),
    effective_end      TIMESTAMP,                       -- NULL = current version
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (account_id, effective_start)
);

CREATE INDEX idx_dim_account_account_id ON dim_account (account_id);
CREATE INDEX idx_dim_account_current ON dim_account (account_id) WHERE is_current = TRUE;

-- ---------------------------------------------------------------------
-- dim_merchant  (merchant accounts)
-- ---------------------------------------------------------------------
CREATE TABLE dim_merchant (
    merchant_key      SERIAL PRIMARY KEY,
    merchant_id       VARCHAR(64) NOT NULL UNIQUE,     -- masked/hashed PaySim nameDest ("M..." IDs)
    merchant_category VARCHAR(50),                      -- synthesized during ETL
    region            VARCHAR(50)                       -- synthesized during ETL
);

CREATE INDEX idx_dim_merchant_merchant_id ON dim_merchant (merchant_id);

-- ---------------------------------------------------------------------
-- fact_transactions
-- ---------------------------------------------------------------------
CREATE TABLE fact_transactions (
    transaction_key       BIGSERIAL PRIMARY KEY,
    date_key              INTEGER NOT NULL REFERENCES dim_date (date_key),
    transaction_type_key  INTEGER NOT NULL REFERENCES dim_transaction_type (transaction_type_key),

    orig_account_key       INTEGER NOT NULL REFERENCES dim_account (account_key),

    dest_account_key        INTEGER REFERENCES dim_account (account_key),
    dest_merchant_key        INTEGER REFERENCES dim_merchant (merchant_key),

    amount                  NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),

    balance_before_orig      NUMERIC(18, 2),
    balance_after_orig        NUMERIC(18, 2),
    balance_before_dest        NUMERIC(18, 2),
    balance_after_dest          NUMERIC(18, 2),

    is_fraud                  BOOLEAN NOT NULL DEFAULT FALSE,
    is_flagged_by_system       BOOLEAN NOT NULL DEFAULT FALSE,

    source_step                INTEGER NOT NULL,        -- raw PaySim step, kept for traceability/debugging
    loaded_at                   TIMESTAMP NOT NULL DEFAULT now(),

    -- exactly one of dest_account_key / dest_merchant_key must be set
    CONSTRAINT chk_dest_exactly_one CHECK (
        (dest_account_key IS NOT NULL AND dest_merchant_key IS NULL)
        OR (dest_account_key IS NULL AND dest_merchant_key IS NOT NULL)
    )
);

-- Core query-pattern indexes (initial baseline — this is the set you'll
-- extend/compare against in the adaptive-indexing experiment, same as
-- the capstone approach applied to a schema you designed yourself)
CREATE INDEX idx_fact_txn_date ON fact_transactions (date_key);
CREATE INDEX idx_fact_txn_orig_account ON fact_transactions (orig_account_key);
CREATE INDEX idx_fact_txn_dest_account ON fact_transactions (dest_account_key) WHERE dest_account_key IS NOT NULL;
CREATE INDEX idx_fact_txn_dest_merchant ON fact_transactions (dest_merchant_key) WHERE dest_merchant_key IS NOT NULL;
CREATE INDEX idx_fact_txn_type ON fact_transactions (transaction_type_key);

-- Partial index tuned for fraud/anomaly-analysis queries specifically —
-- small, fast, and a good candidate to test in the adaptive-indexing
-- experiment matrix later (compare against a full index on is_fraud).
CREATE INDEX idx_fact_txn_flagged ON fact_transactions (transaction_key) WHERE is_fraud = TRUE;

-- Composite index for the account-activity-over-time query pattern
-- (e.g. "all transactions for account X in the last N days") —
-- one of the more common analytical query shapes in Phase 4.
CREATE INDEX idx_fact_txn_account_date ON fact_transactions (orig_account_key, date_key);

COMMIT;

-- =====================================================================
-- Optional: monthly range partitioning on fact_transactions
-- (apply this instead of the plain table above if the loaded dataset
-- is large enough to show a measurable partitioning effect — PaySim's
-- 30-day/744-step window means partitioning by day or by week may be
-- more meaningful than by month; adjust bounds accordingly once you've
-- looked at the row-count distribution.)
-- =====================================================================
-- CREATE TABLE fact_transactions (
--     ... same columns as above ...
-- ) PARTITION BY RANGE (date_key);
--
-- CREATE TABLE fact_transactions_week1 PARTITION OF fact_transactions
--     FOR VALUES FROM (2024010100) TO (2024010800);
-- ... etc., generated dynamically from dim_date once real dates are known
