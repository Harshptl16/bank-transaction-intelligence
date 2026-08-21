# Bank Transaction Intelligence Platform

End-to-end data platform: cloud ingestion → distributed ETL with PII masking → dimensional warehouse on Azure → SQL analysis and BI dashboards.

Built on the PaySim synthetic financial transaction dataset.

---

## Status

| Phase | Status |
|---|---|
| 0 — Setup & sample data | Done |
| 1 — PySpark ETL (clean, mask, dimension build) | Done |
| 2 — Azure infrastructure | Not started |
| 3 — Warehouse loading + SCD Type 2 | Schema done, loader pending |
| 4 — Analytical SQL | Not started |
| 5 — Dashboards | Not started |
| 6 — Findings write-up | Not started |

---

## Architecture

```
PaySim CSV
    │
    ▼
Azure Blob Storage  (raw / processed containers)
    │
    ▼
PySpark transform   — clean, mask PII, derive dimensions
    │
    ▼
Azure Database for PostgreSQL   (star schema)
    │
    ├──────────────┐
    ▼              ▼
Analytical SQL   Power BI
```

### Warehouse schema

Star schema, grain of `fact_transactions` = one row per transaction.

- `fact_transactions` — amounts, balances, fraud flags, FKs to all dimensions
- `dim_account` — customer accounts, **SCD Type 2**
- `dim_merchant` — merchant accounts, kept separate from customer accounts
- `dim_date` — derived from PaySim's integer `step` column
- `dim_transaction_type` — the five PaySim transaction types

Full DDL and design rationale: [`warehouse/schema.sql`](warehouse/schema.sql)

---

## Quick start

```bash
# 1. Install
pip install -r requirements.txt

# 2. Configure
cp .env.example .env      # then edit .env
export MASKING_SALT="your-secret-salt"

# 3. Generate sample data (or download real PaySim from Kaggle)
python etl/generate_sample.py --rows 100000 --out data/raw/paysim_sample.csv

# 4. Run the transform
python etl/transform.py \
    --input data/raw/paysim_sample.csv \
    --output data/processed
```

Output: four Parquet datasets in `data/processed/` ready for warehouse loading.

### Running against the real dataset

The generator produces the exact same schema as real PaySim, so no code changes are needed:

```bash
python etl/transform.py \
    --input data/raw/PS_20174392719_1491204439457_log.csv \
    --output data/processed \
    --shuffle-partitions 200        # raise for the full 6.3M rows
```

---

## Repo layout

```
├── etl/
│   ├── generate_sample.py    # PaySim-shaped test data generator
│   └── transform.py          # PySpark: clean, mask, build dimensions
├── warehouse/
│   └── schema.sql            # star schema DDL
├── analysis/                 # analytical SQL (Phase 4)
├── dashboards/               # Power BI files (Phase 5)
├── infra/                    # Azure provisioning (Phase 2)
└── docs/
    └── PROJECT_OVERVIEW.md   # full design document
```

---

## Design decisions

**Masking with salted SHA-256, not tokenization.** Hashing is deterministic, so the same account maps to the same ID across pipeline runs — which is required for dimension dedup and SCD to work at all. It needs no token vault. The tradeoff is irreversibility, which is acceptable since no downstream use case needs the original IDs.

**Natural keys in the fact staging table, not surrogate keys.** Surrogate keys are assigned by Postgres `SERIAL` columns. Generating them in Spark would mean Spark and Postgres could disagree across re-runs, breaking idempotency. The loader resolves natural → surrogate keys after dimensions are loaded.

**Dual destination FKs.** A transaction's recipient can be a customer or a merchant. `fact_transactions` carries both `dest_account_key` and `dest_merchant_key` with a `CHECK` constraint enforcing exactly one is populated. Clean for querying either side; the cost is that "all counterparties" queries need a `COALESCE` or `UNION`.

**Partition on `calendar_date`, not `date_key`.** Partitioning on `date_key` (YYYYMMDDHH) creates one directory per hour — 744 for PaySim's window. On a 20k-row test that produced 5,733 files averaging 4 KB, inflating the dataset to 64 MB of mostly Parquet metadata. Switching to daily partitions with an explicit `repartition()` brought this to 31 files and 2.2 MB. See [small files problem](#).

**Explicit schema on read, never inference.** Inference costs an extra full pass over the data and can silently change column types between runs if the data shifts.

**Synthetic account attributes are deterministic.** PaySim carries no account attributes, so `region` and `merchant_category` are derived from the hashed ID rather than randomly assigned. Random assignment would make every pipeline run look like an SCD change, and `dim_account` would grow without bound.

---

## Dataset notes

PaySim simulates mobile money transactions from one month of real financial logs. Two quirks the pipeline handles explicitly:

1. **Balance columns are zeroed on fraudulent rows.** This is documented dataset behaviour, not corruption. `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, and `newbalanceDest` cannot be used as fraud signal. The ETL preserves them rather than discarding, so downstream analysis can decide how to treat them.

2. **Fraud only occurs on `TRANSFER` and `CASH_OUT`.** Roughly 0.13% of rows — severe class imbalance that any modelling work would need to account for.

---

## Verified results (20k-row sample)

```
[clean]    20,000 rows in -> 20,000 rows out (0 dropped)
[build]    dim_account:  2,500 rows
[build]    dim_merchant:   500 rows
[build]    dim_date:       744 rows
[build]    fact:        20,000 rows
[validate] all checks passed
[write]    31 files, 2.2 MB
```

Validation confirms: destination key exclusivity holds on every row, no orphan account IDs, no negative amounts, all masked IDs unique at 32 hex characters.
