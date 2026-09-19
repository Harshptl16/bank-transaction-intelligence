"""
transform.py
------------
PySpark transformation stage of the Bank Transaction Intelligence Platform.

INPUT:  Raw PaySim CSV (local path or Azure Blob Storage wasbs:// URI)
OUTPUT: Four Parquet datasets ready for warehouse loading:
            - dim_account      (customer accounts, natural key + attributes)
            - dim_merchant     (merchant accounts)
            - dim_date         (derived from PaySim 'step')
            - fact_transactions_staging  (natural keys, resolved to surrogate
                                          keys later in load.py)

WHY NATURAL KEYS HERE, NOT SURROGATE KEYS:
    Surrogate keys are assigned by Postgres (SERIAL columns). Generating them
    in Spark would mean Spark and Postgres could disagree on key assignment
    across re-runs — breaking idempotency. Instead the fact staging table
    carries the masked natural keys (account_id / merchant_id), and load.py
    resolves them against the dimension tables after those are loaded.
    This is the standard pattern and it's what makes re-runs safe.

MASKING:
    Account and merchant identifiers are hashed with SHA-256 plus a salt
    before they ever reach the warehouse. The salt comes from an environment
    variable, never from source control.

    Hashing (not tokenization) is the right choice here because:
      - it is deterministic, so the same account maps to the same ID across
        pipeline runs, which is required for dimension dedup and SCD to work
      - it is one-way, so the warehouse never holds recoverable identifiers
      - it needs no separate token vault to maintain
    The tradeoff: hashed values cannot be reversed back to the original ID.
    That is acceptable here because no downstream use case requires it.

Usage:
    export MASKING_SALT="your-secret-salt"
    python etl/transform.py \
        --input data/raw/paysim_sample.csv \
        --output data/processed
"""

import argparse
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# PaySim's simulated month is mapped onto a real calendar month so that
# dim_date carries genuine weekday/weekend semantics. Step 1 = hour 0 of
# this date. Any date works; this one is chosen so step 1 lands on a Monday.
SIMULATION_START = "2024-01-01"

# Synthetic attribute pools. PaySim carries no account attributes at all,
# so region / merchant_category are generated deterministically from the
# hashed ID. This is intentional: it gives dim_account real attributes that
# can change over time, which is what makes the SCD Type 2 exercise in
# load.py meaningful rather than theoretical.
REGIONS = ["Ontario", "Quebec", "British Columbia", "Alberta", "Atlantic", "Prairies"]
MERCHANT_CATEGORIES = [
    "Grocery", "Fuel", "Retail", "Restaurant", "Utilities",
    "Travel", "Healthcare", "Entertainment",
]

# Explicit schema — never infer schema on a production read. Inference
# requires an extra full pass over the data and can silently change types
# between runs if the data shifts.
PAYSIM_SCHEMA = StructType([
    StructField("step", IntegerType(), nullable=False),
    StructField("type", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("nameOrig", StringType(), nullable=False),
    StructField("oldbalanceOrg", DoubleType(), nullable=True),
    StructField("newbalanceOrig", DoubleType(), nullable=True),
    StructField("nameDest", StringType(), nullable=False),
    StructField("oldbalanceDest", DoubleType(), nullable=True),
    StructField("newbalanceDest", DoubleType(), nullable=True),
    StructField("isFraud", IntegerType(), nullable=False),
    StructField("isFlaggedFraud", IntegerType(), nullable=False),
])


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark(app_name: str = "paysim-transform",
                shuffle_partitions: int = 8) -> SparkSession:
    """
    shuffle_partitions defaults to Spark's 200, which is tuned for cluster
    workloads. On a laptop or a small Databricks node that means 200 tiny
    tasks per shuffle and the scheduling overhead dominates the actual work.
    8 is a sensible local default; raise it toward 200 when running the full
    6.3M-row dataset on a real cluster.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Adaptive Query Execution: lets Spark re-optimise shuffle partitions
        # at runtime based on actual data size. Meaningful win on the skewed
        # account distribution in this dataset.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Masking
# ---------------------------------------------------------------------------

def mask_column(col_name: str, salt: str):
    """
    Returns a Column expression that SHA-256 hashes an identifier with a salt.

    Truncated to 32 hex chars — full SHA-256 is 64 chars, which doubles the
    storage cost on a column that appears in every fact row and every
    dimension row. 32 hex chars (128 bits) leaves collision probability
    negligible at this cardinality.
    """
    return F.substring(F.sha2(F.concat(F.lit(salt), F.col(col_name)), 256), 1, 32)


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------

def clean(df: DataFrame) -> DataFrame:
    """
    Validation and cleaning pass.

    Rows are dropped only when they cannot be meaningfully loaded (missing
    identifiers, negative amounts, out-of-range step). Everything else is
    kept — including the zeroed balances on fraud rows, which are a
    documented dataset quirk rather than a data error, and are flagged
    rather than discarded so downstream analysis can decide what to do.
    """
    initial = df.count()

    cleaned = (
        df
        .filter(F.col("nameOrig").isNotNull() & (F.trim(F.col("nameOrig")) != ""))
        .filter(F.col("nameDest").isNotNull() & (F.trim(F.col("nameDest")) != ""))
        .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
        .filter(F.col("step").between(1, 744))
        .filter(F.col("type").isin("PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"))
        .dropDuplicates()
    )

    remaining = cleaned.count()
    print(f"[clean] {initial:,} rows in -> {remaining:,} rows out "
          f"({initial - remaining:,} dropped)")
    return cleaned


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

def enrich(df: DataFrame, salt: str) -> DataFrame:
    """
    Masks identifiers, derives timestamps from PaySim's integer 'step',
    and classifies each destination as customer or merchant.
    """
    return (
        df
        # --- masked identifiers -------------------------------------------
        .withColumn("orig_account_id", mask_column("nameOrig", salt))
        .withColumn("dest_id_masked", mask_column("nameDest", salt))

        # --- destination classification ------------------------------------
        # PaySim prefixes merchant accounts with 'M' and customers with 'C'.
        # This must be read from the RAW id, before masking destroys the prefix.
        .withColumn("dest_is_merchant", F.col("nameDest").startswith("M"))

        # --- time derivation ------------------------------------------------
        # step 1 = hour 0 of SIMULATION_START, so offset is (step - 1) hours.
        .withColumn(
            "full_timestamp",
            F.expr(f"timestamp('{SIMULATION_START} 00:00:00') + make_interval(0,0,0,0, step - 1, 0, 0)")
        )
        .withColumn("calendar_date", F.to_date("full_timestamp"))
        .withColumn("hour_of_day", F.hour("full_timestamp"))
        # date_key is YYYYMMDDHH as an integer — matches the dim_date PK
        .withColumn(
            "date_key",
            (F.date_format("full_timestamp", "yyyyMMdd").cast("long") * 100
             + F.col("hour_of_day")).cast("long")
        )

        # --- type normalisation ---------------------------------------------
        # Real PaySim uses CASH_IN / CASH_OUT with underscores in some
        # distributions and CASH-IN / CASH-OUT with hyphens in others.
        # Normalise so the join against dim_transaction_type never silently
        # drops rows.
        .withColumn("type_code", F.regexp_replace(F.upper(F.col("type")), "-", "_"))

        .drop("nameOrig", "nameDest", "type")
    )


# ---------------------------------------------------------------------------
# Synthetic attributes
# ---------------------------------------------------------------------------

def assign_region(id_col: str):
    """
    Deterministically assigns a region from a hashed ID.

    Deterministic (not random) so that re-running the pipeline produces the
    same assignment — otherwise every run would look like an SCD change and
    dim_account would grow without bound.
    """
    bucket = F.conv(F.substring(F.col(id_col), 1, 4), 16, 10).cast("int") % len(REGIONS)
    expr = F.when(bucket == 0, F.lit(REGIONS[0]))
    for i in range(1, len(REGIONS)):
        expr = expr.when(bucket == i, F.lit(REGIONS[i]))
    return expr.otherwise(F.lit(REGIONS[0]))


def assign_merchant_category(id_col: str):
    """Same deterministic approach, for merchant categories."""
    bucket = F.conv(F.substring(F.col(id_col), 5, 4), 16, 10).cast("int") % len(MERCHANT_CATEGORIES)
    expr = F.when(bucket == 0, F.lit(MERCHANT_CATEGORIES[0]))
    for i in range(1, len(MERCHANT_CATEGORIES)):
        expr = expr.when(bucket == i, F.lit(MERCHANT_CATEGORIES[i]))
    return expr.otherwise(F.lit(MERCHANT_CATEGORIES[0]))


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def build_dim_account(df: DataFrame) -> DataFrame:
    """
    Customer accounts appear as originators AND as destinations, so both
    sides must be unioned before deduplication — otherwise accounts that
    only ever receive money would be missing from the dimension and their
    fact rows would fail the foreign key check on load.
    """
    originators = df.select(F.col("orig_account_id").alias("account_id"))

    destinations = (
        df.filter(~F.col("dest_is_merchant"))
          .select(F.col("dest_id_masked").alias("account_id"))
    )

    return (
        originators.union(destinations)
        .distinct()
        .withColumn("region", assign_region("account_id"))
        .withColumn("account_status", F.lit("ACTIVE"))
    )


def build_dim_merchant(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("dest_is_merchant"))
          .select(F.col("dest_id_masked").alias("merchant_id"))
          .distinct()
          .withColumn("merchant_category", assign_merchant_category("merchant_id"))
          .withColumn("region", assign_region("merchant_id"))
    )


def build_dim_date(df: DataFrame) -> DataFrame:
    return (
        df.select("date_key", "full_timestamp", "step", "calendar_date", "hour_of_day")
        .distinct()
        # dayofweek() returns 1=Sunday; the warehouse schema expects 1=Monday
        .withColumn("day_of_week",
                    ((F.dayofweek("full_timestamp") + 5) % 7) + 1)
        .withColumn("day_name", F.date_format("full_timestamp", "EEEE"))
        .withColumn("week_of_month",
                    F.ceil(F.dayofmonth("full_timestamp") / 7).cast("int"))
        .withColumn("month_number", F.month("full_timestamp"))
        .withColumn("month_name", F.date_format("full_timestamp", "MMMM"))
        .withColumn("quarter", F.quarter("full_timestamp"))
        .withColumn("year", F.year("full_timestamp"))
        .withColumn("is_weekend", F.dayofweek("full_timestamp").isin(1, 7))
    )


def build_fact_staging(df: DataFrame) -> DataFrame:
    """
    Fact rows carrying masked NATURAL keys. load.py resolves these to
    surrogate keys after the dimensions are loaded.

    dest_account_id and dest_merchant_id are mutually exclusive — exactly
    one is populated per row, mirroring the CHECK constraint on the
    warehouse table.
    """
    return df.select(
        F.col("orig_account_id"),
        F.when(~F.col("dest_is_merchant"), F.col("dest_id_masked"))
         .otherwise(F.lit(None)).alias("dest_account_id"),
        F.when(F.col("dest_is_merchant"), F.col("dest_id_masked"))
         .otherwise(F.lit(None)).alias("dest_merchant_id"),
        F.col("date_key"),
        # Partition column: calendar_date, NOT date_key. See the write step
        # in main() for why this distinction matters.
        F.col("calendar_date").alias("partition_date"),
        F.col("type_code"),
        F.col("amount"),
        F.col("oldbalanceOrg").alias("balance_before_orig"),
        F.col("newbalanceOrig").alias("balance_after_orig"),
        F.col("oldbalanceDest").alias("balance_before_dest"),
        F.col("newbalanceDest").alias("balance_after_dest"),
        (F.col("isFraud") == 1).alias("is_fraud"),
        (F.col("isFlaggedFraud") == 1).alias("is_flagged_by_system"),
        F.col("step").alias("source_step"),
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(fact: DataFrame, accounts: DataFrame, merchants: DataFrame) -> None:
    """
    Fails loudly before writing rather than letting bad data reach the
    warehouse and surface as a foreign key violation mid-load.
    """
    problems = []

    both_set = fact.filter(
        F.col("dest_account_id").isNotNull() & F.col("dest_merchant_id").isNotNull()
    ).count()
    if both_set:
        problems.append(f"{both_set} rows have BOTH destination keys set")

    neither_set = fact.filter(
        F.col("dest_account_id").isNull() & F.col("dest_merchant_id").isNull()
    ).count()
    if neither_set:
        problems.append(f"{neither_set} rows have NEITHER destination key set")

    orphan_orig = (
        fact.select("orig_account_id").distinct()
        .join(accounts, fact.orig_account_id == accounts.account_id, "left_anti")
        .count()
    )
    if orphan_orig:
        problems.append(f"{orphan_orig} originator IDs missing from dim_account")

    negative = fact.filter(F.col("amount") < 0).count()
    if negative:
        problems.append(f"{negative} rows have negative amounts")

    if problems:
        for p in problems:
            print(f"[validate] FAIL: {p}", file=sys.stderr)
        raise ValueError("Validation failed — refusing to write output")

    print("[validate] all checks passed")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PaySim -> warehouse-ready Parquet")
    parser.add_argument("--input", required=True,
                        help="Raw CSV path (local or wasbs:// for Azure Blob)")
    parser.add_argument("--output", required=True,
                        help="Output directory for Parquet datasets")
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--shuffle-partitions", type=int, default=8,
                        help="Raise toward 200 when running on a real cluster")
    args = parser.parse_args()
    
    load_dotenv()

    salt = os.environ.get("MASKING_SALT")
    if not salt:
        print("ERROR: MASKING_SALT environment variable is not set.\n"
              "  This is the salt used to hash account identifiers. It must "
              "stay consistent across runs (or dimension keys will not match "
              "between loads) and must never be committed to source control.\n"
              "  Fix: copy .env.example to .env and set a real value there,\n"
              "  or set it directly for this session:\n"
              "    PowerShell : $env:MASKING_SALT = 'your-secret-value'\n"
              "    macOS/Linux: export MASKING_SALT='your-secret-value'",
              file=sys.stderr)
        sys.exit(1)

    spark = build_spark(shuffle_partitions=args.shuffle_partitions)
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(f"[read] {args.input}")
        raw = spark.read.csv(args.input, header=True, schema=PAYSIM_SCHEMA)

        cleaned = clean(raw)
        enriched = enrich(cleaned, salt)

        # Enriched frame is consumed four times below. Without caching, Spark
        # would recompute the entire read + clean + hash chain each time.
        enriched.cache()

        dim_account = build_dim_account(enriched)
        dim_merchant = build_dim_merchant(enriched)
        dim_date = build_dim_date(enriched)
        fact = build_fact_staging(enriched)

        print(f"[build] dim_account:  {dim_account.count():,} rows")
        print(f"[build] dim_merchant: {dim_merchant.count():,} rows")
        print(f"[build] dim_date:     {dim_date.count():,} rows")
        print(f"[build] fact:         {fact.count():,} rows")

        if not args.skip_validation:
            validate(fact, dim_account, dim_merchant)

        out = args.output.rstrip("/")

        dim_account.coalesce(1).write.mode("overwrite").parquet(f"{out}/dim_account")
        dim_merchant.coalesce(1).write.mode("overwrite").parquet(f"{out}/dim_merchant")
        dim_date.coalesce(1).write.mode("overwrite").parquet(f"{out}/dim_date")

        (
            fact
            .repartition("partition_date")
            .write.mode("overwrite")
            .partitionBy("partition_date")
            .parquet(f"{out}/fact_transactions")
        )

        print(f"[write] output written to {out}/")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()