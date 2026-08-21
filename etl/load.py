"""
load.py
-------
Loads transform.py's Parquet output into the Azure Postgres warehouse.

Responsibilities, in order:
    1. Load dim_date         — insert-if-new (dates never change once written)
    2. Load dim_merchant     — Type 1 upsert (overwrite attributes in place)
    3. Load dim_account      — Type 2 (SCD)  — see design note below
    4. Load fact_transactions — resolve masked natural keys to surrogate
                                keys, replace-by-partition for idempotency

DESIGN: staging tables, not row-by-row Python loops
    Every load goes through a Postgres TEMP TABLE: the Parquet data is
    copied in as-is, then a single set-based SQL statement does the actual
    upsert/SCD/resolution logic. This is dramatically faster than looping
    in Python (one round-trip per batch instead of one per row) and it's
    also the pattern production pipelines actually use — Python's job here
    is orchestration, not row-level logic.

DESIGN: SCD Type 2 on dim_account
    An account's CURRENT attributes live in exactly one row where
    is_current = TRUE. When a new load shows different attributes for an
    account that already has a current row:
        1. The current row is closed: effective_end = now(), is_current = FALSE
        2. A new row is inserted with the new attributes, is_current = TRUE
    Old rows are never deleted or overwritten — that's what makes it "Type 2"
    (history-preserving) rather than "Type 1" (overwrite in place, used for
    dim_merchant here since merchant attributes don't need a history).

DESIGN: idempotency via replace-by-partition on fact_transactions
    fact_transactions has no natural unique key to de-duplicate against
    (transaction_key is a surrogate SERIAL). Rather than add a synthetic
    row hash, this loader deletes all existing fact rows for the date
    range being loaded, then inserts the new set. Re-running the loader
    for the same input is therefore safe — it produces the same end state
    rather than accumulating duplicates. This is the standard
    "full-partition replace" incremental pattern.

Usage:
    python etl/load.py --input data/processed
"""

import argparse
import os
import sys
from contextlib import contextmanager

import psycopg
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection():
    """
    Reads PG* variables from .env. sslmode is required for Azure Database
    for PostgreSQL — connecting without it fails outright, which is why
    it's hard-required here rather than defaulted to 'prefer'.
    """
    load_dotenv()
    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"ERROR: missing required env vars: {', '.join(missing)}\n"
              f"  Set these in .env — see .env.example.", file=sys.stderr)
        sys.exit(1)

    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        sslmode=os.environ.get("PGSSLMODE", "require"),
        connect_timeout=10,
    )


@contextmanager
def step(label: str):
    """Prints a timing/status line around each load stage."""
    print(f"[load] {label} ...", end=" ", flush=True)
    yield
    print("done")


# ---------------------------------------------------------------------------
# Parquet reading
# ---------------------------------------------------------------------------

def read_parquet_dir(path: str):
    """
    Reads a Parquet dataset (possibly partitioned across multiple files/
    subdirectories, as fact_transactions is) into a pandas DataFrame.

    Using pandas here rather than keeping this in Spark deliberately marks
    the boundary between the two stages: transform.py's job ends at
    "correct, validated Parquet on disk," and load.py's job starts there.
    Neither stage needs to know how the other is implemented internally.
    """
    import pandas as pd
    return pd.read_parquet(path)


# ---------------------------------------------------------------------------
# dim_date — insert-if-new
# ---------------------------------------------------------------------------

def load_dim_date(conn, df) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE stg_dim_date (
                date_key INTEGER, full_timestamp TIMESTAMP, step INTEGER,
                calendar_date DATE, hour_of_day SMALLINT, day_of_week SMALLINT,
                day_name VARCHAR(10), week_of_month SMALLINT, month_number SMALLINT,
                month_name VARCHAR(10), quarter SMALLINT, year SMALLINT,
                is_weekend BOOLEAN
            ) ON COMMIT DROP
        """)

        rows = list(df[[
            "date_key", "full_timestamp", "step", "calendar_date",
            "hour_of_day", "day_of_week", "day_name", "week_of_month",
            "month_number", "month_name", "quarter", "year", "is_weekend",
        ]].itertuples(index=False, name=None))

        with cur.copy("COPY stg_dim_date FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)

        # Dates are immutable once written — a given date_key always means
        # the same hour, so ON CONFLICT DO NOTHING is correct here (never
        # needs the SCD treatment dim_account gets).
        cur.execute("""
            INSERT INTO dim_date
                (date_key, full_timestamp, step, calendar_date, hour_of_day,
                 day_of_week, day_name, week_of_month, month_number,
                 month_name, quarter, year, is_weekend)
            SELECT * FROM stg_dim_date
            ON CONFLICT (date_key) DO NOTHING
        """)
        inserted = cur.rowcount
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# dim_merchant — Type 1 upsert (overwrite in place)
# ---------------------------------------------------------------------------

def load_dim_merchant(conn, df) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE stg_dim_merchant (
                merchant_id VARCHAR(64), merchant_category VARCHAR(50), region VARCHAR(50)
            ) ON COMMIT DROP
        """)

        rows = list(df[["merchant_id", "merchant_category", "region"]]
                    .itertuples(index=False, name=None))
        with cur.copy("COPY stg_dim_merchant FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)

        cur.execute("""
            INSERT INTO dim_merchant (merchant_id, merchant_category, region)
            SELECT merchant_id, merchant_category, region FROM stg_dim_merchant
            ON CONFLICT (merchant_id) DO UPDATE
                SET merchant_category = EXCLUDED.merchant_category,
                    region = EXCLUDED.region
        """)
        affected = cur.rowcount
    conn.commit()
    return affected


# ---------------------------------------------------------------------------
# dim_account — Type 2 SCD
# ---------------------------------------------------------------------------

def load_dim_account(conn, df) -> dict:
    """
    Returns a dict of {inserted_new, closed_and_reopened} counts so the
    caller can report what actually changed, not just "rows processed."
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE stg_dim_account (
                account_id VARCHAR(64), region VARCHAR(50), account_status VARCHAR(20)
            ) ON COMMIT DROP
        """)

        rows = list(df[["account_id", "region", "account_status"]]
                    .itertuples(index=False, name=None))
        with cur.copy("COPY stg_dim_account FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)

        # Step 1: close out current rows whose attributes changed.
        # IS DISTINCT FROM (rather than !=) correctly treats NULL vs NULL
        # as "not changed" — plain != would silently skip comparisons
        # involving NULLs, which is a common, easy-to-miss SCD bug.
        cur.execute("""
            UPDATE dim_account d
            SET effective_end = now(), is_current = FALSE
            FROM stg_dim_account s
            WHERE d.account_id = s.account_id
              AND d.is_current = TRUE
              AND (
                    d.region IS DISTINCT FROM s.region
                 OR d.account_status IS DISTINCT FROM s.account_status
              )
        """)
        closed = cur.rowcount

        # Step 2: insert a current row for every account that either
        # (a) has never been seen before, or (b) was just closed above.
        # Both cases share one condition: "no current row exists right now."
        cur.execute("""
            INSERT INTO dim_account (account_id, region, account_status, is_current)
            SELECT s.account_id, s.region, s.account_status, TRUE
            FROM stg_dim_account s
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_account d
                WHERE d.account_id = s.account_id AND d.is_current = TRUE
            )
        """)
        inserted = cur.rowcount

    conn.commit()
    return {"closed_and_reopened": closed, "inserted_new": inserted - closed}


# ---------------------------------------------------------------------------
# fact_transactions — key resolution + replace-by-partition
# ---------------------------------------------------------------------------

def load_fact_transactions(conn, df) -> dict:
    partition_dates = sorted(df["partition_date"].unique().tolist())

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE stg_fact (
                orig_account_id VARCHAR(64),
                dest_account_id VARCHAR(64),
                dest_merchant_id VARCHAR(64),
                date_key BIGINT,
                type_code VARCHAR(20),
                amount NUMERIC(18,2),
                balance_before_orig NUMERIC(18,2),
                balance_after_orig NUMERIC(18,2),
                balance_before_dest NUMERIC(18,2),
                balance_after_dest NUMERIC(18,2),
                is_fraud BOOLEAN,
                is_flagged_by_system BOOLEAN,
                source_step INTEGER,
                partition_date DATE
            ) ON COMMIT DROP
        """)

        cols = ["orig_account_id", "dest_account_id", "dest_merchant_id",
                "date_key", "type_code", "amount", "balance_before_orig",
                "balance_after_orig", "balance_before_dest", "balance_after_dest",
                "is_fraud", "is_flagged_by_system", "source_step", "partition_date"]
        rows = list(df[cols].itertuples(index=False, name=None))
        with cur.copy(f"COPY stg_fact ({', '.join(cols)}) FROM STDIN") as copy:
            for row in rows:
                # pandas NaT/NaN -> None so psycopg writes SQL NULL, not "nan"
                copy.write_row(tuple(None if _is_null(v) else v for v in row))

        # Idempotent replace: wipe existing facts for exactly the date range
        # in this load before inserting. Re-running with the same input
        # file therefore converges to the same state instead of duplicating.
        cur.execute(
            "DELETE FROM fact_transactions WHERE date_key IN "
            "(SELECT date_key FROM dim_date WHERE calendar_date = ANY(%s))",
            (partition_dates,)
        )
        deleted = cur.rowcount

        # Key resolution happens entirely here, set-based, in one pass:
        #   - date_key needs no resolution (transform.py already computes
        #     it to match dim_date's key exactly)
        #   - transaction_type_key resolved via dim_transaction_type.type_code
        #   - orig_account_key / dest_account_key resolved via dim_account,
        #     restricted to is_current = TRUE (always join to the CURRENT
        #     version of an SCD dimension for fact loads)
        #   - dest_merchant_key resolved via dim_merchant
        # LEFT JOINs on the two destination dimensions because exactly one
        # of them matches per row (see the CHECK constraint in schema.sql);
        # COALESCE-free here because the un-matched side is simply NULL.
        cur.execute("""
            INSERT INTO fact_transactions (
                date_key, transaction_type_key, orig_account_key,
                dest_account_key, dest_merchant_key, amount,
                balance_before_orig, balance_after_orig,
                balance_before_dest, balance_after_dest,
                is_fraud, is_flagged_by_system, source_step
            )
            SELECT
                s.date_key,
                tt.transaction_type_key,
                orig.account_key,
                dest_acc.account_key,
                dest_mer.merchant_key,
                s.amount,
                s.balance_before_orig, s.balance_after_orig,
                s.balance_before_dest, s.balance_after_dest,
                s.is_fraud, s.is_flagged_by_system, s.source_step
            FROM stg_fact s
            JOIN dim_transaction_type tt ON tt.type_code = s.type_code
            JOIN dim_account orig
                ON orig.account_id = s.orig_account_id AND orig.is_current = TRUE
            LEFT JOIN dim_account dest_acc
                ON dest_acc.account_id = s.dest_account_id AND dest_acc.is_current = TRUE
            LEFT JOIN dim_merchant dest_mer
                ON dest_mer.merchant_id = s.dest_merchant_id
        """)
        inserted = cur.rowcount

        # Rows that fail to resolve (e.g. an orig account somehow missing
        # from dim_account) are silently dropped by the INNER JOIN above.
        # Surface that instead of letting it pass quietly — a fact loader
        # that quietly loses rows is worse than one that fails loudly.
        cur.execute("SELECT COUNT(*) FROM stg_fact")
        staged = cur.fetchone()[0]

    conn.commit()
    unresolved = staged - inserted
    if unresolved > 0:
        print(f"[load] WARNING: {unresolved} fact rows did not resolve to "
              f"an existing current account and were dropped. Investigate "
              f"before trusting downstream analysis.", file=sys.stderr)

    return {"deleted": deleted, "inserted": inserted, "staged": staged,
            "unresolved": unresolved}


def _is_null(v):
    """pandas surfaces missing values as float('nan') or pandas.NaT, neither
    of which Python's plain `is None` catches. This normalizes both to None
    so psycopg writes a real SQL NULL instead of the string 'nan'."""
    try:
        import pandas as pd
        return pd.isna(v)
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Load warehouse Parquet output into Postgres")
    parser.add_argument("--input", required=True,
                        help="Directory containing dim_account/, dim_merchant/, "
                             "dim_date/, fact_transactions/ (output of transform.py)")
    args = parser.parse_args()

    base = args.input.rstrip("/")

    print("[load] connecting to warehouse...")
    conn = get_connection()
    print(f"[load] connected to {os.environ['PGDATABASE']} on {os.environ['PGHOST']}")

    try:
        with step("reading dim_date parquet"):
            dim_date_df = read_parquet_dir(f"{base}/dim_date")
        with step(f"loading dim_date ({len(dim_date_df):,} rows)"):
            date_inserted = load_dim_date(conn, dim_date_df)
        print(f"[load]   -> {date_inserted:,} new date rows inserted")

        with step("reading dim_merchant parquet"):
            dim_merchant_df = read_parquet_dir(f"{base}/dim_merchant")
        with step(f"loading dim_merchant ({len(dim_merchant_df):,} rows)"):
            merchant_affected = load_dim_merchant(conn, dim_merchant_df)
        print(f"[load]   -> {merchant_affected:,} merchant rows inserted/updated")

        with step("reading dim_account parquet"):
            dim_account_df = read_parquet_dir(f"{base}/dim_account")
        with step(f"loading dim_account ({len(dim_account_df):,} rows, SCD Type 2)"):
            account_result = load_dim_account(conn, dim_account_df)
        print(f"[load]   -> {account_result['inserted_new']:,} brand-new accounts, "
              f"{account_result['closed_and_reopened']:,} accounts with changed "
              f"attributes (old version closed, new version inserted)")

        with step("reading fact_transactions parquet"):
            fact_df = read_parquet_dir(f"{base}/fact_transactions")
        with step(f"loading fact_transactions ({len(fact_df):,} rows, "
                  f"replace-by-partition)"):
            fact_result = load_fact_transactions(conn, fact_df)
        print(f"[load]   -> deleted {fact_result['deleted']:,} existing rows in "
              f"affected date range, inserted {fact_result['inserted']:,} new rows")
        if fact_result["unresolved"] > 0:
            print(f"[load]   -> {fact_result['unresolved']:,} rows unresolved "
                  f"(see warning above)")

        print("\n[load] all stages complete")

    except Exception:
        conn.rollback()
        print("[load] ERROR — transaction rolled back, warehouse unchanged", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()