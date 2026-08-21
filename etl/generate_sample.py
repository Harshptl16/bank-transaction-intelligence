import argparse
import csv
import random
from pathlib import Path

# PaySim transaction types and their real-world frequency distribution
TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]
TXN_WEIGHTS = [0.34, 0.08, 0.35, 0.22, 0.01]

# Fraud only occurs on these two types in PaySim
FRAUD_ELIGIBLE_TYPES = {"TRANSFER", "CASH_OUT"}

FRAUD_RATE = 0.0013          # ~0.13%, matches real PaySim
FLAGGED_THRESHOLD = 200_000  # isFlaggedFraud fires on very large transfers

TOTAL_STEPS = 744            # 30 days x 24 hours


def make_customer_id(n: int) -> str:
    """PaySim customer IDs look like C1231006815."""
    return f"C{n}"


def make_merchant_id(n: int) -> str:
    """PaySim merchant IDs look like M1979787155."""
    return f"M{n}"


def generate(rows: int, seed: int = 42):
    rng = random.Random(seed)

    # Build a pool of accounts so IDs repeat across rows — this is important,
    # it's what makes dimension deduplication a real problem in the ETL.
    n_customers = max(100, rows // 8)
    n_merchants = max(20, rows // 40)

    customers = [make_customer_id(rng.randint(10**9, 2 * 10**9)) for _ in range(n_customers)]
    merchants = [make_merchant_id(rng.randint(10**9, 2 * 10**9)) for _ in range(n_merchants)]

    for _ in range(rows):
        step = rng.randint(1, TOTAL_STEPS)
        txn_type = rng.choices(TXN_TYPES, weights=TXN_WEIGHTS, k=1)[0]

        # Amount distribution is heavily right-skewed in PaySim
        amount = round(rng.lognormvariate(8.5, 1.5), 2)

        name_orig = rng.choice(customers)

        # PAYMENT always goes to a merchant; everything else to a customer
        if txn_type == "PAYMENT":
            name_dest = rng.choice(merchants)
            is_merchant_dest = True
        else:
            name_dest = rng.choice([c for c in (rng.choice(customers),)])
            is_merchant_dest = False

        # Originator balances
        old_balance_orig = round(rng.lognormvariate(9.0, 1.8), 2)
        new_balance_orig = max(0.0, round(old_balance_orig - amount, 2))

        # Destination balances — merchants always report zero in PaySim
        if is_merchant_dest:
            old_balance_dest = 0.0
            new_balance_dest = 0.0
        else:
            old_balance_dest = round(rng.lognormvariate(9.0, 1.8), 2)
            new_balance_dest = round(old_balance_dest + amount, 2)

        # Fraud determination
        is_fraud = 0
        if txn_type in FRAUD_ELIGIBLE_TYPES and rng.random() < FRAUD_RATE / 0.43:
            is_fraud = 1

        # QUIRK: PaySim zeroes out balance columns on fraudulent rows.
        # The ETL and downstream analysis must not treat these as signal.
        if is_fraud:
            old_balance_orig = 0.0
            new_balance_orig = 0.0
            old_balance_dest = 0.0
            new_balance_dest = 0.0

        is_flagged_fraud = 1 if (txn_type == "TRANSFER" and amount > FLAGGED_THRESHOLD) else 0

        yield {
            "step": step,
            "type": txn_type,
            "amount": amount,
            "nameOrig": name_orig,
            "oldbalanceOrg": old_balance_orig,
            "newbalanceOrig": new_balance_orig,
            "nameDest": name_dest,
            "oldbalanceDest": old_balance_dest,
            "newbalanceDest": new_balance_dest,
            "isFraud": is_fraud,
            "isFlaggedFraud": is_flagged_fraud,
        }


def main():
    parser = argparse.ArgumentParser(description="Generate PaySim-shaped sample data")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--out", type=str, default="data/raw/paysim_sample.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
        "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
    ]

    fraud_count = 0
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in generate(args.rows, args.seed):
            fraud_count += row["isFraud"]
            writer.writerow(row)

    print(f"Wrote {args.rows:,} rows to {out_path}")
    print(f"Fraud rows: {fraud_count:,} ({fraud_count / args.rows * 100:.3f}%)")


if __name__ == "__main__":
    main()
