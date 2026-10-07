"""Generate synthetic sample data into the landing zone (SYNTHETIC; no download needed).

Writes: transactions (micro-batch CSVs), customers, accounts, a FICTIONAL sanctions list,
a demo country-risk table, and the screening ground truth under data/ground_truth/.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from aml.generators.replay import write_microbatches
from aml.generators.synthetic import (
    COUNTRIES,
    HIGH_RISK_DEMO,
    generate_customers,
    generate_fictional_sanctions,
    generate_transactions,
    seed_name_matches,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", default="./data/landing")
    ap.add_argument("--truth", default="./data/ground_truth")
    ap.add_argument("--accounts", type=int, default=300)
    ap.add_argument("--normal", type=int, default=6000)
    ap.add_argument("--rows-per-file", type=int, default=1000)
    args = ap.parse_args()

    landing, truth = Path(args.landing), Path(args.truth)
    txn, acct = generate_transactions(n_accounts=args.accounts, n_normal=args.normal)
    write_microbatches(txn, landing / "transactions", rows_per_file=args.rows_per_file)

    customers, accounts = generate_customers(list(acct["account_id"]))
    sanctions = generate_fictional_sanctions()
    listed = sanctions.loc[sanctions["record_type"] == "primary", "name"].tolist()
    customers, ground_truth = seed_name_matches(customers, listed)

    (landing / "customers").mkdir(parents=True, exist_ok=True)
    customers.to_csv(landing / "customers" / "customers_000001.csv", index=False)
    (landing / "accounts").mkdir(parents=True, exist_ok=True)
    accounts.to_csv(landing / "accounts" / "accounts_000001.csv", index=False)

    snap = landing / "sanctions" / "fictional_list" / date.today().isoformat()
    snap.mkdir(parents=True, exist_ok=True)
    sanctions.to_csv(snap / "entries.csv", index=False)

    risk = [{"country": c, "risk_tier": "HIGH" if c in HIGH_RISK_DEMO else "LOW",
             "source_list": "DEMO ONLY", "effective_date": "2024-01-01"} for c in COUNTRIES]
    (landing / "country_risk").mkdir(parents=True, exist_ok=True)
    import pandas as pd
    pd.DataFrame(risk).to_csv(landing / "country_risk" / "country_risk_demo.csv", index=False)

    truth.mkdir(parents=True, exist_ok=True)
    ground_truth.to_csv(truth / "screening_ground_truth.csv", index=False)


if __name__ == "__main__":
    main()
