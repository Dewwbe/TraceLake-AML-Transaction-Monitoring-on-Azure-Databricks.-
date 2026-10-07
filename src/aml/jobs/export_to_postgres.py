"""Load the Gold and ops tables into PostgreSQL so Grafana can query them.

Usage: ``python -m aml.jobs.export_to_postgres``  (after ``make local-run``, with ``docker compose up -d`` running)
Reads parquet with pandas (no Spark, no Hadoop needed) and replaces the tables on every run.
Connection: ``AML_PG_URL`` (default matches dashboard/docker-compose.yml).
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

TABLES = [
    "gold.dim_customer", "gold.dim_country", "gold.dim_account", "gold.dim_rule",
    "gold.fact_transaction", "gold.fact_screening", "gold.fact_alert", "gold.fact_case",
    "gold.mart_rule_performance",
    "silver.transactions_quarantine",
    "ops.dq_results", "ops.pipeline_runs",
]
DEFAULT_URL = "postgresql+psycopg2://aml:aml@localhost:5433/aml"


def main() -> None:
    base = Path(os.environ.get("AML_BASE_PATH", "./data/lake"))
    engine = create_engine(os.environ.get("AML_PG_URL", DEFAULT_URL))
    with engine.begin() as conn:
        for schema in sorted({t.split(".")[0] for t in TABLES}):
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    for name in TABLES:
        schema, table = name.split(".")
        path = base / schema / table
        if not path.exists() or not any(path.rglob("*.parquet")):
            print(f"skip {name}: no parquet at {path}")  # noqa: T201
            continue
        df = pd.read_parquet(path)
        for col in df.columns:  # Postgres cannot adapt arrays via pandas; store them as comma-separated text
            if df[col].map(lambda v: isinstance(v, (list, np.ndarray))).any():
                df[col] = df[col].map(lambda v: ",".join(map(str, v)) if isinstance(v, (list, np.ndarray)) else v)
        df.to_sql(table, engine, schema=schema, if_exists="replace", index=False, chunksize=2000)
        print(f"{name}: {len(df)} rows")  # noqa: T201


if __name__ == "__main__":
    main()
