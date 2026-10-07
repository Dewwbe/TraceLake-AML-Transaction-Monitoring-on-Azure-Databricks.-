"""Run the whole pipeline locally on sample data (local Spark, parquet tables).

Usage: ``python -m aml.jobs.local_pipeline``  (after ``make sample-data``)
No Azure, Databricks, Delta or Airflow is involved; stage order mirrors the Airflow DAG.
"""

from __future__ import annotations

import os
import shutil

from aml.jobs.run_stage import make_ctx, run

ORDER = [
    "bronze_transactions",
    "bronze_reference",
    "silver_transactions",
    "silver_reference",
    "dq_gate",
    "update_country_dim",
    "screening",
    "update_customer_dim",   # initial versions (valid from onboarding)
    "detect_rules",
    "build_alerts",
    "update_customer_dim",  # second pass: alerts feed ratings, producing SCD2 history
    "build_marts",
]


def main(reset: bool = True) -> None:
    os.environ.setdefault("AML_STORE", "parquet")
    ctx = make_ctx()
    if reset:
        for path in (ctx.cfg.base_path, ctx.cfg.checkpoint_path):
            shutil.rmtree(path, ignore_errors=True)
    for stage in ORDER:
        run(stage, ctx)


if __name__ == "__main__":
    main()
