"""Print a summary of pipeline outputs: counts, data quality, detection and screening metrics.

Usage: ``python -m aml.jobs.report [--truth ./data/ground_truth/screening_ground_truth.csv]``
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from aml.evaluation.metrics import screening_metrics
from aml.jobs.run_stage import make_ctx
from aml.schemas import TABLES


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--truth", default="./data/ground_truth/screening_ground_truth.csv")
    args = ap.parse_args()
    ctx = make_ctx()
    spark, store = ctx.spark, ctx.store

    def show(title: str, df, n: int = 30) -> None:
        print(f"\n=== {title}")  # noqa: T201
        df.show(n, truncate=False)

    counts = [(t, store.read(t).count()) for t in TABLES if store.exists(t)]
    show("Row counts", spark.createDataFrame(counts, "table string, rows long").orderBy("table"), 40)

    if store.exists("ops.dq_results"):
        dq = store.read("ops.dq_results").filter("rows_failed > 0").select(
            "table_name", "rule_id", "severity", "action", "rows_checked", "rows_failed",
            F.round("fail_pct", 2).alias("fail_pct"))
        show("Data quality failures", dq)
    if store.exists("gold.mart_rule_performance"):
        perf = store.read("gold.mart_rule_performance").select(
            "rule_id", "alerted_accounts", "true_positive_accounts", "positive_accounts",
            F.round("precision", 3).alias("precision"), F.round("recall", 3).alias("recall")).orderBy("rule_id")
        show("Rule performance (account level, vs ground-truth labels)", perf)
    if store.exists("gold.fact_screening"):
        truth = spark.read.option("header", True).csv(args.truth)
        res = store.read("gold.fact_screening")
        cust = store.read("silver.customers")
        show("Screening recall by perturbation type (seeded ground truth)",
             screening_metrics(res, truth, cust).orderBy("perturbation"))
    if store.exists("gold.dim_customer"):
        multi = (store.read("gold.dim_customer").groupBy("customer_id").count().filter("count > 1")
                 .select("customer_id").limit(3))
        hist = store.read("gold.dim_customer").join(multi, "customer_id").select(
            "customer_id", "risk_rating", "risk_score", "valid_from", "valid_to", "is_current"
        ).orderBy("customer_id", "valid_from")
        show("SCD2 history sample (customers whose rating changed)", hist)
    if store.exists("gold.fact_case"):
        show("Cases by priority and status", store.read("gold.fact_case").groupBy("priority", "status").count()
             .orderBy("priority", "status"))


if __name__ == "__main__":
    main()
