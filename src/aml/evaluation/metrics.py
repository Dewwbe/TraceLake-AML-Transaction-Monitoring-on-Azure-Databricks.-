"""Evaluation against ground-truth labels and seeded screening cases.

Labels are used here only. Detection rules and screening never read them.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def positive_accounts(txn: DataFrame) -> DataFrame:
    """Originator accounts: senders of at least one laundering-labelled transaction.

    Counterparties of a labelled transaction (for example the many receivers in a fan-out) are not
    counted as positives, because the rules are designed to flag the originating account.
    """
    return (
        txn.filter(F.col("is_laundering") == 1)
        .select(F.col("sender_account").alias("account_id"))
        .distinct()
    )


def rule_performance(hits: DataFrame, txn: DataFrame, run_id: str) -> DataFrame:
    """Account-level precision and recall per rule (positives = originator accounts), plus an ANY row across all rules."""
    pos = positive_accounts(txn).withColumn("_pos", F.lit(1))
    n_pos = pos.count()
    per_rule = hits.select("rule_id", "account_id").distinct()
    any_rule = hits.select(F.lit("ANY").alias("rule_id"), "account_id").distinct()
    flagged = per_rule.unionByName(any_rule).join(pos, "account_id", "left")
    agg = flagged.groupBy("rule_id").agg(
        F.count(F.lit(1)).alias("alerted_accounts"),
        F.sum(F.coalesce(F.col("_pos"), F.lit(0))).cast("long").alias("true_positive_accounts"),
    )
    return (
        agg.withColumn("run_id", F.lit(run_id))
        .withColumn("positive_accounts", F.lit(n_pos).cast("long"))
        .withColumn("precision", F.col("true_positive_accounts") / F.col("alerted_accounts"))
        .withColumn(
            "recall",
            F.when(F.lit(n_pos) > 0, F.col("true_positive_accounts") / F.lit(n_pos)).otherwise(F.lit(None)),
        )
        .withColumn("computed_at", F.current_timestamp())
    )


def screening_metrics(results: DataFrame, ground_truth: DataFrame, all_customers: DataFrame) -> DataFrame:
    """Recall of seeded matches by perturbation type, plus precision over all customers.

    ``results``: customer_id, band.  ``ground_truth``: customer_id, perturbation.
    """
    flagged = results.groupBy("customer_id").agg(F.max("band").alias("band"))
    seeded = ground_truth.join(flagged, "customer_id", "left").withColumn(
        "caught", F.col("band").isNotNull().cast("int")
    )
    by_type = seeded.groupBy("perturbation").agg(
        F.count(F.lit(1)).alias("seeded"), F.sum("caught").alias("caught")
    )
    by_type = by_type.withColumn("recall", F.col("caught") / F.col("seeded"))

    gt_ids = ground_truth.select("customer_id").withColumn("_gt", F.lit(1))
    fp = (
        flagged.join(gt_ids, "customer_id", "left")
        .filter(F.col("_gt").isNull())
        .count()
    )
    total = by_type.agg(F.sum("seeded").alias("s"), F.sum("caught").alias("c")).collect()[0]
    overall = all_customers.sparkSession.createDataFrame(
        [("ALL", int(total["s"] or 0), int(total["c"] or 0),
          (total["c"] / total["s"]) if total["s"] else None)],
        "perturbation string, seeded long, caught long, recall double",
    )
    out = by_type.unionByName(overall)
    return out.withColumn("false_positive_customers", F.lit(fp).cast("long"))


def simulate_dispositions(cases: DataFrame, alerts: DataFrame, txn: DataFrame) -> DataFrame:
    """SIMULATED analyst outcomes, used only to populate dashboards in this portfolio project.

    A case is ESCALATED if any of its alerts is on a laundering-labelled account or is a STRONG
    sanctions match; otherwise FALSE_POSITIVE. Closure delay, assignment and open/closed status
    are derived deterministically from the case id.
    """
    pos = positive_accounts(txn).withColumn("_pos", F.lit(1)).withColumnRenamed("account_id", "_pacc")
    flagged = alerts.join(pos, alerts["account_id"] == F.col("_pacc"), "left")
    truth = flagged.groupBy("case_id").agg(
        F.max(
            F.when(F.col("_pos").isNotNull() | (F.col("band") == "STRONG"), 1).otherwise(0)
        ).alias("_true")
    )
    h = F.abs(F.xxhash64(F.col("case_id")))
    return (
        cases.drop("assigned_to", "disposition", "closed_ts", "status")
        .join(truth, "case_id", "left")
        .withColumn("assigned_to", F.concat(F.lit("analyst_"), (h % 5 + 1).cast("string")))
        .withColumn("_closed", (h % 10) < 8)
        .withColumn(
            "disposition",
            F.when(F.col("_closed"), F.when(F.col("_true") == 1, "ESCALATED").otherwise("FALSE_POSITIVE")),
        )
        .withColumn(
            "closed_ts",
            F.when(F.col("_closed"), F.expr("opened_ts + make_interval(0, 0, 0, 1 + int(abs(hash(case_id)) % 10))")),
        )
        .withColumn("status", F.when(F.col("_closed"), "CLOSED").otherwise("IN_REVIEW"))
        .drop("_true", "_closed")
    )
