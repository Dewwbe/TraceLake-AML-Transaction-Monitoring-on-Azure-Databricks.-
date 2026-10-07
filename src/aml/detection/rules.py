"""Behavioural detection rules.

Each rule is a function ``(transactions, params, ctx) -> hits`` returning a DataFrame with the
standard hit schema: ``rule_id, account_id, transaction_id, event_ts, score, evidence``.

Labels (``is_laundering``) are NEVER read by any rule; they are used only by evaluation.

Scoring convention: ``score = min(1, 0.5 * observed / threshold)``, so a hit exactly at the
threshold scores 0.5 and a hit at twice the threshold scores 1.0.

Hits are de-duplicated to one per (rule, account, day), keeping the highest score.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

HIT_COLUMNS = ["rule_id", "account_id", "transaction_id", "event_ts", "score", "evidence"]

Rule = Callable[[DataFrame, dict[str, Any], DataFrame | None], DataFrame]


def _score(observed: Column, threshold: float) -> Column:
    return F.least(F.lit(1.0), F.lit(0.5) * observed.cast("double") / F.lit(float(threshold)))


def _finalise(df: DataFrame, rule_id: str, evidence_cols: list[str]) -> DataFrame:
    """Standardise columns and keep the best hit per account per day."""
    shaped = df.select(
        F.lit(rule_id).alias("rule_id"),
        F.col("account_id"),
        F.col("transaction_id"),
        F.col("event_ts"),
        F.col("score"),
        F.to_json(F.struct(*[F.col(c) for c in evidence_cols])).alias("evidence"),
    )
    w = Window.partitionBy("account_id", F.to_date("event_ts")).orderBy(
        F.col("score").desc(), F.col("event_ts")
    )
    return shaped.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").select(*HIT_COLUMNS)


def _range_window(partition: str, seconds: int):
    return (
        Window.partitionBy(partition)
        .orderBy(F.col("event_ts").cast("long"))
        .rangeBetween(-seconds, 0)
    )


def structuring(txn: DataFrame, p: dict[str, Any], _: DataFrame | None) -> DataFrame:
    threshold = float(p["threshold_aed"])
    lower = threshold * float(p["lower_ratio"])
    near = txn.filter((F.col("amount_aed") >= lower) & (F.col("amount_aed") < threshold))
    w = _range_window("sender_account", int(p["window_days"]) * 86400)
    scored = (
        near.withColumn("near_count", F.count(F.lit(1)).over(w))
        .withColumn("near_total_aed", F.sum("amount_aed").over(w))
        .filter(F.col("near_count") >= int(p["min_count"]))
        .withColumn("account_id", F.col("sender_account"))
        .withColumn("score", _score(F.col("near_count"), p["min_count"]))
    )
    return _finalise(scored, "R001", ["near_count", "near_total_aed"])


def pass_through(txn: DataFrame, p: dict[str, Any], _: DataFrame | None) -> DataFrame:
    out_d = txn.groupBy(
        F.col("sender_account").alias("account_id"), F.col("event_date").alias("d")
    ).agg(F.sum("amount_aed").alias("out_amt"), F.count(F.lit(1)).alias("out_n"))
    in_d = txn.groupBy(
        F.col("receiver_account").alias("account_id"), F.col("event_date").alias("d")
    ).agg(F.sum("amount_aed").alias("in_amt"), F.count(F.lit(1)).alias("in_n"))
    daily = (
        out_d.join(in_d, ["account_id", "d"], "full_outer")
        .na.fill(0, ["out_amt", "out_n", "in_amt", "in_n"])
        .withColumn("day_num", F.datediff(F.col("d"), F.lit("1970-01-01").cast("date")))
    )
    w = (
        Window.partitionBy("account_id")
        .orderBy("day_num")
        .rangeBetween(-(int(p["window_days"]) - 1), 0)
    )
    rolled = (
        daily.withColumn("in_sum_aed", F.sum("in_amt").over(w))
        .withColumn("out_sum_aed", F.sum("out_amt").over(w))
        .withColumn("in_cnt", F.sum("in_n").over(w))
        .withColumn("out_cnt", F.sum("out_n").over(w))
    )
    flagged = (
        rolled.filter(
            (F.col("in_sum_aed") >= float(p["min_amount_aed"]))
            & (F.col("out_sum_aed") >= float(p["out_in_ratio"]) * F.col("in_sum_aed"))
            & (F.col("in_cnt") >= 1)
            & (F.col("out_cnt") >= 1)
        )
        .withColumn("event_ts", F.col("d").cast("timestamp"))
        .withColumn("transaction_id", F.lit(None).cast("string"))
        .withColumn("score", _score(F.col("in_sum_aed"), p["min_amount_aed"]))
    )
    return _finalise(flagged, "R002", ["in_sum_aed", "out_sum_aed"])


def high_risk_jurisdiction(
    txn: DataFrame, p: dict[str, Any], country_dim: DataFrame | None
) -> DataFrame:
    if country_dim is None:
        return txn.sparkSession.createDataFrame([], "rule_id string, account_id string, "
            "transaction_id string, event_ts timestamp, score double, evidence string")
    tiers = list(p["tiers"])
    base = txn.filter(
        (F.col("amount_aed") >= float(p["min_amount_aed"]))
        & (F.col("sender_country") != F.col("receiver_country"))
    )
    risk = country_dim.select("country", "risk_tier", "valid_from", "valid_to")

    def with_tier(df: DataFrame, side: str) -> DataFrame:
        r = risk.select(
            F.col("country").alias(f"_{side}_c"),
            F.col("risk_tier").alias(f"{side}_tier"),
            F.col("valid_from").alias(f"_{side}_vf"),
            F.col("valid_to").alias(f"_{side}_vt"),
        )
        cond = (
            (F.col(f"{side}_country") == F.col(f"_{side}_c"))
            & (F.col("event_ts") >= F.col(f"_{side}_vf"))
            & (F.col(f"_{side}_vt").isNull() | (F.col("event_ts") < F.col(f"_{side}_vt")))
        )
        return df.join(r, cond, "left").drop(f"_{side}_c", f"_{side}_vf", f"_{side}_vt")

    tiered = with_tier(with_tier(base, "sender"), "receiver")
    flagged = (
        tiered.filter(F.col("sender_tier").isin(tiers) | F.col("receiver_tier").isin(tiers))
        .withColumn("account_id", F.col("sender_account"))
        .withColumn("score", _score(F.col("amount_aed"), p["min_amount_aed"]))
    )
    return _finalise(
        flagged, "R003", ["amount_aed", "sender_country", "receiver_country", "sender_tier", "receiver_tier"]
    )


def fan_out_fan_in(txn: DataFrame, p: dict[str, Any], _: DataFrame | None) -> DataFrame:
    seconds = int(p["window_days"]) * 86400
    n_min = int(p["min_counterparties"])

    out = (
        txn.withColumn(
            "n_counterparties",
            F.size(F.collect_set("receiver_account").over(_range_window("sender_account", seconds))),
        )
        .withColumn("account_id", F.col("sender_account"))
        .withColumn("direction", F.lit("fan_out"))
    )
    inn = (
        txn.withColumn(
            "n_counterparties",
            F.size(F.collect_set("sender_account").over(_range_window("receiver_account", seconds))),
        )
        .withColumn("account_id", F.col("receiver_account"))
        .withColumn("direction", F.lit("fan_in"))
    )
    cols = ["account_id", "transaction_id", "event_ts", "n_counterparties", "direction"]
    both = out.select(*cols).unionByName(inn.select(*cols))
    flagged = both.filter(F.col("n_counterparties") >= n_min).withColumn(
        "score", _score(F.col("n_counterparties"), n_min)
    )
    return _finalise(flagged, "R004", ["n_counterparties", "direction"])


def round_amounts(txn: DataFrame, p: dict[str, Any], _: DataFrame | None) -> DataFrame:
    unit = float(p["round_unit"])
    rnd = txn.filter(
        (F.col("amount_aed") >= float(p["min_amount_aed"])) & (F.col("amount_aed") % unit == 0)
    )
    w = _range_window("sender_account", int(p["window_days"]) * 86400)
    flagged = (
        rnd.withColumn("round_count", F.count(F.lit(1)).over(w))
        .filter(F.col("round_count") >= int(p["min_count"]))
        .withColumn("account_id", F.col("sender_account"))
        .withColumn("score", _score(F.col("round_count"), p["min_count"]))
    )
    return _finalise(flagged, "R005", ["round_count"])


RULES: dict[str, Rule] = {
    "R001": structuring,
    "R002": pass_through,
    "R003": high_risk_jurisdiction,
    "R004": fan_out_fan_in,
    "R005": round_amounts,
}


def run_rules(
    txn: DataFrame, rules_cfg: dict[str, Any], country_dim: DataFrame | None = None
) -> DataFrame:
    """Run every enabled rule and union the hits."""
    hits: DataFrame | None = None
    for rule_id, fn in RULES.items():
        cfg = rules_cfg["rules"].get(rule_id)
        if not cfg or not cfg.get("enabled", True):
            continue
        out = fn(txn, cfg["params"], country_dim)
        hits = out if hits is None else hits.unionByName(out)
    if hits is None:
        return txn.sparkSession.createDataFrame(
            [], "rule_id string, account_id string, transaction_id string, event_ts timestamp, "
            "score double, evidence string"
        )
    return hits.withColumn(
        "hit_id",
        F.sha2(
            F.concat_ws("|", F.col("rule_id"), F.col("account_id"), F.to_date("event_ts").cast("string")),
            256,
        ),
    )
