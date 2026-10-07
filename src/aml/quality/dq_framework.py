"""Metadata-driven data quality framework.

Rules are declared in YAML (config/dq_rules.yml) and applied to a DataFrame. Rows that fail an
``action: quarantine`` rule are split off with the list of failed rule ids. ``action: flag``
rules never remove rows; they add the rule id to a ``dq_warnings`` array.

Supported rule types: not_null, unique, range, in_set, regex, expression.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from aml.config import load_yaml

RESULT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("table_name", StringType()),
        StructField("rule_id", StringType()),
        StructField("rule_type", StringType()),
        StructField("severity", StringType()),
        StructField("action", StringType()),
        StructField("rows_checked", LongType()),
        StructField("rows_failed", LongType()),
        StructField("fail_pct", DoubleType()),
        StructField("checked_at", TimestampType()),
    ]
)


class DQGateError(RuntimeError):
    """Raised when error-level failures exceed the configured threshold."""


@dataclass(frozen=True)
class DQRule:
    rule_id: str
    rule_type: str
    column: str | None = None
    columns: tuple[str, ...] = ()
    severity: str = "error"
    action: str = "quarantine"
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class DQOutcome:
    valid: DataFrame
    quarantine: DataFrame
    results: DataFrame
    result_rows: list[dict[str, Any]]


def load_rules(path: str | Path, table: str) -> tuple[list[DQRule], float]:
    """Load the rules and ``max_error_fail_pct`` for one table from a YAML file."""
    cfg = load_yaml(path)["tables"][table]
    rules = []
    for r in cfg["rules"]:
        rules.append(
            DQRule(
                rule_id=r["id"],
                rule_type=r["type"],
                column=r.get("column"),
                columns=tuple(r.get("columns", ())),
                severity=r.get("severity", "error"),
                action=r.get("action", "quarantine"),
                params=r.get("params", {}),
            )
        )
    return rules, float(cfg.get("max_error_fail_pct", 5.0))


def _failure_expr(rule: DQRule) -> Column:
    """Boolean column that is True when the row FAILS the rule (null results count as failures)."""
    t = rule.rule_type
    if t == "not_null":
        col = F.col(rule.column)
        return col.isNull() | (F.trim(col.cast("string")) == "")
    if t == "range":
        col = F.col(rule.column)
        p = rule.params
        conds = []
        if "gt" in p:
            conds.append(col > p["gt"])
        if "gte" in p:
            conds.append(col >= p["gte"])
        if "lt" in p:
            conds.append(col < p["lt"])
        if "lte" in p:
            conds.append(col <= p["lte"])
        passed = conds[0]
        for c in conds[1:]:
            passed = passed & c
        return F.coalesce(~passed, F.lit(True))
    if t == "in_set":
        return F.coalesce(~F.col(rule.column).isin(*rule.params["values"]), F.lit(True))
    if t == "regex":
        return F.coalesce(~F.col(rule.column).rlike(rule.params["pattern"]), F.lit(True))
    if t == "expression":
        return F.coalesce(~F.expr(rule.params["expr"]), F.lit(True))
    raise ValueError(f"Unsupported rule type: {t} (rule {rule.rule_id})")


def apply_rules(
    spark: SparkSession,
    df: DataFrame,
    rules: list[DQRule],
    table: str,
    run_id: str,
    now: datetime | None = None,
) -> DQOutcome:
    now = now or datetime.now(timezone.utc)
    original_cols = df.columns
    work = df

    fail_cols: dict[str, str] = {}
    for r in rules:
        fcol = f"__f_{r.rule_id}"
        fail_cols[r.rule_id] = fcol
        if r.rule_type == "unique":
            keys = list(r.columns) or [r.column]
            rn = F.row_number().over(Window.partitionBy(*keys).orderBy(F.lit(1)))
            work = work.withColumn(fcol, rn > 1)
        else:
            work = work.withColumn(fcol, _failure_expr(r))

    aggs = [F.count(F.lit(1)).alias("__n")] + [
        F.sum(F.col(fail_cols[r.rule_id]).cast("long")).alias(f"__s_{r.rule_id}") for r in rules
    ]
    row = work.agg(*aggs).collect()[0]
    total = int(row["__n"] or 0)

    result_rows: list[dict[str, Any]] = []
    for r in rules:
        failed = int(row[f"__s_{r.rule_id}"] or 0)
        result_rows.append(
            {
                "run_id": run_id,
                "table_name": table,
                "rule_id": r.rule_id,
                "rule_type": r.rule_type,
                "severity": r.severity,
                "action": r.action,
                "rows_checked": total,
                "rows_failed": failed,
                "fail_pct": (100.0 * failed / total) if total else 0.0,
                "checked_at": now,
            }
        )
    results = spark.createDataFrame(result_rows, RESULT_SCHEMA)

    def failed_ids(action: str) -> Column:
        parts = [
            F.when(F.col(fail_cols[r.rule_id]), F.lit(r.rule_id))
            for r in rules
            if r.action == action
        ]
        if not parts:
            return F.array().cast("array<string>")
        return F.filter(F.array(*parts), lambda x: x.isNotNull())

    work = work.withColumn("dq_failed_rules", failed_ids("quarantine")).withColumn(
        "dq_warnings", failed_ids("flag")
    )
    valid = (
        work.filter(F.size("dq_failed_rules") == 0)
        .select(*original_cols, "dq_warnings")
    )
    quarantine = work.filter(F.size("dq_failed_rules") > 0).select(*original_cols, "dq_failed_rules")
    return DQOutcome(valid=valid, quarantine=quarantine, results=results, result_rows=result_rows)


def evaluate_gate(result_rows: list[dict[str, Any]], max_error_fail_pct: float) -> list[str]:
    """Return breach messages for error-severity rules whose failure rate exceeds the threshold."""
    breaches = []
    for r in result_rows:
        if r["severity"] == "error" and r["fail_pct"] > max_error_fail_pct:
            breaches.append(
                f"{r['table_name']}.{r['rule_id']}: {r['fail_pct']:.2f}% > {max_error_fail_pct}%"
            )
    return breaches


def enforce_gate(result_rows: list[dict[str, Any]], max_error_fail_pct: float) -> None:
    breaches = evaluate_gate(result_rows, max_error_fail_pct)
    if breaches:
        raise DQGateError("Data quality gate failed: " + "; ".join(breaches))
