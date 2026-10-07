"""Pipeline stages. Each stage is idempotent and communicates only through tables.

The same functions run on Databricks (one stage per Airflow-triggered job run) and locally
(``aml.jobs.local_pipeline``). ``Ctx.as_of`` is the pipeline's LOGICAL time, the latest event time
seen so far. For a replayed historical dataset it differs from wall-clock time, and it is the
effective timestamp for SCD2 changes.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from aml.alerts.alerts_cases import assign_cases, build_alerts, derive_customer_risk
from aml.common.logging import get_logger
from aml.config import Settings, load_yaml
from aml.detection.rules import run_rules
from aml.detection.screening import latest_snapshot, screen_customers
from aml.evaluation.metrics import rule_performance, simulate_dispositions
from aml.ingestion.bronze import ingest_batch_files, read_transactions_stream
from aml.ingestion.ofac import ofac_to_canonical
from aml.quality.dq_framework import RESULT_SCHEMA, apply_rules, enforce_gate, load_rules
from aml.schemas import TABLES
from aml.store import Store
from aml.transform.scd2 import scd2_changes
from aml.transform.silver import (
    adapt_transactions,
    dedupe_by,
    fx_table,
    silver_customers,
    silver_sanctions,
    try_cast,
)

log = get_logger("aml.stages")

DIM_CUSTOMER_TRACKED = ["full_name", "nationality", "residence_country", "occupation", "risk_rating"]
DIM_COUNTRY_TRACKED = ["risk_tier", "source_list"]
OFAC_SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"  # UNVERIFIED: confirm current URL
OFAC_ALT_URL = "https://www.treasury.gov/ofac/downloads/alt.csv"  # UNVERIFIED


@dataclass
class Ctx:
    spark: SparkSession
    store: Store
    cfg: Settings
    run_id: str
    run_ts: datetime

    @property
    def landing(self) -> str:
        return self.cfg.landing_path

    def checkpoint(self, name: str) -> str:
        return f"{self.cfg.checkpoint_path}/{name}"

    def profile(self) -> dict[str, Any]:
        return load_yaml(self.cfg.config_file("sources", f"{self.cfg.source_profile}.yml"))


# ------------------------------------------------------------------ helpers
def get_watermark(ctx: Ctx, name: str) -> datetime | None:
    if not ctx.store.exists("ops.watermarks"):
        return None
    row = ctx.store.read("ops.watermarks").filter(F.col("pipeline") == name).collect()
    return row[0]["watermark"] if row else None


def set_watermark(ctx: Ctx, name: str, value: datetime) -> None:
    others = ctx.store.read_or_empty("ops.watermarks").filter(F.col("pipeline") != name)
    new = ctx.spark.createDataFrame(
        [(name, value, ctx.run_ts)], "pipeline string, watermark timestamp, updated_at timestamp"
    )
    ctx.store.write("ops.watermarks", others.unionByName(new))


def logical_now(ctx: Ctx) -> datetime:
    """Latest event time seen in Silver transactions, else the run timestamp."""
    if ctx.store.exists("silver.transactions"):
        row = ctx.store.read("silver.transactions").agg(F.max("event_ts").alias("m")).collect()[0]
        if row["m"] is not None:
            return row["m"]
    return ctx.run_ts


def _record_dq(ctx: Ctx, rows: list[dict[str, Any]]) -> None:
    ctx.store.write("ops.dq_results", ctx.spark.createDataFrame(rows, RESULT_SCHEMA), mode="append")


# ------------------------------------------------------------------ ingestion
def bronze_transactions(ctx: Ctx) -> None:
    profile = ctx.profile()
    stream = read_transactions_stream(
        ctx.spark, f"{ctx.landing}/transactions", profile["source_columns"],
        ctx.checkpoint("bronze_transactions"), ctx.cfg.use_autoloader,
    )
    ctx.store.stream_to(stream, "bronze.transactions_raw", ctx.checkpoint("bronze_transactions"))


def bronze_customers(ctx: Ctx) -> None:
    cols = ["customer_id", "full_name", "date_of_birth", "nationality", "residence_country",
            "occupation", "onboarding_date"]
    n = ingest_batch_files(ctx.spark, ctx.store, "bronze.customers_raw", f"{ctx.landing}/customers", cols)
    acc = ingest_batch_files(ctx.spark, ctx.store, "bronze.accounts_raw", f"{ctx.landing}/accounts",
                             ["account_id", "customer_id", "account_open_date"])
    log.info("bronze customers/accounts", extra={"ctx": {"customers": n, "accounts": acc}})


def bronze_sanctions(ctx: Ctx) -> None:
    pattern = r".*/sanctions/([^/]+)/(\d{4}-\d{2}-\d{2})/.*"
    extra = {
        "list_source": F.regexp_extract(F.col("_source_file"), pattern, 1),
        "snapshot_date": F.regexp_extract(F.col("_source_file"), pattern, 2),
    }
    cols = ["ent_num", "record_type", "alt_num", "name", "entity_type", "program", "remarks"]
    ingest_batch_files(ctx.spark, ctx.store, "bronze.sanctions_raw", f"{ctx.landing}/sanctions", cols, extra)


def bronze_country_risk(ctx: Ctx) -> None:
    cols = ["country", "risk_tier", "source_list", "effective_date"]
    ingest_batch_files(ctx.spark, ctx.store, "bronze.country_risk_raw", f"{ctx.landing}/country_risk", cols)


def bronze_reference(ctx: Ctx) -> None:
    bronze_customers(ctx)
    bronze_sanctions(ctx)
    bronze_country_risk(ctx)


def fetch_sanctions(ctx: Ctx) -> None:
    """Download OFAC SDN/ALT files and land them in canonical layout (needs internet egress)."""
    snapshot = ctx.run_ts.strftime("%Y-%m-%d")
    dest = Path(ctx.landing) / "sanctions" / "ofac_sdn" / snapshot
    dest.mkdir(parents=True, exist_ok=True)
    sdn, alt = dest / "sdn.csv", dest / "alt.csv"
    urllib.request.urlretrieve(OFAC_SDN_URL, sdn)  # noqa: S310
    urllib.request.urlretrieve(OFAC_ALT_URL, alt)  # noqa: S310
    ofac_to_canonical(str(sdn), str(alt)).to_csv(dest / "entries.csv", index=False)
    sdn.unlink()
    alt.unlink()


# ------------------------------------------------------------------ silver
def silver_transactions(ctx: Ctx) -> None:
    profile = ctx.profile()
    fx = fx_table(ctx.spark, f"{ctx.cfg.reference_dir}/fx_rates_illustrative.csv")
    bronze = ctx.store.read("bronze.transactions_raw")
    wm = get_watermark(ctx, "silver_transactions")
    if wm is not None:
        bronze = bronze.filter(F.col("_ingested_at") > F.lit(wm))
    if bronze.limit(1).count() == 0:
        log.info("silver_transactions: nothing new")
        return

    adapted = dedupe_by(adapt_transactions(bronze, profile, fx), "transaction_id")
    rules, _ = load_rules(ctx.cfg.config_file("dq_rules.yml"), "silver.transactions")
    outcome = apply_rules(ctx.spark, adapted, rules, "silver.transactions", ctx.run_id)
    _record_dq(ctx, outcome.result_rows)

    new_wm = bronze.agg(F.max("_ingested_at")).collect()[0][0]
    ctx.store.append_new("silver.transactions", outcome.valid, ["transaction_id"])
    quarantined = (
        outcome.quarantine.withColumn("quarantined_at", F.lit(ctx.run_ts).cast("timestamp"))
        .withColumn("run_id", F.lit(ctx.run_id))
    )
    ctx.store.write("silver.transactions_quarantine", quarantined, mode="append")
    set_watermark(ctx, "silver_transactions", new_wm)


def silver_reference(ctx: Ctx) -> None:
    cust = silver_customers(ctx.store.read("bronze.customers_raw"))
    rules, _ = load_rules(ctx.cfg.config_file("dq_rules.yml"), "silver.customers")
    outcome = apply_rules(ctx.spark, cust, rules, "silver.customers", ctx.run_id)
    _record_dq(ctx, outcome.result_rows)
    ctx.store.write("silver.customers", outcome.valid)
    acc = ctx.store.read("bronze.accounts_raw")
    ctx.store.write(
        "silver.accounts",
        dedupe_by(acc, "account_id").select(
            "account_id", "customer_id", try_cast(F.col("account_open_date"), "date").alias("account_open_date")
        ),
    )
    san = silver_sanctions(ctx.store.read("bronze.sanctions_raw"))
    ctx.store.append_new("silver.sanctions_entries", san, ["entry_id", "snapshot_date"])


def dq_gate(ctx: Ctx) -> None:
    """Fail the run if error-level failure rates for this run exceed the table thresholds."""
    results = ctx.store.read("ops.dq_results").filter(F.col("run_id") == ctx.run_id)
    rows = [r.asDict() for r in results.collect()]
    for table in {r["table_name"] for r in rows}:
        _, max_pct = load_rules(ctx.cfg.config_file("dq_rules.yml"), table)
        enforce_gate([r for r in rows if r["table_name"] == table], max_pct)


# ------------------------------------------------------------------ gold dimensions
def update_country_dim(ctx: Ctx) -> None:
    if not ctx.store.exists("bronze.country_risk_raw"):
        return
    raw = ctx.store.read("bronze.country_risk_raw")
    incoming = raw.select(
        F.upper(F.trim("country")).alias("country"),
        F.upper(F.trim("risk_tier")).alias("risk_tier"),
        F.trim("source_list").alias("source_list"),
    ).dropDuplicates(["country"])
    eff = F.lit(logical_now(ctx)).cast("timestamp")
    current = ctx.store.read_or_empty("gold.dim_country")
    expire, insert = scd2_changes(
        current, incoming, "country", "country_key", DIM_COUNTRY_TRACKED, eff,
        new_key_valid_from=F.lit("1900-01-01").cast("timestamp"),
    )
    ctx.store.apply_scd2("gold.dim_country", expire, insert, "country", "country_key")


# ------------------------------------------------------------------ detection
def screening(ctx: Ctx) -> None:
    cfg = load_yaml(ctx.cfg.config_file("screening.yml"))
    customers = ctx.store.read("silver.customers").select("customer_id", "name_norm")
    sanctions = latest_snapshot(ctx.store.read("silver.sanctions_entries"))
    if cfg.get("list_source_filter"):
        sanctions = sanctions.filter(F.col("list_source").isin(*cfg["list_source_filter"]))
    hits = screen_customers(customers, sanctions, cfg)
    out = (
        hits.withColumn(
            "screening_id",
            F.sha2(F.concat_ws("|", "customer_id", "entry_id", F.col("list_snapshot_date").cast("string")), 256),
        )
        .withColumn("scored_at", F.lit(logical_now(ctx)).cast("timestamp"))
        .withColumn("run_id", F.lit(ctx.run_id))
    )
    ctx.store.append_new("gold.fact_screening", out, ["screening_id"])


def detect_rules(ctx: Ctx) -> None:
    cfg = load_yaml(ctx.cfg.config_file("rules.yml"))
    txn = ctx.store.read("silver.transactions")
    wm = get_watermark(ctx, "rules")
    if wm is not None:
        lookback = F.expr(f"timestamp'{wm:%Y-%m-%d %H:%M:%S}' - interval {int(cfg['lookback_days'])} days")
        txn = txn.filter(F.col("event_ts") > lookback)
    country = ctx.store.read("gold.dim_country") if ctx.store.exists("gold.dim_country") else None
    hits = run_rules(txn, cfg, country)
    if wm is not None:
        hits = hits.filter(F.col("event_ts") > F.lit(wm))
    hits = hits.withColumn("run_id", F.lit(ctx.run_id))
    ctx.store.append_new("silver.rule_hits", hits, ["hit_id"])
    mx = ctx.store.read("silver.transactions").agg(F.max("event_ts")).collect()[0][0]
    if mx is not None:
        set_watermark(ctx, "rules", mx)


def build_alerts_stage(ctx: Ctx) -> None:
    hits = ctx.store.read("silver.rule_hits")
    scr = ctx.store.read_or_empty("gold.fact_screening")
    accounts = ctx.store.read("silver.accounts")
    dim = ctx.store.read_or_empty("gold.dim_customer")
    scored_at = F.lit(logical_now(ctx)).cast("timestamp")
    alerts = build_alerts(hits, scr, accounts, dim, ctx.run_id, scored_at)
    ctx.store.append_new("gold.fact_alert", alerts, ["alert_id"])


def update_customer_dim(ctx: Ctx) -> None:
    """Derive customer risk, then apply SCD2 changes (feedback loop: alerts -> rating -> history)."""
    cfg = load_yaml(ctx.cfg.config_file("rules.yml"))["customer_risk"]
    as_of = logical_now(ctx)
    customers = ctx.store.read("silver.customers")
    country_current = (
        ctx.store.read("gold.dim_country").filter("is_current")
        if ctx.store.exists("gold.dim_country")
        else ctx.store.read_or_empty("gold.dim_country")
    )
    scr = ctx.store.read_or_empty("gold.fact_screening")
    alerts = ctx.store.read_or_empty("gold.fact_alert")
    scored = derive_customer_risk(customers, country_current, scr, alerts, F.lit(as_of).cast("timestamp"), cfg)
    incoming = scored.select(
        "customer_id", "full_name", "name_norm", "date_of_birth", "nationality",
        "residence_country", "occupation", "onboarding_date", "risk_score", "risk_rating",
    )
    current = ctx.store.read_or_empty("gold.dim_customer")
    expire, insert = scd2_changes(
        current, incoming, "customer_id", "customer_key", DIM_CUSTOMER_TRACKED,
        F.lit(as_of).cast("timestamp"),
        new_key_valid_from=F.col("onboarding_date").cast("timestamp"),
    )
    ctx.store.apply_scd2("gold.dim_customer", expire, insert, "customer_id", "customer_key")


# ------------------------------------------------------------------ gold marts
def build_marts(ctx: Ctx, simulate_review: bool = True) -> None:
    txn = ctx.store.read("silver.transactions")
    ctx.store.write("gold.dim_account", ctx.store.read("silver.accounts"))

    bounds = txn.agg(F.min("event_date").alias("a"), F.max("event_date").alias("b")).collect()[0]
    if bounds["a"] is not None:
        days = ctx.spark.sql(
            f"SELECT explode(sequence(date'{bounds['a']}', date'{bounds['b']}', interval 1 day)) AS date"
        )
        ctx.store.write(
            "gold.dim_date",
            days.select(
                F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
                F.year("date").alias("year"), F.quarter("date").alias("quarter"),
                F.month("date").alias("month"), F.dayofmonth("date").alias("day"),
                F.dayofweek("date").alias("day_of_week"),
                F.dayofweek("date").isin(1, 7).alias("is_weekend"),
            ),
        )

    rules_cfg = load_yaml(ctx.cfg.config_file("rules.yml"))
    rule_rows = [
        (rid, r["name"], r["typology"], r["description"], bool(r.get("enabled", True)), json.dumps(r["params"]))
        for rid, r in rules_cfg["rules"].items()
    ]
    ctx.store.write(
        "gold.dim_rule",
        ctx.spark.createDataFrame(
            rule_rows,
            "rule_id string, rule_name string, typology string, description string, enabled boolean, parameters string",
        ),
    )

    ctx.store.append_new(
        "gold.fact_transaction",
        txn.select(
            "transaction_id", F.date_format("event_ts", "yyyyMMdd").cast("int").alias("date_key"),
            "event_ts", "sender_account", "receiver_account", "sender_country", "receiver_country",
            "payment_type", "amount_aed", "is_laundering",
        ),
        ["transaction_id"],
    )

    alerts = ctx.store.read_or_empty("gold.fact_alert")
    alerts, cases = assign_cases(alerts)
    if simulate_review:
        cases = simulate_dispositions(cases, alerts, txn)
    # Write cases first: both frames are lazily derived from the current fact_alert files, which
    # are replaced by the second write (a rebuilt table must not be read after it is swapped).
    ctx.store.write("gold.fact_case", cases)
    ctx.store.write("gold.fact_alert", alerts)

    hits = ctx.store.read_or_empty("silver.rule_hits")
    if hits.limit(1).count():
        ctx.store.write("gold.mart_rule_performance", rule_performance(hits, txn, ctx.run_id), mode="append")


def check_freshness(ctx: Ctx, max_age_hours: float = 24.0) -> None:
    """Fail if no Bronze rows have arrived within ``max_age_hours`` of the run time."""
    row = ctx.store.read("bronze.transactions_raw").agg(F.max("_ingested_at")).collect()[0][0]
    if row is None:
        raise RuntimeError("Freshness check failed: bronze.transactions_raw is empty")
    age = (ctx.run_ts.replace(tzinfo=None) - row.replace(tzinfo=None)).total_seconds() / 3600
    if age > max_age_hours:
        raise RuntimeError(f"Freshness check failed: last ingestion {age:.1f}h ago")


def init_tables(ctx: Ctx) -> None:
    """Create schemas and Delta tables with comments (Databricks only; no-op for parquet)."""
    if ctx.cfg.store != "delta":
        return
    for schema in sorted({t.schema_name for t in TABLES.values()}):
        ctx.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.cfg.catalog}.{schema}")
    for spec in TABLES.values():
        ctx.spark.sql(spec.create_sql(ctx.cfg.catalog))


STAGES: dict[str, Callable[[Ctx], None]] = {
    "init_tables": init_tables,
    "fetch_sanctions": fetch_sanctions,
    "bronze_transactions": bronze_transactions,
    "bronze_reference": bronze_reference,
    "silver_transactions": silver_transactions,
    "silver_reference": silver_reference,
    "dq_gate": dq_gate,
    "update_country_dim": update_country_dim,
    "screening": screening,
    "detect_rules": detect_rules,
    "update_customer_dim": update_customer_dim,
    "build_alerts": build_alerts_stage,
    "build_marts": build_marts,
    "check_freshness": check_freshness,
}



