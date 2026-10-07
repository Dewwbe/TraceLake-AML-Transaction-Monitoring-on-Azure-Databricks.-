"""End-to-end run on synthetic data with local Spark and parquet tables (no Delta, no Databricks)."""

from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import functions as F

from aml.evaluation.metrics import screening_metrics
from aml.generators.replay import write_microbatches
from aml.generators.synthetic import (
    COUNTRIES,
    HIGH_RISK_DEMO,
    generate_customers,
    generate_fictional_sanctions,
    generate_transactions,
    seed_name_matches,
)
from aml.jobs import local_pipeline
from aml.jobs.run_stage import make_ctx

ROOT = Path(__file__).resolve().parents[1]
CHECK_TABLES = [
    "bronze.transactions_raw", "silver.transactions", "silver.transactions_quarantine",
    "silver.rule_hits", "gold.fact_transaction", "gold.fact_alert", "gold.fact_case",
    "gold.fact_screening", "gold.dim_customer", "gold.dim_country", "silver.sanctions_entries",
]


@pytest.fixture(scope="module")
def lake(tmp_path_factory, spark):
    base = tmp_path_factory.mktemp("e2e")
    landing = base / "landing"
    txn, acct = generate_transactions(n_accounts=120, n_normal=1500, seed=5)
    write_microbatches(txn, landing / "transactions", rows_per_file=500)
    customers, accounts = generate_customers(list(acct["account_id"]), seed=5)
    sanctions = generate_fictional_sanctions()
    listed = sanctions.loc[sanctions["record_type"] == "primary", "name"].tolist()
    customers, truth = seed_name_matches(customers, listed)
    for sub, df, name in [("customers", customers, "customers_1.csv"), ("accounts", accounts, "accounts_1.csv")]:
        (landing / sub).mkdir(parents=True)
        df.to_csv(landing / sub / name, index=False)
    (landing / "sanctions" / "fictional_list" / "2024-01-01").mkdir(parents=True)
    sanctions.to_csv(landing / "sanctions" / "fictional_list" / "2024-01-01" / "entries.csv", index=False)
    (landing / "country_risk").mkdir()
    pd.DataFrame([{"country": c, "risk_tier": "HIGH" if c in HIGH_RISK_DEMO else "LOW",
                   "source_list": "DEMO ONLY", "effective_date": "2024-01-01"} for c in COUNTRIES]
                 ).to_csv(landing / "country_risk" / "c.csv", index=False)
    truth.to_csv(base / "truth.csv", index=False)
    return base


@pytest.fixture(scope="module")
def ctx(lake, monkeypatch_module):
    env = {"AML_STORE": "parquet", "AML_BASE_PATH": str(lake / "lake"), "AML_LANDING_PATH": str(lake / "landing"),
           "AML_CHECKPOINT_PATH": str(lake / "chk"), "AML_CONFIG_DIR": str(ROOT / "config"),
           "AML_REFERENCE_DIR": str(ROOT / "reference")}
    for k, v in env.items():
        monkeypatch_module.setenv(k, v)
    return make_ctx()


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="module")
def counts_after_first_run(ctx):
    local_pipeline.main(reset=False)
    return {t: ctx.store.read(t).count() for t in CHECK_TABLES}


def test_quarantine_and_silver_counts_reconcile(ctx, counts_after_first_run):
    c = counts_after_first_run
    assert c["silver.transactions_quarantine"] == 5
    # raw = valid + quarantined + 1 exact duplicate
    assert c["bronze.transactions_raw"] == c["silver.transactions"] + 5 + 1


def test_every_rule_produces_hits(ctx, counts_after_first_run):
    rules = {r["rule_id"] for r in ctx.store.read("silver.rule_hits").select("rule_id").distinct().collect()}
    assert rules == {"R001", "R002", "R003", "R004", "R005"}


def test_screening_recall_on_seeded_names(ctx, lake, counts_after_first_run):
    truth = ctx.spark.read.option("header", True).csv(str(lake / "truth.csv"))
    m = screening_metrics(ctx.store.read("gold.fact_screening"), truth, ctx.store.read("silver.customers"))
    overall = m.filter("perturbation = 'ALL'").collect()[0]
    assert overall["recall"] >= 0.9


def test_scd2_invariants_hold(ctx, counts_after_first_run):
    dim = ctx.store.read("gold.dim_customer")
    bad = dim.groupBy("customer_id").agg(F.sum(F.col("is_current").cast("int")).alias("n")).filter("n != 1")
    assert bad.count() == 0
    from pyspark.sql import Window
    w = Window.partitionBy("customer_id").orderBy("valid_from")
    gaps = dim.withColumn("next_from", F.lead("valid_from").over(w)).filter(
        F.col("next_from").isNotNull() & (F.col("valid_to") != F.col("next_from")))
    assert gaps.count() == 0


def test_alerts_carry_point_in_time_customer_key_and_cases(ctx, counts_after_first_run):
    alerts = ctx.store.read("gold.fact_alert")
    assert alerts.filter("customer_key IS NULL").count() == 0
    assert alerts.filter("case_id IS NULL").count() == 0
    assert alerts.select("alert_id").distinct().count() == alerts.count()
    cases = ctx.store.read("gold.fact_case")
    assert cases.agg(F.sum("alert_count")).collect()[0][0] == alerts.count()


def test_second_run_changes_no_business_tables(ctx, counts_after_first_run):
    local_pipeline.main(reset=False)
    again = {t: ctx.store.read(t).count() for t in CHECK_TABLES}
    assert again == counts_after_first_run
