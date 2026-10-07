"""Airflow DAG: orchestrates the AML pipeline as Databricks stage runs.

Each task triggers the Databricks job ``aml-stage-runner`` (defined in databricks.yml) with a
different ``stage`` parameter. Stage order mirrors aml.jobs.local_pipeline.

NOT EXECUTED: this file has been syntax-checked only. Operator argument names (``job_name``,
``python_named_params``) depend on the apache-airflow-providers-databricks version; verify them
against the installed provider before use. Requires an Airflow connection ``databricks_default``.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

JOB_NAME = "aml-stage-runner"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def stage(task_id: str, name: str) -> DatabricksRunNowOperator:
    return DatabricksRunNowOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        job_name=JOB_NAME,
        python_named_params={"stage": name, "run-id": "{{ run_id }}"},
    )


with DAG(
    dag_id="aml_monitoring_batch",
    description="AML monitoring: reference refresh, silver, DQ gate, detection, alerts, marts",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",  # tune: hourly for near-real-time monitoring
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["aml", "databricks"],
) as dag:
    fetch = stage("fetch_sanctions", "fetch_sanctions")
    freshness = stage("check_freshness", "check_freshness")
    bronze_ref = stage("bronze_reference", "bronze_reference")
    silver_txn = stage("silver_transactions", "silver_transactions")
    silver_ref = stage("silver_reference", "silver_reference")
    gate = stage("dq_gate", "dq_gate")
    country = stage("update_country_dim", "update_country_dim")
    scr = stage("screening", "screening")
    cust_initial = stage("update_customer_dim_initial", "update_customer_dim")
    rules = stage("detect_rules", "detect_rules")
    alerts = stage("build_alerts", "build_alerts")
    cust_risk = stage("update_customer_dim_risk", "update_customer_dim")
    marts = stage("build_marts", "build_marts")

    # Streaming ingestion (bronze_transactions) runs as its own job; Airflow only checks freshness.
    freshness >> silver_txn
    fetch >> bronze_ref >> silver_ref
    [silver_txn, silver_ref] >> gate >> country >> scr >> cust_initial >> rules >> alerts >> cust_risk >> marts
