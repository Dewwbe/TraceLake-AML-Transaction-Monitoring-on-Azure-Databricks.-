"""Table registry: the single source of truth for every table in the platform.

DDL (sql/ddl) and the data model documentation (docs/data_model.md) are generated from
this module by ``scripts/generate_docs.py``. Do not hand-edit the generated files.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Col:
    name: str
    dtype: str
    desc: str


@dataclass(frozen=True)
class TableSpec:
    name: str  # "<schema>.<table>"
    layer: str
    description: str
    cols: tuple[Col, ...]
    pk: tuple[str, ...]
    load: str  # load pattern, shown in docs

    @property
    def schema_name(self) -> str:
        return self.name.split(".")[0]

    def ddl_columns(self) -> str:
        """Column list in Spark DDL form, e.g. ``a string, b int``."""
        return ", ".join(f"{c.name} {c.dtype}" for c in self.cols)

    def column_names(self) -> list[str]:
        return [c.name for c in self.cols]

    def create_sql(self, catalog: str = "${catalog}") -> str:
        body = ",\n".join(f"  {c.name} {c.dtype} COMMENT '{c.desc.replace(chr(39), '')}'" for c in self.cols)
        return (
            f"CREATE TABLE IF NOT EXISTS {catalog}.{self.name} (\n{body}\n)\n"
            f"USING DELTA\nCOMMENT '{self.description.replace(chr(39), '')}';\n"
        )


S = "string"

_META = (
    Col("_source_file", S, "Landing file the row came from"),
    Col("_ingested_at", "timestamp", "Time the row was ingested into Bronze"),
)

TABLES: dict[str, TableSpec] = {}


def _t(spec: TableSpec) -> None:
    TABLES[spec.name] = spec


# ----------------------------------------------------------------------------- BRONZE
_t(TableSpec(
    "bronze.transactions_raw", "bronze",
    "Raw transactions exactly as received (SAML-D column layout), all strings.",
    (
        Col("Time", S, "Source time of day"),
        Col("Date", S, "Source date"),
        Col("Sender_account", S, "Sender account id"),
        Col("Receiver_account", S, "Receiver account id"),
        Col("Amount", S, "Amount in the payment currency"),
        Col("Payment_currency", S, "Currency sent"),
        Col("Received_currency", S, "Currency received"),
        Col("Sender_bank_location", S, "Sender bank country"),
        Col("Receiver_bank_location", S, "Receiver bank country"),
        Col("Payment_type", S, "Payment channel"),
        Col("Is_laundering", S, "Ground-truth label (evaluation only, never used in detection)"),
        Col("Laundering_type", S, "Ground-truth typology (evaluation only)"),
        *_META,
    ),
    ("_source_file", "Sender_account", "Receiver_account", "Date", "Time", "Amount"),
    "Append-only stream (Auto Loader or file stream, availableNow trigger)",
))
_t(TableSpec(
    "bronze.customers_raw", "bronze", "Raw synthetic KYC customer snapshots, all strings.",
    (
        Col("customer_id", S, "Customer identifier"),
        Col("full_name", S, "Customer name as captured"),
        Col("date_of_birth", S, "Date of birth (ISO)"),
        Col("nationality", S, "Nationality"),
        Col("residence_country", S, "Country of residence"),
        Col("occupation", S, "Occupation"),
        Col("onboarding_date", S, "Onboarding date (ISO)"),
        *_META,
    ),
    ("customer_id", "_source_file"),
    "Batch, new files only (anti-join on _source_file)",
))
_t(TableSpec(
    "bronze.accounts_raw", "bronze", "Raw account-to-customer mapping, all strings.",
    (
        Col("account_id", S, "Account identifier"),
        Col("customer_id", S, "Owning customer"),
        Col("account_open_date", S, "Account opening date (ISO)"),
        *_META,
    ),
    ("account_id", "_source_file"),
    "Batch, new files only (anti-join on _source_file)",
))
_t(TableSpec(
    "bronze.sanctions_raw", "bronze", "Dated sanctions-list snapshots in canonical layout, all strings.",
    (
        Col("list_source", S, "List identifier, e.g. ofac_sdn"),
        Col("snapshot_date", S, "Snapshot date taken from the landing path"),
        Col("record_type", S, "primary or alias"),
        Col("ent_num", S, "List entry number"),
        Col("alt_num", S, "Alias number, null for primary names"),
        Col("name", S, "Listed name or alias"),
        Col("entity_type", S, "individual, entity, vessel, ..."),
        Col("program", S, "Sanctions programme"),
        Col("remarks", S, "Free-text remarks"),
        *_META,
    ),
    ("list_source", "snapshot_date", "ent_num", "alt_num", "name"),
    "Batch, new files only; every snapshot is kept",
))
_t(TableSpec(
    "bronze.country_risk_raw", "bronze", "Dated country-risk list snapshots, all strings.",
    (
        Col("country", S, "Country name as used in the transaction source"),
        Col("risk_tier", S, "LOW, MEDIUM or HIGH"),
        Col("source_list", S, "Originating list, e.g. FATF call for action"),
        Col("effective_date", S, "Date the classification took effect"),
        *_META,
    ),
    ("country", "effective_date", "_source_file"),
    "Batch, new files only",
))

# ----------------------------------------------------------------------------- SILVER
_TXN_COLS = (
    Col("transaction_id", S, "Deterministic hash of the natural transaction fields"),
    Col("event_ts", "timestamp", "Transaction timestamp (UTC)"),
    Col("event_date", "date", "Transaction date"),
    Col("sender_account", S, "Sender account id"),
    Col("receiver_account", S, "Receiver account id"),
    Col("amount_original", "double", "Amount in the sent currency"),
    Col("currency_sent", S, "ISO currency sent"),
    Col("currency_received", S, "ISO currency received"),
    Col("amount_aed", "double", "Amount converted to AED"),
    Col("sender_country", S, "Sender bank country (upper case)"),
    Col("receiver_country", S, "Receiver bank country (upper case)"),
    Col("payment_type", S, "Payment channel"),
    Col("is_laundering", "int", "Ground-truth label, evaluation only"),
    Col("laundering_type", S, "Ground-truth typology, evaluation only"),
    Col("_ingested_at", "timestamp", "Bronze ingestion time (incremental watermark)"),
)
_t(TableSpec(
    "silver.transactions", "silver",
    "Typed, deduplicated, validated transactions with AED amounts.",
    _TXN_COLS, ("transaction_id",),
    "Incremental on _ingested_at watermark; insert-only on transaction_id",
))
_t(TableSpec(
    "silver.transactions_quarantine", "silver",
    "Transactions that failed an error-level data quality rule, with the failed rule ids.",
    (
        *_TXN_COLS,
        Col("raw_event_ts", S, "Original timestamp text"),
        Col("raw_amount", S, "Original amount text"),
        Col("dq_failed_rules", "array<string>", "Ids of the rules the row failed"),
        Col("quarantined_at", "timestamp", "Quarantine time"),
        Col("run_id", S, "Pipeline run id"),
    ),
    ("transaction_id", "run_id"),
    "Append per run",
))
_t(TableSpec(
    "silver.customers", "silver", "Latest standardised customer snapshot with normalised name.",
    (
        Col("customer_id", S, "Customer identifier"),
        Col("full_name", S, "Customer name"),
        Col("name_norm", S, "Normalised name used for screening"),
        Col("date_of_birth", "date", "Date of birth"),
        Col("nationality", S, "Nationality (upper case)"),
        Col("residence_country", S, "Country of residence (upper case)"),
        Col("occupation", S, "Occupation"),
        Col("onboarding_date", "date", "Onboarding date"),
    ),
    ("customer_id",), "Overwrite from latest Bronze snapshot",
))
_t(TableSpec(
    "silver.accounts", "silver", "Account-to-customer mapping.",
    (
        Col("account_id", S, "Account identifier"),
        Col("customer_id", S, "Owning customer"),
        Col("account_open_date", "date", "Account opening date"),
    ),
    ("account_id",), "Overwrite from Bronze",
))
_t(TableSpec(
    "silver.sanctions_entries", "silver",
    "Normalised sanctions entries, all snapshots. Screening uses the latest snapshot per list.",
    (
        Col("entry_id", S, "list_source|ent_num|alt_num"),
        Col("list_source", S, "List identifier"),
        Col("ent_num", S, "List entry number"),
        Col("record_type", S, "primary or alias"),
        Col("name", S, "Listed name or alias"),
        Col("name_norm", S, "Normalised name"),
        Col("entity_type", S, "Entity type"),
        Col("program", S, "Sanctions programme"),
        Col("snapshot_date", "date", "Snapshot date"),
    ),
    ("entry_id", "snapshot_date"), "Insert-only on (entry_id, snapshot_date)",
))
_t(TableSpec(
    "silver.rule_hits", "silver", "Raw behavioural rule hits before alert construction.",
    (
        Col("hit_id", S, "Hash of rule, account and date"),
        Col("rule_id", S, "Rule identifier"),
        Col("account_id", S, "Account that triggered the rule"),
        Col("transaction_id", S, "Representative transaction, null for account-level hits"),
        Col("event_ts", "timestamp", "Event time of the hit"),
        Col("score", "double", "Rule score between 0 and 1"),
        Col("evidence", S, "JSON evidence"),
        Col("run_id", S, "Pipeline run id"),
    ),
    ("hit_id",), "Incremental with lookback; insert-only on hit_id",
))

# ----------------------------------------------------------------------------- GOLD
_t(TableSpec(
    "gold.dim_customer", "gold", "Customer dimension, SCD Type 2 on risk and profile attributes.",
    (
        Col("customer_key", "bigint", "Surrogate key (hash of customer_id and valid_from)"),
        Col("customer_id", S, "Natural key"),
        Col("full_name", S, "Customer name"),
        Col("name_norm", S, "Normalised name"),
        Col("date_of_birth", "date", "Date of birth"),
        Col("nationality", S, "Nationality"),
        Col("residence_country", S, "Country of residence"),
        Col("occupation", S, "Occupation"),
        Col("onboarding_date", "date", "Onboarding date"),
        Col("risk_score", "double", "Derived customer risk score (illustrative weights)"),
        Col("risk_rating", S, "LOW, MEDIUM or HIGH"),
        Col("row_hash", S, "Hash of tracked attributes for change detection"),
        Col("valid_from", "timestamp", "Version start (logical time)"),
        Col("valid_to", "timestamp", "Version end, null while current"),
        Col("is_current", "boolean", "True for the current version"),
    ),
    ("customer_key",), "SCD2: MERGE on Delta, rebuild for local parquet",
))
_t(TableSpec(
    "gold.dim_country", "gold", "Country risk dimension, SCD Type 2 on risk tier.",
    (
        Col("country_key", "bigint", "Surrogate key"),
        Col("country", S, "Country name as used in the transaction source"),
        Col("risk_tier", S, "LOW, MEDIUM or HIGH"),
        Col("source_list", S, "Originating list"),
        Col("row_hash", S, "Hash of tracked attributes"),
        Col("valid_from", "timestamp", "Version start"),
        Col("valid_to", "timestamp", "Version end, null while current"),
        Col("is_current", "boolean", "True for the current version"),
    ),
    ("country_key",), "SCD2",
))
_t(TableSpec(
    "gold.dim_account", "gold", "Account dimension (SCD Type 1).",
    (
        Col("account_id", S, "Account identifier"),
        Col("customer_id", S, "Owning customer"),
        Col("account_open_date", "date", "Account opening date"),
    ),
    ("account_id",), "Overwrite",
))
_t(TableSpec(
    "gold.dim_date", "gold", "Calendar dimension.",
    (
        Col("date_key", "int", "yyyymmdd"),
        Col("date", "date", "Calendar date"),
        Col("year", "int", "Year"),
        Col("quarter", "int", "Quarter"),
        Col("month", "int", "Month"),
        Col("day", "int", "Day of month"),
        Col("day_of_week", "int", "1 = Sunday ... 7 = Saturday (Spark dayofweek)"),
        Col("is_weekend", "boolean", "Saturday or Sunday"),
    ),
    ("date_key",), "Overwrite over the data date range",
))
_t(TableSpec(
    "gold.dim_rule", "gold", "Detection rule catalogue generated from config/rules.yml.",
    (
        Col("rule_id", S, "Rule identifier"),
        Col("rule_name", S, "Short name"),
        Col("typology", S, "Money laundering typology"),
        Col("description", S, "What the rule detects"),
        Col("enabled", "boolean", "Whether the rule is active"),
        Col("parameters", S, "JSON parameters (illustrative thresholds)"),
    ),
    ("rule_id",), "Overwrite",
))
_t(TableSpec(
    "gold.fact_transaction", "gold", "Transaction fact (grain: one transaction).",
    (
        Col("transaction_id", S, "Degenerate key"),
        Col("date_key", "int", "FK to dim_date"),
        Col("event_ts", "timestamp", "Transaction timestamp"),
        Col("sender_account", S, "FK to dim_account"),
        Col("receiver_account", S, "FK to dim_account"),
        Col("sender_country", S, "Sender bank country"),
        Col("receiver_country", S, "Receiver bank country"),
        Col("payment_type", S, "Payment channel"),
        Col("amount_aed", "double", "Amount in AED"),
        Col("is_laundering", "int", "Ground-truth label, evaluation only"),
    ),
    ("transaction_id",), "Insert-only on transaction_id",
))
_t(TableSpec(
    "gold.fact_screening", "gold", "Sanctions screening results at review or strong level.",
    (
        Col("screening_id", S, "Hash of customer, entry and list snapshot"),
        Col("customer_id", S, "Screened customer"),
        Col("entry_id", S, "Matched list entry"),
        Col("list_source", S, "List identifier"),
        Col("customer_name", S, "Normalised customer name"),
        Col("matched_name", S, "Normalised listed name"),
        Col("score", "double", "Similarity score 0-100"),
        Col("band", S, "REVIEW or STRONG"),
        Col("list_snapshot_date", "date", "Snapshot the match was made against"),
        Col("scored_at", "timestamp", "Logical time of scoring"),
        Col("run_id", S, "Pipeline run id"),
    ),
    ("screening_id",), "Insert-only on screening_id",
))
_t(TableSpec(
    "gold.fact_alert", "gold", "Alerts from behavioural rules and sanctions screening.",
    (
        Col("alert_id", S, "Deterministic alert id (idempotent re-runs)"),
        Col("alert_type", S, "BEHAVIOURAL or SCREENING"),
        Col("rule_id", S, "Rule id, S001 for screening"),
        Col("alert_ts", "timestamp", "Event time of the alert"),
        Col("alert_date_key", "int", "FK to dim_date"),
        Col("customer_id", S, "Customer the alert relates to"),
        Col("customer_key", "bigint", "Point-in-time FK to dim_customer"),
        Col("risk_rating_at_alert", S, "Customer rating in force at alert time"),
        Col("account_id", S, "Account, null for screening alerts"),
        Col("transaction_id", S, "Representative transaction, may be null"),
        Col("score", "double", "Score between 0 and 1"),
        Col("band", S, "REVIEW or STRONG for screening, null otherwise"),
        Col("evidence", S, "JSON evidence"),
        Col("status", S, "NEW at creation"),
        Col("case_id", S, "Case the alert belongs to"),
        Col("run_id", S, "Pipeline run id"),
    ),
    ("alert_id",), "Insert-only on alert_id",
))
_t(TableSpec(
    "gold.fact_case", "gold",
    "Cases grouping alerts per customer per week. Workflow fields are SIMULATED in this repository.",
    (
        Col("case_id", S, "Hash of customer and week"),
        Col("customer_id", S, "Customer"),
        Col("customer_key", "bigint", "Point-in-time customer key of the first alert"),
        Col("risk_rating", S, "Rating at the first alert"),
        Col("opened_ts", "timestamp", "Earliest alert time"),
        Col("priority", S, "CRITICAL, HIGH, MEDIUM or LOW"),
        Col("status", S, "OPEN, IN_REVIEW or CLOSED"),
        Col("alert_count", "int", "Number of alerts in the case"),
        Col("max_score", "double", "Highest alert score"),
        Col("assigned_to", S, "Simulated analyst"),
        Col("disposition", S, "Simulated outcome: ESCALATED or FALSE_POSITIVE"),
        Col("closed_ts", "timestamp", "Simulated closure time"),
    ),
    ("case_id",), "Rebuilt each run from fact_alert",
))
_t(TableSpec(
    "gold.mart_rule_performance", "gold",
    "Rule precision and recall at account level against ground-truth labels.",
    (
        Col("run_id", S, "Pipeline run id"),
        Col("rule_id", S, "Rule id or ANY"),
        Col("alerted_accounts", "bigint", "Distinct accounts alerted"),
        Col("true_positive_accounts", "bigint", "Alerted accounts that are laundering accounts"),
        Col("positive_accounts", "bigint", "All laundering accounts"),
        Col("precision", "double", "True positives divided by alerted accounts"),
        Col("recall", "double", "True positives divided by positive accounts"),
        Col("computed_at", "timestamp", "Computation time"),
    ),
    ("run_id", "rule_id"), "Append per run",
))

# ----------------------------------------------------------------------------- OPS
_t(TableSpec(
    "ops.dq_results", "ops", "Data quality results per rule per run.",
    (
        Col("run_id", S, "Pipeline run id"),
        Col("table_name", S, "Table validated"),
        Col("rule_id", S, "Rule identifier"),
        Col("rule_type", S, "not_null, unique, range, in_set, regex or expression"),
        Col("severity", S, "error or warn"),
        Col("action", S, "quarantine or flag"),
        Col("rows_checked", "bigint", "Rows evaluated"),
        Col("rows_failed", "bigint", "Rows that failed"),
        Col("fail_pct", "double", "Failure percentage"),
        Col("checked_at", "timestamp", "Check time"),
    ),
    ("run_id", "table_name", "rule_id"), "Append per run",
))
_t(TableSpec(
    "ops.watermarks", "ops", "Incremental processing watermarks.",
    (
        Col("pipeline", S, "Watermark name"),
        Col("watermark", "timestamp", "Last processed timestamp"),
        Col("updated_at", "timestamp", "Update time"),
    ),
    ("pipeline",), "Overwrite",
))
_t(TableSpec(
    "ops.pipeline_runs", "ops", "Stage execution log.",
    (
        Col("run_id", S, "Pipeline run id"),
        Col("stage", S, "Stage name"),
        Col("started_at", "timestamp", "Start time"),
        Col("finished_at", "timestamp", "End time"),
        Col("status", S, "SUCCESS or FAILED"),
        Col("message", S, "Error message if failed"),
    ),
    ("run_id", "stage"), "Append",
))


def spec(name: str) -> TableSpec:
    return TABLES[name]
