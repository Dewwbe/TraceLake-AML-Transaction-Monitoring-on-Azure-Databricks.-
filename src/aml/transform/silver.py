"""Bronze -> Silver transformations (pure DataFrame functions, no I/O)."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from aml.transform.names import normalise_name

NULL_TOKENS = ["", "null", "NULL", "None", "nan", "NaN", "-0-"]


def try_cast(col: F.Column, dtype: str) -> F.Column:
    """Cast that yields null on bad input under both ANSI (Spark 4) and non-ANSI (Spark 3.5) modes."""
    if hasattr(col, "try_cast"):
        return col.try_cast(dtype)
    return col.cast(dtype)


def try_timestamp(col: F.Column, fmt: str) -> F.Column:
    fn = getattr(F, "try_to_timestamp", F.to_timestamp)
    return fn(col, F.lit(fmt))


def _clean(col: str) -> F.Column:
    c = F.trim(F.col(col))
    return F.when(c.isin(*NULL_TOKENS), F.lit(None)).otherwise(c)


def normalise_name_udf():
    return F.udf(normalise_name, StringType())


def fx_table(spark: SparkSession, path: str) -> DataFrame:
    """Load the currency -> AED rate table (CSV with header: currency,rate_to_aed)."""
    return (
        spark.read.option("header", True).csv(path)
        .select(F.upper(F.trim("currency")).alias("currency"), F.col("rate_to_aed").cast("double"))
    )


def adapt_transactions(bronze: DataFrame, profile: dict[str, Any], fx: DataFrame) -> DataFrame:
    """Map a source profile onto the canonical transaction schema (before data quality checks).

    Unparseable values become null (and are then caught by DQ rules); raw timestamp and amount
    text is kept for the quarantine table.
    """
    cols = profile["columns"]
    ts = profile["timestamp"]
    aliases = profile.get("currency_aliases", {})
    alias_map = F.create_map([F.lit(x) for kv in aliases.items() for x in kv]) if aliases else None

    def iso(col: str) -> F.Column:
        raw = _clean(col)
        mapped = alias_map[raw] if alias_map is not None else F.lit(None)
        return F.upper(F.coalesce(mapped, raw))

    raw_ts = F.concat_ws(" ", _clean(ts["date_col"]), _clean(ts["time_col"]))
    df = (
        bronze.select(
            raw_ts.alias("raw_event_ts"),
            _clean(cols["amount_original"]).alias("raw_amount"),
            try_timestamp(raw_ts, ts["format"]).alias("event_ts"),
            _clean(cols["sender_account"]).alias("sender_account"),
            _clean(cols["receiver_account"]).alias("receiver_account"),
            try_cast(_clean(cols["amount_original"]), "double").alias("amount_original"),
            iso(cols["currency_sent"]).alias("currency_sent"),
            iso(cols["currency_received"]).alias("currency_received"),
            F.upper(_clean(cols["sender_country"])).alias("sender_country"),
            F.upper(_clean(cols["receiver_country"])).alias("receiver_country"),
            _clean(cols["payment_type"]).alias("payment_type"),
            try_cast(_clean(cols["is_laundering"]), "int").alias("is_laundering"),
            _clean(cols["laundering_type"]).alias("laundering_type"),
            F.col("_ingested_at"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )
    df = df.withColumn(
        "transaction_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("raw_event_ts"),
                F.col("sender_account"),
                F.col("receiver_account"),
                F.col("raw_amount"),
                F.col("currency_sent"),
                F.col("payment_type"),
            ),
            256,
        ),
    )
    rates = F.broadcast(fx.select(F.col("currency").alias("_ccy"), F.col("rate_to_aed")))
    df = df.join(rates, df["currency_sent"] == F.col("_ccy"), "left").drop("_ccy")
    return df.withColumn("amount_aed", F.round(F.col("amount_original") * F.col("rate_to_aed"), 2)).drop(
        "rate_to_aed"
    )


def dedupe_by(df: DataFrame, key: str, order_col: str = "_ingested_at") -> DataFrame:
    """Keep the earliest-ingested row for each key."""
    from pyspark.sql import Window

    w = Window.partitionBy(key).orderBy(F.col(order_col).asc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def silver_customers(bronze: DataFrame) -> DataFrame:
    """Latest snapshot per customer, standardised."""
    from pyspark.sql import Window

    w = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    latest = bronze.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return latest.select(
        _clean("customer_id").alias("customer_id"),
        _clean("full_name").alias("full_name"),
        normalise_name_udf()(_clean("full_name")).alias("name_norm"),
        try_cast(_clean("date_of_birth"), "date").alias("date_of_birth"),
        F.upper(_clean("nationality")).alias("nationality"),
        F.upper(_clean("residence_country")).alias("residence_country"),
        _clean("occupation").alias("occupation"),
        try_cast(_clean("onboarding_date"), "date").alias("onboarding_date"),
    )


def silver_sanctions(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.concat_ws(
            "|",
            F.col("list_source"),
            F.col("ent_num"),
            F.coalesce(_clean("alt_num"), F.lit("0")),
        ).alias("entry_id"),
        F.col("list_source"),
        F.col("ent_num"),
        F.col("record_type"),
        _clean("name").alias("name"),
        normalise_name_udf()(_clean("name")).alias("name_norm"),
        _clean("entity_type").alias("entity_type"),
        _clean("program").alias("program"),
        try_cast(F.col("snapshot_date"), "date").alias("snapshot_date"),
    ).dropDuplicates(["entry_id", "snapshot_date"])
