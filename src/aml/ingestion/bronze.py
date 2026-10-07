"""Bronze ingestion: raw, all-string, append-only, with source file and ingestion time."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from aml.store import Store


def _string_schema(columns: Sequence[str]) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in columns])


def read_transactions_stream(
    spark: SparkSession, landing_dir: str, source_columns: Sequence[str], checkpoint: str,
    use_autoloader: bool,
) -> DataFrame:
    """Streaming read of landed transaction CSVs.

    On Databricks, Auto Loader (``cloudFiles``) is used. Elsewhere a plain file stream is used with
    ``enforceSchema=false`` so the CSV header is validated against the expected columns.
    """
    if use_autoloader:
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaLocation", checkpoint + "/schema")
            .option("header", True)
            .load(landing_dir)
        )
    else:
        reader = (
            spark.readStream.format("csv")
            .schema(_string_schema(source_columns))
            .option("header", True)
            .option("enforceSchema", False)
            .load(landing_dir)
        )
    missing = [c for c in source_columns if c not in reader.columns]
    if missing:
        raise ValueError(f"Landing files are missing expected columns: {missing}")
    return reader.select(
        *[F.col(c).cast("string").alias(c) for c in source_columns],
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def ingest_batch_files(
    spark: SparkSession, store: Store, table: str, landing_dir: str, columns: Sequence[str],
    extra: dict[str, F.Column] | None = None,
) -> int:
    """Ingest CSV files not yet present in the Bronze table (anti-join on ``_source_file``)."""
    raw = (
        spark.read.schema(_string_schema(columns))
        .option("header", True)
        .option("enforceSchema", False)
        .option("recursiveFileLookup", True)
        .csv(landing_dir)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    for name, col in (extra or {}).items():
        raw = raw.withColumn(name, col)
    raw = raw.withColumn("_ingested_at", F.current_timestamp())
    if store.exists(table):
        seen = store.read(table).select("_source_file").distinct()
        raw = raw.join(seen, "_source_file", "left_anti")
    n = raw.count()
    if n:
        store.write(table, raw, mode="append")
    return n
