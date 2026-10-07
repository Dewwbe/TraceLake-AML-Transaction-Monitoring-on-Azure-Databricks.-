"""SparkSession factory."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "aml", local: bool = False) -> SparkSession:
    """Return a SparkSession.

    On Databricks the active session is reused. For local runs a small
    single-node session is created with UTC as the session time zone.
    """
    builder = SparkSession.builder.appName(app_name)
    if local:
        builder = (
            builder.master("local[2]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "2g")
        )
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark
