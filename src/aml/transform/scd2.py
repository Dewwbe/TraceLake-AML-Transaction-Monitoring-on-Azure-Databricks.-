"""SCD Type 2 helpers.

``scd2_changes`` compares an incoming snapshot with the current dimension rows and returns two
DataFrames: the current rows to close and the new versions to insert. The storage layer applies
them (``Store.apply_scd2``): a single MERGE on Delta, or ``scd2_apply_df`` for local parquet.

Logical time: ``effective_ts`` is the version boundary for changed rows. New keys use
``new_key_valid_from`` (for example the onboarding date) so that point-in-time joins on
historical events find a row.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def add_row_hash(df: DataFrame, tracked: Sequence[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def scd2_changes(
    current: DataFrame,
    incoming: DataFrame,
    key: str,
    sk: str,
    tracked: Sequence[str],
    effective_ts: Column,
    new_key_valid_from: Column | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Return ``(to_expire, to_insert)``.

    ``to_expire`` has columns ``key, sk, expire_ts``. ``to_insert`` has every incoming column
    plus ``row_hash, valid_from, valid_to, is_current`` and the surrogate key.
    """
    inc = add_row_hash(incoming, tracked)
    cur = current.filter(F.col("is_current")).select(
        F.col(key).alias("_ckey"),
        F.col(sk).alias("_csk"),
        F.col("row_hash").alias("_chash"),
    )
    joined = inc.join(cur, inc[key] == cur["_ckey"], "left")

    is_new = F.col("_ckey").isNull()
    is_changed = F.col("_ckey").isNotNull() & (F.col("_chash") != F.col("row_hash"))

    to_expire = joined.filter(is_changed).select(
        F.col(key), F.col("_csk").alias(sk), effective_ts.alias("expire_ts")
    )

    first_from = new_key_valid_from if new_key_valid_from is not None else effective_ts
    to_insert = (
        joined.filter(is_new | is_changed)
        .withColumn("_is_new", is_new)
        .drop("_ckey", "_csk", "_chash")
        .withColumn(
            "valid_from", F.when(F.col("_is_new"), first_from).otherwise(effective_ts).cast("timestamp")
        )
        .drop("_is_new")
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )
    to_insert = to_insert.withColumn(sk, F.xxhash64(F.col(key), F.col("valid_from").cast("string")))
    return to_expire, to_insert


def scd2_apply_df(current: DataFrame, expire: DataFrame, insert: DataFrame, sk: str) -> DataFrame:
    """Apply the change sets to the full dimension and return the new dimension."""
    exp = expire.select(F.col(sk).alias("_esk"), F.col("expire_ts").alias("_expire_ts"))
    closed = (
        current.join(exp, current[sk] == exp["_esk"], "left")
        .withColumn(
            "valid_to",
            F.when(F.col("_expire_ts").isNotNull(), F.col("_expire_ts")).otherwise(F.col("valid_to")),
        )
        .withColumn("is_current", F.col("is_current") & F.col("_expire_ts").isNull())
        .drop("_esk", "_expire_ts")
    )
    return closed.unionByName(insert.select(*closed.columns))
