"""Table store abstraction.

All pipeline code reads and writes tables by logical name (``silver.transactions``) through a
``Store``. Two implementations share one interface:

* ``ParquetStore``: plain parquet files under a local directory. Used for local runs and tests.
* ``DeltaStore``: Delta tables in Unity Catalog. Used on Azure Databricks.

The Delta implementation cannot run outside a Databricks (or delta-spark) environment and has
not been executed in this repository's CI; see docs/assumptions_and_limitations.md.
"""

from __future__ import annotations

import os
import shutil
from abc import ABC, abstractmethod
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from aml.schemas import TABLES


def conform(df: DataFrame, name: str) -> DataFrame:
    """Project and cast a DataFrame to the registered schema (missing columns become null)."""
    cols = []
    for c in TABLES[name].cols:
        if c.name in df.columns:
            cols.append(F.col(c.name).cast(c.dtype).alias(c.name))
        else:
            cols.append(F.lit(None).cast(c.dtype).alias(c.name))
    return df.select(*cols)


class Store(ABC):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @abstractmethod
    def exists(self, name: str) -> bool: ...

    @abstractmethod
    def read(self, name: str) -> DataFrame: ...

    @abstractmethod
    def write(self, name: str, df: DataFrame, mode: str = "overwrite") -> None: ...

    @abstractmethod
    def append_new(self, name: str, df: DataFrame, key: Sequence[str]) -> None:
        """Insert only rows whose key is not already present (idempotent load)."""

    @abstractmethod
    def apply_scd2(self, name: str, expire: DataFrame, insert: DataFrame, key: str, sk: str) -> None:
        """Apply SCD2 changes: close the listed current rows, then add the new versions."""

    @abstractmethod
    def stream_to(self, df: DataFrame, name: str, checkpoint: str) -> None:
        """Run a streaming write to completion using the availableNow trigger."""

    def read_or_empty(self, name: str) -> DataFrame:
        if self.exists(name):
            return self.read(name)
        return self.spark.createDataFrame([], TABLES[name].ddl_columns())


class ParquetStore(Store):
    def __init__(self, spark: SparkSession, base_path: str) -> None:
        super().__init__(spark)
        self.base_path = base_path

    def _path(self, name: str) -> str:
        return os.path.join(self.base_path, *name.split("."))

    def exists(self, name: str) -> bool:
        return os.path.isdir(self._path(name))

    def read(self, name: str) -> DataFrame:
        return self.spark.read.parquet(self._path(name))

    def write(self, name: str, df: DataFrame, mode: str = "overwrite") -> None:
        df = conform(df, name)
        path = self._path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if mode == "append" or not os.path.isdir(path):
            df.write.mode("append" if os.path.isdir(path) else "overwrite").parquet(path)
            return
        # Overwrite safely even when df was derived from this very table.
        tmp = path + "__tmp"
        shutil.rmtree(tmp, ignore_errors=True)
        df.write.mode("overwrite").parquet(tmp)
        shutil.rmtree(path)
        os.rename(tmp, path)

    def append_new(self, name: str, df: DataFrame, key: Sequence[str]) -> None:
        df = conform(df, name)
        if self.exists(name):
            existing = self.read(name).select(*key).distinct()
            df = df.join(existing, list(key), "left_anti")
        self.write(name, df, mode="append")

    def apply_scd2(self, name: str, expire: DataFrame, insert: DataFrame, key: str, sk: str) -> None:
        from aml.transform.scd2 import scd2_apply_df

        current = self.read_or_empty(name)
        new_dim = scd2_apply_df(current, expire, conform(insert, name), sk)
        self.write(name, new_dim)

    def stream_to(self, df: DataFrame, name: str, checkpoint: str) -> None:
        path = self._path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        query = (
            conform(df, name)
            .writeStream.format("parquet")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()


class DeltaStore(Store):
    def __init__(self, spark: SparkSession, catalog: str) -> None:
        super().__init__(spark)
        self.catalog = catalog

    def _fq(self, name: str) -> str:
        schema, table = name.split(".")
        return f"{self.catalog}.{schema}.{table}"

    def exists(self, name: str) -> bool:
        return self.spark.catalog.tableExists(self._fq(name))

    def read(self, name: str) -> DataFrame:
        return self.spark.table(self._fq(name))

    def write(self, name: str, df: DataFrame, mode: str = "overwrite") -> None:
        conform(df, name).write.format("delta").mode(mode).saveAsTable(self._fq(name))

    def append_new(self, name: str, df: DataFrame, key: Sequence[str]) -> None:
        from delta.tables import DeltaTable

        df = conform(df, name)
        if not self.exists(name):
            self.write(name, df)
            return
        cond = " AND ".join(f"t.{k} <=> s.{k}" for k in key)
        (
            DeltaTable.forName(self.spark, self._fq(name))
            .alias("t")
            .merge(df.alias("s"), cond)
            .whenNotMatchedInsertAll()
            .execute()
        )

    def apply_scd2(self, name: str, expire: DataFrame, insert: DataFrame, key: str, sk: str) -> None:
        """Single atomic MERGE: matched rows are closed, unmatched (merge_key NULL) rows inserted."""
        from delta.tables import DeltaTable

        insert = conform(insert, name)
        if not self.exists(name):
            self.write(name, insert)
            return
        cols = TABLES[name].column_names()
        close_rows = expire.select(
            F.col(key).alias("merge_key"), F.col(sk).alias(sk), F.col("expire_ts")
        )
        new_rows = insert.withColumn("merge_key", F.lit(None).cast("string"))
        source = close_rows.unionByName(new_rows, allowMissingColumns=True)
        (
            DeltaTable.forName(self.spark, self._fq(name))
            .alias("t")
            .merge(source.alias("s"), f"t.{key} = s.merge_key AND t.is_current = true")
            .whenMatchedUpdate(set={"valid_to": "s.expire_ts", "is_current": "false"})
            .whenNotMatchedInsert(values={c: f"s.{c}" for c in cols})
            .execute()
        )

    def stream_to(self, df: DataFrame, name: str, checkpoint: str) -> None:
        query = (
            conform(df, name)
            .writeStream.option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(self._fq(name))
        )
        query.awaitTermination()


def make_store(spark: SparkSession, store: str, base_path: str, catalog: str) -> Store:
    if store == "delta":
        return DeltaStore(spark, catalog)
    return ParquetStore(spark, base_path)
