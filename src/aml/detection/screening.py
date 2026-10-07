"""Sanctions screening: blocking on phonetic token keys, then fuzzy scoring.

1. Candidate generation (blocking): customers and listed names are split into tokens; each token
   yields Soundex, prefix and suffix keys; a customer/entry pair is a candidate if enough distinct
   tokens match on any key (two when both names have at least two blocking tokens, otherwise one).
2. Scoring: ``aml.transform.names.score_names`` on the normalised names.
3. Banding: REVIEW / STRONG using thresholds from config/screening.yml.
"""


from typing import Any

import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from aml.transform.names import score_names


def _block_tokens(name_col: str, min_len: int, particles: list[str]) -> Column:
    tokens = F.split(F.col(name_col), r"\s+")
    return F.filter(
        tokens,
        lambda t: (F.length(t) >= min_len) & (~t.isin(*particles) if particles else F.lit(True)),
    )


def _keys(df: DataFrame, id_col: str, min_len: int, particles: list[str], prefix: str) -> DataFrame:
    """One row per (id, token, block key). Each token yields three keys: Soundex, first three
    letters and last three letters, so a single typo or transliteration difference rarely breaks
    every key at once (a transposition changes Soundex but usually not the prefix or suffix)."""
    toks = df.select(id_col, _block_tokens("name_norm", min_len, particles).alias("toks"))
    exploded = toks.select(id_col, F.size("toks").alias(f"{prefix}_ntok"), F.explode("toks").alias(f"{prefix}_tok"))
    tok = F.col(f"{prefix}_tok")
    keyed = exploded.select(
        id_col,
        f"{prefix}_ntok",
        f"{prefix}_tok",
        F.explode(
            F.array(
                F.concat(F.lit("S:"), F.soundex(tok)),
                F.concat(F.lit("P:"), F.substring(tok, 1, 3)),
                F.concat(F.lit("X:"), F.substring(tok, -3, 3)),
            )
        ).alias("block_key"),
    )
    return keyed.distinct()


def latest_snapshot(sanctions: DataFrame) -> DataFrame:
    """Keep only entries from the most recent snapshot of each list."""
    latest = sanctions.groupBy("list_source").agg(F.max("snapshot_date").alias("_latest"))
    return (
        sanctions.join(latest, "list_source")
        .filter(F.col("snapshot_date") == F.col("_latest"))
        .drop("_latest")
    )


def screen_customers(
    customers: DataFrame,
    sanctions: DataFrame,
    cfg: dict[str, Any],
) -> DataFrame:
    """Return scored candidate matches at REVIEW or STRONG level.

    ``customers``: customer_id, name_norm.  ``sanctions``: entry_id, list_source, name,
    name_norm, snapshot_date (already restricted to the latest snapshot).
    Output: customer_id, entry_id, list_source, customer_name, matched_name, score, band,
    list_snapshot_date.
    """
    min_len = int(cfg.get("min_token_length", 3))
    particles = list(cfg.get("particles", []))
    review_min = float(cfg["review_min"])
    strong_min = float(cfg["strong_min"])

    c_keys = _keys(customers.filter(F.col("name_norm") != ""), "customer_id", min_len, particles, "c")
    s_keys = _keys(sanctions.filter(F.col("name_norm") != ""), "entry_id", min_len, particles, "s")

    pairs = (
        c_keys.join(s_keys, "block_key")
        .groupBy("customer_id", "entry_id")
        .agg(
            F.least(F.countDistinct("c_tok"), F.countDistinct("s_tok")).alias("shared_tokens"),
            F.max("c_ntok").alias("c_ntok"),
            F.max("s_ntok").alias("s_ntok"),
        )
        .filter(
            F.col("shared_tokens")
            >= F.when((F.col("c_ntok") >= 2) & (F.col("s_ntok") >= 2), 2).otherwise(1)
        )
        .select("customer_id", "entry_id")
    )

    scored_in = (
        pairs.join(customers.select("customer_id", F.col("name_norm").alias("customer_name")), "customer_id")
        .join(
            sanctions.select(
                "entry_id",
                "list_source",
                F.col("name_norm").alias("matched_name"),
                F.col("snapshot_date").alias("list_snapshot_date"),
            ),
            "entry_id",
        )
    )

    @F.pandas_udf("double")
    def _score(a: pd.Series, b: pd.Series) -> pd.Series:
        return pd.Series([score_names(x, y) for x, y in zip(a, b, strict=True)], dtype="float64")

    scored = scored_in.withColumn("score", _score(F.col("customer_name"), F.col("matched_name")))
    return (
        scored.filter(F.col("score") >= review_min)
        .withColumn("band", F.when(F.col("score") >= strong_min, "STRONG").otherwise("REVIEW"))
        .select(
            "customer_id",
            "entry_id",
            "list_source",
            "customer_name",
            "matched_name",
            "score",
            "band",
            "list_snapshot_date",
        )
    )
