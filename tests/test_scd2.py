from datetime import datetime

from pyspark.sql import functions as F

from aml.schemas import TABLES
from aml.transform.scd2 import scd2_apply_df, scd2_changes

TRACKED = ["risk_rating", "residence_country"]
T1, T2, T3 = datetime(2024, 1, 1), datetime(2024, 6, 1), datetime(2024, 9, 1)


def _incoming(spark, rows):
    return spark.createDataFrame(
        rows, "customer_id string, risk_rating string, residence_country string, onboarding_date date"
    ).withColumn("onboarding_date", F.col("onboarding_date"))


def _empty_dim(spark):
    return spark.createDataFrame([], TABLES["gold.dim_customer"].ddl_columns())


def _load(spark, dim, rows, ts):
    exp, ins = scd2_changes(
        dim, _incoming(spark, rows), "customer_id", "customer_key", TRACKED,
        F.lit(ts).cast("timestamp"), new_key_valid_from=F.col("onboarding_date").cast("timestamp"),
    )
    full_insert = ins
    for c in TABLES["gold.dim_customer"].cols:
        if c.name not in full_insert.columns:
            full_insert = full_insert.withColumn(c.name, F.lit(None).cast(c.dtype))
    return scd2_apply_df(dim, exp, full_insert, "customer_key")


def test_initial_load_uses_onboarding_date_and_marks_current(spark):
    import datetime as dt
    dim = _load(spark, _empty_dim(spark), [("c1", "LOW", "UAE", dt.date(2020, 5, 1))], T1)
    row = dim.collect()[0]
    assert row["is_current"] is True and row["valid_to"] is None
    assert row["valid_from"] == datetime(2020, 5, 1)


def test_change_closes_old_version_and_opens_new(spark):
    import datetime as dt
    d = dt.date(2020, 5, 1)
    dim = _load(spark, _empty_dim(spark), [("c1", "LOW", "UAE", d), ("c2", "LOW", "UK", d)], T1)
    dim = _load(spark, dim, [("c1", "HIGH", "UAE", d), ("c2", "LOW", "UK", d)], T2)
    rows = sorted(dim.filter("customer_id = 'c1'").collect(), key=lambda r: r["valid_from"])
    assert [r["risk_rating"] for r in rows] == ["LOW", "HIGH"]
    assert rows[0]["valid_to"] == T2 and rows[0]["is_current"] is False
    assert rows[1]["valid_from"] == T2 and rows[1]["is_current"] is True
    assert dim.filter("customer_id = 'c2'").count() == 1  # unchanged -> no new version


def test_reload_of_identical_snapshot_is_idempotent(spark):
    import datetime as dt
    d = dt.date(2020, 5, 1)
    dim = _load(spark, _empty_dim(spark), [("c1", "LOW", "UAE", d)], T1)
    again = _load(spark, dim, [("c1", "LOW", "UAE", d)], T3)
    assert again.count() == 1
    keys = [r["customer_key"] for r in again.collect()]
    assert len(set(keys)) == len(keys)
