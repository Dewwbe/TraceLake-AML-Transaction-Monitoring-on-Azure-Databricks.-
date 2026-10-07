from datetime import date

from aml.detection.screening import latest_snapshot, screen_customers

CFG = {"review_min": 80, "strong_min": 92, "min_token_length": 3, "particles": ["al", "bin"]}


def _cust(spark, rows):
    return spark.createDataFrame(rows, "customer_id string, name_norm string")


def _sanc(spark, rows):
    return spark.createDataFrame(
        rows, "entry_id string, list_source string, name_norm string, snapshot_date date"
    ).selectExpr("entry_id", "list_source", "name_norm as name", "name_norm", "snapshot_date")


def test_screening_finds_variants_and_ignores_unrelated(spark):
    customers = _cust(spark, [
        ("c1", "mohammed haddad"),   # exact
        ("c2", "muhammad haddad"),   # transliteration
        ("c3", "haddad mohammed"),   # reordered
        ("c4", "maria silva"),       # unrelated
    ])
    sanctions = _sanc(spark, [("e1", "L", "mohammed haddad", date(2024, 1, 1))])
    out = {r["customer_id"]: r for r in screen_customers(customers, sanctions, CFG).collect()}
    assert set(out) == {"c1", "c2", "c3"}
    assert out["c1"]["band"] == "STRONG"
    assert out["c2"]["score"] >= 80


def test_latest_snapshot_keeps_only_newest_per_list(spark):
    df = spark.createDataFrame(
        [("e1", "A", "2024-01-01"), ("e2", "A", "2024-02-01"), ("e3", "B", "2024-01-01")],
        "entry_id string, list_source string, snapshot_date string",
    ).selectExpr("entry_id", "list_source", "cast(snapshot_date as date) snapshot_date")
    kept = {r["entry_id"] for r in latest_snapshot(df).collect()}
    assert kept == {"e2", "e3"}
