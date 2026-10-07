from datetime import datetime, timedelta

from aml.detection.rules import (
    fan_out_fan_in,
    high_risk_jurisdiction,
    pass_through,
    round_amounts,
    run_rules,
    structuring,
)

T0 = datetime(2024, 3, 1, 9, 0, 0)
SCHEMA = (
    "transaction_id string, event_ts timestamp, event_date date, sender_account string, "
    "receiver_account string, amount_aed double, sender_country string, receiver_country string"
)


def _txn(spark, rows):
    out = [(t, ts, ts.date(), s, r, a, sc, rc) for t, ts, s, r, a, sc, rc in rows]
    return spark.createDataFrame(out, SCHEMA)


def test_structuring_flags_repeated_just_below_threshold(spark):
    p = {"threshold_aed": 50000, "lower_ratio": 0.8, "min_count": 3, "window_days": 3}
    rows = [(f"t{i}", T0 + timedelta(hours=6 * i), "A", f"R{i}", 45000.0, "UAE", "UAE") for i in range(3)]
    rows += [("n1", T0, "B", "R9", 45000.0, "UAE", "UAE"), ("n2", T0, "C", "R9", 20000.0, "UAE", "UAE")]
    hits = structuring(_txn(spark, rows), p, None).collect()
    assert {h["account_id"] for h in hits} == {"A"}
    assert hits[0]["rule_id"] == "R001" and hits[0]["score"] >= 0.5


def test_pass_through_flags_funds_moved_onward(spark):
    p = {"min_amount_aed": 50000, "out_in_ratio": 0.9, "window_days": 2}
    rows = [("t1", T0, "SRC", "MID", 100000.0, "UAE", "UAE"),
            ("t2", T0 + timedelta(hours=3), "MID", "DST", 95000.0, "UAE", "UAE"),
            ("t3", T0, "X", "Y", 100000.0, "UAE", "UAE")]  # Y receives but never sends on
    hits = pass_through(_txn(spark, rows), p, None).collect()
    assert {h["account_id"] for h in hits} == {"MID"}


def test_high_risk_jurisdiction_uses_tier_in_force_at_event_time(spark):
    p = {"min_amount_aed": 10000, "tiers": ["HIGH"]}
    dim = spark.createDataFrame(
        [("NIGERIA", "HIGH", datetime(2024, 3, 15), None), ("NIGERIA", "LOW", datetime(1900, 1, 1), datetime(2024, 3, 15)),
         ("UAE", "LOW", datetime(1900, 1, 1), None)],
        "country string, risk_tier string, valid_from timestamp, valid_to timestamp",
    )
    rows = [("before", datetime(2024, 3, 1), "A", "B", 30000.0, "UAE", "NIGERIA"),
            ("after", datetime(2024, 3, 20), "A", "B", 30000.0, "UAE", "NIGERIA"),
            ("small", datetime(2024, 3, 20), "C", "B", 500.0, "UAE", "NIGERIA"),
            ("domestic", datetime(2024, 3, 20), "D", "B", 30000.0, "NIGERIA", "NIGERIA")]
    hits = high_risk_jurisdiction(_txn(spark, rows), p, dim).collect()
    assert [h["transaction_id"] for h in hits] == ["after"]


def test_fan_out_and_fan_in(spark):
    p = {"min_counterparties": 5, "window_days": 1}
    out_rows = [(f"o{i}", T0 + timedelta(minutes=i), "HUB", f"R{i}", 9000.0, "UAE", "UAE") for i in range(6)]
    in_rows = [(f"i{i}", T0 + timedelta(minutes=i), f"S{i}", "SINK", 9000.0, "UAE", "UAE") for i in range(6)]
    hits = fan_out_fan_in(_txn(spark, out_rows + in_rows), p, None).collect()
    assert {h["account_id"] for h in hits} == {"HUB", "SINK"}


def test_round_amounts(spark):
    p = {"round_unit": 1000, "min_amount_aed": 10000, "min_count": 3, "window_days": 7}
    rows = [(f"t{i}", T0 + timedelta(days=i), "A", "B", 20000.0, "UAE", "UAE") for i in range(3)]
    rows += [("u1", T0, "C", "B", 20500.0, "UAE", "UAE")]
    hits = round_amounts(_txn(spark, rows), p, None).collect()
    assert {h["account_id"] for h in hits} == {"A"}


def test_one_hit_per_account_per_day_and_hit_id_is_deterministic(spark):
    cfg = {"rules": {"R005": {"enabled": True, "params": {"round_unit": 1000, "min_amount_aed": 10000,
                                                         "min_count": 2, "window_days": 7}}}}
    rows = [(f"t{i}", T0 + timedelta(minutes=i), "A", "B", 20000.0, "UAE", "UAE") for i in range(4)]
    a = run_rules(_txn(spark, rows), cfg).collect()
    b = run_rules(_txn(spark, rows), cfg).collect()
    assert len(a) == 1 and a[0]["hit_id"] == b[0]["hit_id"]


def test_disabled_rules_are_skipped(spark):
    cfg = {"rules": {"R005": {"enabled": False, "params": {}}}}
    assert run_rules(_txn(spark, []), cfg).count() == 0
