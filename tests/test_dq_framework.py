import pytest

from aml.quality.dq_framework import DQGateError, DQRule, apply_rules, enforce_gate, evaluate_gate


def _df(spark):
    return spark.createDataFrame(
        [("a", 10.0, "AED"), ("b", -1.0, "AED"), (None, 5.0, "XXX"), ("a", 7.0, "USD")],
        "id string, amount double, ccy string",
    )


def test_rules_split_valid_and_quarantine_and_report_counts(spark):
    rules = [
        DQRule("R1", "not_null", column="id"),
        DQRule("R2", "range", column="amount", params={"gt": 0}),
        DQRule("R3", "in_set", column="ccy", params={"values": ["AED", "USD"]}),
        DQRule("R4", "unique", columns=("id",), action="flag", severity="warn"),
    ]
    out = apply_rules(spark, _df(spark), rules, "t", "run1")
    results = {r["rule_id"]: r for r in out.result_rows}
    assert results["R1"]["rows_failed"] == 1
    assert results["R2"]["rows_failed"] == 1
    assert results["R3"]["rows_failed"] == 1
    assert results["R4"]["rows_failed"] == 1  # second "a"
    assert out.valid.count() == 1 + 1  # ("a",10) and ("a",7); R4 only flags
    assert out.quarantine.count() == 2
    q = {tuple(r["dq_failed_rules"]) for r in out.quarantine.collect()}
    assert ("R2",) in q and ("R1", "R3") in q
    flagged = [r for r in out.valid.collect() if r["dq_warnings"]]
    assert len(flagged) == 1


def test_expression_rule_treats_null_as_failure(spark):
    rules = [DQRule("E1", "expression", params={"expr": "amount > 0 AND id IS NOT NULL"})]
    out = apply_rules(spark, _df(spark), rules, "t", "run1")
    assert out.valid.count() == 2


def test_gate_breach_and_pass():
    rows = [{"table_name": "t", "rule_id": "R", "severity": "error", "fail_pct": 6.0},
            {"table_name": "t", "rule_id": "W", "severity": "warn", "fail_pct": 90.0}]
    assert len(evaluate_gate(rows, 5.0)) == 1
    with pytest.raises(DQGateError):
        enforce_gate(rows, 5.0)
    enforce_gate(rows, 10.0)  # warn-level failures never breach the gate


def test_unsupported_rule_type_raises(spark):
    with pytest.raises(ValueError):
        apply_rules(spark, _df(spark), [DQRule("X", "bogus", column="id")], "t", "r")
