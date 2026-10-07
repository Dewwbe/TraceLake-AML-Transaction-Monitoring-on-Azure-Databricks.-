import subprocess
import sys
from pathlib import Path

from aml.schemas import TABLES

ROOT = Path(__file__).resolve().parents[1]


def test_generated_ddl_and_docs_are_up_to_date():
    res = subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_docs.py"), "--check"],
                         capture_output=True, text=True, env={"PYTHONPATH": str(ROOT / "src"), "PATH": ""})
    assert res.returncode == 0, res.stderr


def test_registry_is_consistent():
    for spec in TABLES.values():
        names = spec.column_names()
        assert len(names) == len(set(names)), f"duplicate columns in {spec.name}"
        assert spec.name.split(".")[0] == spec.layer
        assert spec.pk, f"{spec.name} has no key"
        assert set(spec.pk) <= set(names), f"{spec.name} key not in columns"
    for name in ("gold.fact_alert", "gold.dim_customer", "silver.transactions"):
        assert name in TABLES
