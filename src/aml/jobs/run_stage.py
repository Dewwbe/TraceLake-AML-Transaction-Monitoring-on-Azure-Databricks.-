"""Entrypoint for one pipeline stage: ``aml-stage --stage silver_transactions --run-id <id>``.

Used by the Databricks job (python_wheel_task) that Airflow triggers. Records success or failure
in ops.pipeline_runs and re-raises on failure so the orchestrator sees a failed task.
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timezone

from aml.common.logging import get_logger
from aml.common.spark import get_spark
from aml.config import load_settings
from aml.jobs.stages import STAGES, Ctx
from aml.store import make_store

log = get_logger("aml.run_stage")


def make_ctx(run_id: str | None = None, local: bool | None = None) -> Ctx:
    cfg = load_settings()
    spark = get_spark("aml", local=(cfg.store == "parquet") if local is None else local)
    store = make_store(spark, cfg.store, cfg.base_path, cfg.catalog)
    return Ctx(spark, store, cfg, run_id or uuid.uuid4().hex[:12], datetime.now(timezone.utc))


def run(stage: str, ctx: Ctx) -> None:
    started = datetime.now(timezone.utc)
    status, message = "SUCCESS", None
    try:
        STAGES[stage](ctx)
    except Exception as exc:
        status, message = "FAILED", str(exc)[:1000]
        raise
    finally:
        ctx.store.write(
            "ops.pipeline_runs",
            ctx.spark.createDataFrame(
                [(ctx.run_id, stage, started, datetime.now(timezone.utc), status, message)],
                "run_id string, stage string, started_at timestamp, finished_at timestamp, "
                "status string, message string",
            ),
            mode="append",
        )
        log.info("stage finished", extra={"ctx": {"stage": stage, "status": status, "run_id": ctx.run_id}})


# Optional overrides for AML_* settings. Serverless jobs cannot set environment variables, so the
# bundle passes these as task parameters instead.
SETTING_ARGS = {
    "store": "AML_STORE",
    "catalog": "AML_CATALOG",
    "landing-path": "AML_LANDING_PATH",
    "checkpoint-path": "AML_CHECKPOINT_PATH",
    "config-dir": "AML_CONFIG_DIR",
    "reference-dir": "AML_REFERENCE_DIR",
    "use-autoloader": "AML_USE_AUTOLOADER",
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run one AML pipeline stage")
    parser.add_argument("--stage", required=True, choices=sorted(STAGES))
    parser.add_argument("--run-id", default=None)
    for name in SETTING_ARGS:
        parser.add_argument(f"--{name}", default=None)
    args = parser.parse_args(argv)
    for name, env_var in SETTING_ARGS.items():
        value = getattr(args, name.replace("-", "_"))
        if value is not None:
            os.environ[env_var] = value
    run(args.stage, make_ctx(args.run_id))


if __name__ == "__main__":
    main()
