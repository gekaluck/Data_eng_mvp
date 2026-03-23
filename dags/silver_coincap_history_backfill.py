"""Silver DAG for loading Bronze history backfill datasets into Iceberg tables."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import datetime as stdlib_datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.python import PythonSensor
from pendulum import datetime, duration

from utils.run_dates import bronze_history_backfill_key, resolve_int_param, resolve_optional_date_param

logger = logging.getLogger(__name__)

BRONZE_BUCKET = "bronze"
S3_CONN_ID = "minio_s3"
REQUIRED_ENVVARS = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_ENDPOINT"]
PLAN_PREFIX = "PLAN_JSON:"


def validate_envvars(envvars: dict[str, str]) -> None:
    """Ensure required environment variables exist before launching Spark."""
    missing_envvars = [var for var in REQUIRED_ENVVARS if not envvars.get(var)]
    if missing_envvars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_envvars)}"
        )


def bronze_history_backfill_exists(plan: dict, **_context) -> bool:
    """Return True when all expected Bronze history backfill files exist."""
    anchor_snapshot_date = stdlib_datetime.strptime(
        plan["anchor_snapshot_date"],
        "%Y-%m-%d",
    ).date()
    backfill_days = int(plan["backfill_days"])
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    dataset_names = (
        "asset_history",
        "asset_market_cap_history",
        "total_market_cap_history",
    )
    results = {
        dataset_name: hook.check_for_key(
            key=bronze_history_backfill_key(
                anchor_snapshot_date,
                backfill_days,
                dataset_name,
            ),
            bucket_name=BRONZE_BUCKET,
        )
        for dataset_name in dataset_names
    }
    logger.info(
        "Checking Bronze history backfill files for anchor=%s window_days=%s -> %s",
        plan["anchor_snapshot_date"],
        backfill_days,
        results,
    )
    return all(results.values())


def _extract_backfill_plan(stdout: str, stderr: str) -> dict:
    """Extract the tagged JSON plan from noisy subprocess output."""
    for stream_name, content in (("stdout", stdout), ("stderr", stderr)):
        for line in content.splitlines():
            if line.startswith(PLAN_PREFIX):
                payload = line[len(PLAN_PREFIX):].strip()
                try:
                    return json.loads(payload)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Found malformed backfill plan JSON in {stream_name}: {payload}"
                    ) from exc

    raise ValueError(
        "silver_backfill_plan.py did not emit a PLAN_JSON payload. "
        "Check the logged stdout/stderr for the real failure."
    )


@dag(
    dag_id="silver_coincap_history_backfill",
    description="Load Bronze CoinCap history backfill parquet into Silver Iceberg tables",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "anchor_snapshot_date": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Optional YYYY-MM-DD anchor date. Defaults to the earliest "
                "silver.crypto.price_snapshots date at runtime."
            ),
        ),
        "backfill_days": Param(
            default=60,
            type="integer",
            minimum=1,
            description="Number of days to load before the anchor date.",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": duration(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["silver", "coincap", "backfill"],
)
def silver_coincap_history_backfill():

    @task()
    def discover_backfill_plan(**context):
        """Read current Silver state and return the fixed backfill plan."""
        anchor_snapshot_date = resolve_optional_date_param(context, "anchor_snapshot_date")
        backfill_days = resolve_int_param(context, "backfill_days", default=60)

        result = subprocess.run(
            [
                sys.executable,
                "/opt/airflow/spark/silver_backfill_plan.py",
                anchor_snapshot_date.isoformat() if anchor_snapshot_date else "auto",
                str(backfill_days),
            ],
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            check=False,
            timeout=600,
        )
        if result.stdout:
            logger.info("Backfill plan stdout:\n%s", result.stdout)
        if result.stderr:
            logger.warning("Backfill plan stderr:\n%s", result.stderr)
        if result.returncode != 0:
            raise RuntimeError(
                f"silver_backfill_plan.py failed with exit code {result.returncode}"
            )
        return _extract_backfill_plan(result.stdout, result.stderr)

    plan = discover_backfill_plan()

    wait_for_bronze = PythonSensor(
        task_id="wait_for_bronze_history_backfill",
        python_callable=bronze_history_backfill_exists,
        op_kwargs={"plan": plan},
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        soft_fail=False,
    )

    @task()
    def run_silver_history_backfill(plan: dict):
        """Run the Spark backfill subprocess for the resolved anchor/window."""
        validate_envvars(os.environ)
        result = subprocess.run(
            [
                sys.executable,
                "/opt/airflow/spark/silver_history_backfill.py",
                plan["anchor_snapshot_date"],
                str(plan["backfill_days"]),
            ],
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            check=False,
            timeout=1200,
        )

        if result.stdout:
            logger.info("Spark stdout:\n%s", result.stdout)
        if result.stderr:
            logger.warning("Spark stderr:\n%s", result.stderr)
        if result.returncode != 0:
            raise RuntimeError(
                f"silver_history_backfill.py failed with exit code {result.returncode}"
            )

    wait_for_bronze >> run_silver_history_backfill(plan)


silver_coincap_history_backfill()
