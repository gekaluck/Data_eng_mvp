"""Silver layer DAG to transform CoinCap Bronze data into Iceberg tables."""

import logging
import os
import subprocess
import sys

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.python import PythonSensor
from pendulum import datetime, duration

from utils.run_dates import bronze_assets_key, resolve_target_dates

logger = logging.getLogger(__name__)

BRONZE_BUCKET = "bronze"
S3_CONN_ID = "minio_s3"
REQUIRED_ENVVARS = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_ENDPOINT"]


def bronze_partition_exists(**context) -> bool:
    """Return True once every Bronze partition this run needs is present.

    For a range run all dates must be there before Spark starts, so a partial
    catch-up waits rather than silently transforming half the window.
    """
    target_dates = resolve_target_dates(context)
    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    missing = [
        target_date
        for target_date in target_dates
        if not hook.check_for_key(key=bronze_assets_key(target_date), bucket_name=BRONZE_BUCKET)
    ]
    logger.info(
        "Checking %d Bronze partition(s) in s3://%s/ -> missing=%s",
        len(target_dates),
        BRONZE_BUCKET,
        ", ".join(d.isoformat() for d in missing) or "none",
    )
    return not missing


def validate_envvars(envvars: dict[str, str]) -> None:
    """Ensure required environment variables exist before launching Spark."""
    missing_envvars = [var for var in REQUIRED_ENVVARS if not envvars.get(var)]
    if missing_envvars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_envvars)}"
        )


@dag(
    dag_id="silver_coincap_assets",
    description="Transform CoinCap Bronze Parquet to Silver Iceberg tables",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "target_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD override for a single manual run.",
        ),
        "start_date": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Optional YYYY-MM-DD start of an inclusive range (requires end_date). "
                "Used to process a multi-day catch-up from the capture sync."
            ),
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD end of an inclusive range (requires start_date).",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": duration(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["silver", "coincap"],
)
def silver_coincap_assets():
    wait_for_bronze = PythonSensor(
        task_id="wait_for_bronze",
        python_callable=bronze_partition_exists,
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
        soft_fail=False,
    )

    @task()
    def run_silver_transform(**context):
        """Run the Spark transform subprocess for each resolved target date.

        Runs a single date by default, or an inclusive start_date..end_date range
        (a catch-up after the capture sync pulled several days down at once). Each
        date is independent and idempotent, so we attempt every one and report which
        failed rather than stopping at the first — same approach as Gold.
        """
        target_dates = resolve_target_dates(context)
        validate_envvars(os.environ)

        logger.info(
            "Silver transform will run for %d date(s): %s",
            len(target_dates),
            ", ".join(d.isoformat() for d in target_dates),
        )

        failures: list[str] = []
        for target_date in target_dates:
            target_date_str = target_date.strftime("%Y-%m-%d")
            logger.info("Starting silver transform for %s", target_date_str)

            try:
                result = subprocess.run(
                    [sys.executable, "/opt/airflow/spark/silver_transform.py", target_date_str],
                    env=os.environ.copy(),
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=600,
                )
            except FileNotFoundError as exc:
                raise RuntimeError("Failed to start silver_transform.py subprocess") from exc
            except subprocess.TimeoutExpired:
                logger.error("silver_transform.py timed out for %s", target_date_str)
                failures.append(target_date_str)
                continue

            if result.stdout:
                logger.info("Spark stdout (%s):\n%s", target_date_str, result.stdout)
            if result.stderr:
                logger.warning("Spark stderr (%s):\n%s", target_date_str, result.stderr)

            if result.returncode != 0:
                logger.error(
                    "Silver transform failed for %s (exit code %d).",
                    target_date_str,
                    result.returncode,
                )
                failures.append(target_date_str)
            else:
                logger.info("Silver transform complete for %s", target_date_str)

        if failures:
            raise RuntimeError(
                f"Silver transform failed for {len(failures)} of {len(target_dates)} "
                f"date(s): {', '.join(failures)}. See Spark output above for details."
            )

    silver_task = run_silver_transform()

    wait_for_bronze >> silver_task


silver_coincap_assets()
