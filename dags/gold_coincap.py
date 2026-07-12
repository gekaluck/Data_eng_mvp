"""Gold layer DAG to transform Silver Iceberg tables into Gold outputs."""

import logging
import os
import subprocess
import sys

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime, duration

from utils.run_dates import resolve_target_dates

logger = logging.getLogger(__name__)

REQUIRED_ENVVARS = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_ENDPOINT"]


def validate_envvars(envvars: dict[str, str]) -> None:
    """Ensure required environment variables exist before launching Spark."""
    missing_envvars = [var for var in REQUIRED_ENVVARS if not envvars.get(var)]
    if missing_envvars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_envvars)}"
        )


@dag(
    dag_id="gold_coincap_assets",
    description="Transform CoinCap Silver Iceberg tables into Gold Iceberg tables",
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
                "Optional YYYY-MM-DD start of an inclusive range to rebuild "
                "(requires end_date). Use to rebuild Gold across a backfilled window."
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
    tags=["gold", "coincap"],
)
def gold_coincap_assets():
    @task()
    def run_gold_transform(**context):
        """Run the Spark gold transform subprocess for each resolved target date.

        Runs a single date by default, or an inclusive start_date..end_date range
        (used to rebuild Gold across a backfilled window). Each date is independent
        and idempotent (per-date partition overwrite), so we attempt every date and
        report which ones failed rather than stopping at the first failure.
        """
        target_dates = resolve_target_dates(context)
        validate_envvars(os.environ)

        logger.info(
            "Gold transform will run for %d date(s): %s",
            len(target_dates),
            ", ".join(d.isoformat() for d in target_dates),
        )

        failures: list[str] = []
        for target_date in target_dates:
            target_date_str = target_date.strftime("%Y-%m-%d")
            logger.info("Starting gold transform for %s", target_date_str)

            try:
                result = subprocess.run(
                    [sys.executable, "/opt/airflow/spark/gold_transform.py", target_date_str],
                    env=os.environ.copy(),
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=600,
                )
            except FileNotFoundError as exc:
                raise RuntimeError("Failed to start gold_transform.py subprocess") from exc
            except subprocess.TimeoutExpired:
                logger.error("gold_transform.py timed out for %s", target_date_str)
                failures.append(target_date_str)
                continue

            if result.stdout:
                logger.info("Spark stdout (%s):\n%s", target_date_str, result.stdout)
            if result.stderr:
                logger.warning("Spark stderr (%s):\n%s", target_date_str, result.stderr)

            if result.returncode != 0:
                logger.error(
                    "Gold transform failed for %s (exit code %d).",
                    target_date_str,
                    result.returncode,
                )
                failures.append(target_date_str)
            else:
                logger.info("Gold transform complete for %s", target_date_str)

        if failures:
            raise RuntimeError(
                f"Gold transform failed for {len(failures)} of {len(target_dates)} "
                f"date(s): {', '.join(failures)}. See Spark output above for details."
            )

    run_gold_transform()


gold_coincap_assets()
