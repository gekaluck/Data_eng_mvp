"""Gold layer DAG to transform Silver Iceberg tables into Gold outputs."""

import logging
import os
import subprocess
import sys

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime, duration

from utils.run_dates import resolve_target_date

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
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "target_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD override for manual runs.",
        )
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
        """Run the Spark gold transform subprocess for the resolved target date."""
        target_date = resolve_target_date(context)
        target_date_str = target_date.strftime("%Y-%m-%d")

        logger.info("Starting gold transform for %s", target_date_str)
        validate_envvars(os.environ)

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
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("gold_transform.py subprocess timed out") from exc

        if result.stdout:
            logger.info("Spark stdout:\n%s", result.stdout)
        if result.stderr:
            logger.warning("Spark stderr:\n%s", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"Gold transform failed (exit code {result.returncode}). "
                "See Spark output above for details."
            )

        logger.info("Gold transform complete for %s", target_date_str)

    run_gold_transform()


gold_coincap_assets()
