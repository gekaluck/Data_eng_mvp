"""Silver layer DAG to transform CoinCap Bronze data into Iceberg tables."""

import logging
import os
import subprocess
import sys

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.python import PythonSensor
from pendulum import datetime, duration

from utils.run_dates import bronze_assets_key, resolve_target_date

logger = logging.getLogger(__name__)

BRONZE_BUCKET = "bronze"
S3_CONN_ID = "minio_s3"
REQUIRED_ENVVARS = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_ENDPOINT"]


def bronze_partition_exists(**context) -> bool:
    """Return True when the expected Bronze file exists for this run's target date."""
    target_date = resolve_target_date(context)
    s3_key = bronze_assets_key(target_date)
    exists = S3Hook(aws_conn_id=S3_CONN_ID).check_for_key(
        key=s3_key,
        bucket_name=BRONZE_BUCKET,
    )
    logger.info(
        "Checking Bronze partition for target_date=%s at s3://%s/%s -> exists=%s",
        target_date,
        BRONZE_BUCKET,
        s3_key,
        exists,
    )
    return exists


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
        """Run the Spark transform subprocess for the resolved target date."""
        target_date = resolve_target_date(context)
        target_date_str = target_date.strftime("%Y-%m-%d")

        logger.info("Starting silver transform for %s", target_date_str)
        validate_envvars(os.environ)

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
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("silver_transform.py subprocess timed out") from exc

        if result.stdout:
            logger.info("Spark stdout:\n%s", result.stdout)
        if result.stderr:
            logger.warning("Spark stderr:\n%s", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"Silver transform failed (exit code {result.returncode}). "
                "See Spark output above for details."
            )

        logger.info("Silver transform complete for %s", target_date_str)

    silver_task = run_silver_transform()
    trigger_spark_gold = TriggerDagRunOperator(
        task_id="trigger_gold_assets",
        trigger_dag_id="gold_coincap_assets",
        conf={"target_date": "{{ dag_run.conf.get('target_date', ds) if dag_run and dag_run.conf else ds }}"},
        wait_for_completion=False,
    )
    trigger_dbt_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dbt_assets",
        trigger_dag_id="gold_dbt_coincap_assets",
        conf={"target_date": "{{ dag_run.conf.get('target_date', ds) if dag_run and dag_run.conf else ds }}"},
        wait_for_completion=False,
    )

    wait_for_bronze >> silver_task >> [trigger_spark_gold, trigger_dbt_gold]


silver_coincap_assets()
