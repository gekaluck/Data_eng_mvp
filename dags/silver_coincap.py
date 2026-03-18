"""Silver layer DAG — transform CoinCap bronze Parquet to Iceberg tables.

Waits for bronze_coincap_assets to finish for the same logical date, then
runs the PySpark silver_transform.py job that:

  1. Reads today's bronze Parquet from MinIO
  2. Type-casts and renames columns (camelCase → snake_case)
  3. Writes to two Iceberg tables in the silver catalog:
       - silver.crypto.coins            (metadata, upserted by coin id)
       - silver.crypto.price_snapshots  (daily facts, partitioned by date)

Key Airflow patterns demonstrated:
  - PythonSensor: how to wait on a concrete Bronze artifact instead of matching
    Airflow run timestamps
  - mode="reschedule": sensor releases the worker slot while waiting,
    so the scheduler can run other tasks instead of blocking a worker
  - subprocess execution: SparkSession runs in an isolated subprocess,
    cleanly separated from the Airflow worker process
"""

import logging
import os
import subprocess
import sys

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.python import PythonSensor
from pendulum import datetime

from utils.run_dates import bronze_assets_key, resolve_target_date

logger = logging.getLogger(__name__)
BRONZE_BUCKET = "bronze"
S3_CONN_ID = "minio_s3"


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


@dag(
    dag_id="silver_coincap_assets",
    description="Transform CoinCap bronze Parquet to silver Iceberg tables",
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
    tags=["silver", "coincap"],
)
def silver_coincap_assets():

    # Wait for the Bronze file for this run's target date to exist in MinIO.
    # This keeps scheduled runs aligned while also making manual runs work even
    # when Bronze and Silver are triggered at different timestamps.
    wait_for_bronze = PythonSensor(
        task_id="wait_for_bronze",
        python_callable=bronze_partition_exists,
        mode="reschedule",
        timeout=3600,       # give up after 1 hour if bronze never finishes
        poke_interval=60,   # check every 60 seconds
    )

    @task()
    def run_silver_transform(**context):
        """Submit silver_transform.py as a subprocess.

        Why subprocess instead of importing SparkSession inline?
        - The JVM lifecycle is fully isolated (Spark starts, does work, stops)
        - The script can be tested and run independently of Airflow
        - Mirrors how SparkSubmitOperator works in production setups
        - Avoids any risk of SparkContext conflicts with the Airflow worker
        """
        target_date = resolve_target_date(context)
        logical_date_str = target_date.strftime("%Y-%m-%d")

        logger.info("Starting silver transform for %s", logical_date_str)

        result = subprocess.run(
            [sys.executable, "/opt/airflow/spark/silver_transform.py", logical_date_str],
            env=os.environ.copy(),   # passes MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, etc.
            capture_output=True,
            text=True,
        )

        # Surface all Spark output in the Airflow task logs so it's visible in the UI.
        if result.stdout:
            logger.info("Spark stdout:\n%s", result.stdout)
        if result.stderr:
            logger.info("Spark stderr:\n%s", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"Silver transform failed (exit code {result.returncode}). "
                "See Spark output above for details."
            )

        logger.info("Silver transform complete for %s", logical_date_str)

    wait_for_bronze >> run_silver_transform()


silver_coincap_assets()
