"""Silver layer DAG — transform CoinCap bronze Parquet to Iceberg tables.

Waits for bronze_coincap_assets to finish for the same logical date, then
runs the PySpark silver_transform.py job that:

  1. Reads today's bronze Parquet from MinIO
  2. Type-casts and renames columns (camelCase → snake_case)
  3. Writes to two Iceberg tables in the silver catalog:
       - silver.crypto.coins            (metadata, upserted by coin id)
       - silver.crypto.price_snapshots  (daily facts, partitioned by date)

Key Airflow patterns demonstrated:
  - ExternalTaskSensor: how to express cross-DAG dependencies
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
from airflow.sensors.external_task import ExternalTaskSensor
from pendulum import datetime

logger = logging.getLogger(__name__)


@dag(
    dag_id="silver_coincap_assets",
    description="Transform CoinCap bronze Parquet to silver Iceberg tables",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "coincap"],
)
def silver_coincap_assets():

    # Wait for the bronze DAG to complete for the same execution date.
    # mode="reschedule": the sensor sleeps between checks, freeing the worker
    # slot. This is better than mode="poke" which holds the slot the entire time.
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_coincap_assets",
        external_task_id="fetch_validate_upload",
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
        logical_date = context["logical_date"]
        logical_date_str = logical_date.strftime("%Y-%m-%d")

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
