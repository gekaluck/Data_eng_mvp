"""Bronze layer DAG - fetch CoinCap assets and store as Parquet in MinIO.

Single-task DAG: the payload is <100KB so splitting into multiple tasks
would add XCom overhead with no real benefit.

Flow:
  1. GET /assets?limit=20 from the configured CoinCap API base URL
  2. Validate response with Pydantic
  3. Convert to Parquet (Snappy compression)
  4. Upload to MinIO: bronze/crypto/assets/year=YYYY/month=MM/day=DD/assets.parquet

Uses logical_date for deterministic partitioning (idempotent reruns).
"""

import io
import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime, duration

from schemas.coincap import CoinCapAssetsResponse
from utils.coincap_api import format_coincap_request_error
from utils.run_dates import bronze_assets_key, resolve_target_date

logger = logging.getLogger(__name__)

COINCAP_API_BASE_URL = os.getenv("COINCAP_API_BASE_URL", "https://rest.coincap.io/v3").rstrip("/")
COINCAP_ASSETS_PATH = os.getenv("COINCAP_ASSETS_PATH", "/assets")
COINCAP_URL = f"{COINCAP_API_BASE_URL}{COINCAP_ASSETS_PATH}"
COINCAP_API_KEY = os.getenv("COINCAP_API_KEY", "").strip()
COINCAP_LIMIT = 20
BRONZE_BUCKET = "bronze"
S3_CONN_ID = "minio_s3"


@dag(
    dag_id="bronze_coincap_assets",
    description="Fetch CoinCap top assets and store raw Parquet in Bronze",
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
        "retries": 3,
        "retry_delay": duration(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=["bronze", "coincap"],
)
def bronze_coincap_assets():

    @task()
    def fetch_validate_upload(**context):
        """Fetch from CoinCap, validate with Pydantic, write Parquet to MinIO."""
        target_date = resolve_target_date(context)
        headers = {}

        if COINCAP_API_KEY:
            headers["Authorization"] = f"Bearer {COINCAP_API_KEY}"
        elif "rest.coincap.io" in COINCAP_API_BASE_URL:
            raise RuntimeError(
                "COINCAP_API_KEY is required for the current CoinCap API. "
                "Set it in .env before running bronze_coincap_assets."
            )

        logger.info(
            "Fetching top %d assets from CoinCap: %s (target_date=%s)",
            COINCAP_LIMIT,
            COINCAP_URL,
            target_date,
        )
        try:
            response = requests.get(
                COINCAP_URL,
                params={"limit": COINCAP_LIMIT},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            raw_json = response.json()
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(format_coincap_request_error(exc, COINCAP_URL)) from exc

        validated = CoinCapAssetsResponse.model_validate(raw_json)
        logger.info("Validated %d assets", len(validated.data))

        records = [asset.model_dump() for asset in validated.data]
        table = pa.Table.from_pylist(records)

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        parquet_bytes = buffer.getvalue()
        logger.info("Parquet size: %d bytes", len(parquet_bytes))

        s3_key = bronze_assets_key(target_date)
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        hook.load_bytes(
            bytes_data=parquet_bytes,
            key=s3_key,
            bucket_name=BRONZE_BUCKET,
            replace=True,
        )
        logger.info("Uploaded to s3://%s/%s", BRONZE_BUCKET, s3_key)

    bronze_task = fetch_validate_upload()
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_assets",
        trigger_dag_id="silver_coincap_assets",
        conf={"target_date": "{{ ds }}"},
        wait_for_completion=False,
    )

    bronze_task >> trigger_silver


bronze_coincap_assets()
