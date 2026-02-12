"""Bronze layer DAG — fetch CoinCap assets and store as Parquet in MinIO.

Single-task DAG: the payload is <100KB so splitting into multiple tasks
would add XCom overhead with no real benefit.

Flow:
  1. GET /v2/assets?limit=20 from CoinCap API
  2. Validate response with Pydantic
  3. Convert to Parquet (Snappy compression)
  4. Upload to MinIO: bronze/crypto/assets/year=YYYY/month=MM/day=DD/assets.parquet

Uses logical_date for deterministic partitioning (idempotent reruns).
"""

import io
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime

from schemas.coincap import CoinCapAssetsResponse

logger = logging.getLogger(__name__)

COINCAP_URL = "https://api.coincap.io/v2/assets"
COINCAP_LIMIT = 20
BRONZE_BUCKET = "bronze"
S3_CONN_ID = "minio_s3"


@dag(
    dag_id="bronze_coincap_assets",
    description="Fetch CoinCap top assets and store raw Parquet in Bronze",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "coincap"],
)
def bronze_coincap_assets():

    @task()
    def fetch_validate_upload(**context):
        """Fetch from CoinCap, validate with Pydantic, write Parquet to MinIO."""
        logical_date = context["logical_date"]

        # --- Step 1: Fetch from API ---
        logger.info("Fetching top %d assets from CoinCap", COINCAP_LIMIT)
        response = requests.get(
            COINCAP_URL,
            params={"limit": COINCAP_LIMIT},
            timeout=30,
        )
        response.raise_for_status()
        raw_json = response.json()

        # --- Step 2: Validate with Pydantic ---
        validated = CoinCapAssetsResponse.model_validate(raw_json)
        logger.info("Validated %d assets", len(validated.data))

        # --- Step 3: Convert to Parquet ---
        records = [asset.model_dump() for asset in validated.data]
        table = pa.Table.from_pylist(records)

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        parquet_bytes = buffer.getvalue()
        logger.info("Parquet size: %d bytes", len(parquet_bytes))

        # --- Step 4: Upload to MinIO ---
        s3_key = (
            f"crypto/assets/"
            f"year={logical_date.year}/"
            f"month={logical_date.month:02d}/"
            f"day={logical_date.day:02d}/"
            f"assets.parquet"
        )
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        hook.load_bytes(
            bytes_data=parquet_bytes,
            key=s3_key,
            bucket_name=BRONZE_BUCKET,
            replace=True,
        )
        logger.info("Uploaded to s3://%s/%s", BRONZE_BUCKET, s3_key)

    fetch_validate_upload()


bronze_coincap_assets()
