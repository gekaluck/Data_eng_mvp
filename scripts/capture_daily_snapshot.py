"""Capture one daily CoinCap /assets snapshot into an S3-compatible bucket.

This is the cloud-side, laptop-independent daily capture. It runs in GitHub
Actions on a schedule and writes a raw Parquet snapshot to a durable bucket
(Cloudflare R2 or AWS S3), using the *same* Pydantic validation, Parquet shape,
and object-key layout as the local Bronze DAG (`dags/bronze_coincap.py`), so the
local pipeline can later ingest it unchanged.

Config is read entirely from environment variables (set as GitHub Actions
secrets); nothing sensitive is hardcoded. See `.env.example` for the full list.

Usage:
    python scripts/capture_daily_snapshot.py            # capture today (UTC)
    python scripts/capture_daily_snapshot.py --date 2026-07-12
    python scripts/capture_daily_snapshot.py --dry-run  # fetch+build, no upload
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

# Reuse the local pipeline's data contract and key layout so the cloud capture
# is byte-for-byte compatible with what Bronze writes. `dags/` holds both.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
from schemas.coincap import CoinCapAssetsResponse  # noqa: E402
from utils.coincap_api import format_coincap_request_error  # noqa: E402
from utils.run_dates import bronze_assets_key  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("capture_daily_snapshot")

COINCAP_API_BASE_URL = os.getenv("COINCAP_API_BASE_URL", "https://rest.coincap.io/v3").rstrip("/")
COINCAP_ASSETS_PATH = os.getenv("COINCAP_ASSETS_PATH", "/assets")
COINCAP_URL = f"{COINCAP_API_BASE_URL}{COINCAP_ASSETS_PATH}"
COINCAP_API_KEY = os.getenv("COINCAP_API_KEY", "").strip()
COINCAP_LIMIT = int(os.getenv("COINCAP_LIMIT", "20"))


def _resolve_capture_date(date_arg: str | None) -> date:
    """The snapshot's partition date: an explicit override, else today (UTC)."""
    if date_arg:
        return datetime.strptime(date_arg, "%Y-%m-%d").date()
    return datetime.now(timezone.utc).date()


def _fetch_snapshot_parquet() -> bytes:
    """Fetch, validate, and serialize the /assets snapshot exactly like Bronze."""
    headers = {}
    if COINCAP_API_KEY:
        headers["Authorization"] = f"Bearer {COINCAP_API_KEY}"
    elif "rest.coincap.io" in COINCAP_API_BASE_URL:
        raise RuntimeError("COINCAP_API_KEY is required. Set it as a GitHub Actions secret.")

    logger.info("Fetching top %d assets from %s", COINCAP_LIMIT, COINCAP_URL)
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
    return buffer.getvalue()


def _upload_to_bucket(parquet_bytes: bytes, key: str) -> None:
    """Write the snapshot to the S3-compatible capture bucket (R2 or S3)."""
    import boto3  # imported here so --dry-run needs no AWS deps

    endpoint_url = os.environ["CAPTURE_S3_ENDPOINT_URL"]
    bucket = os.environ["CAPTURE_S3_BUCKET"]
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ["CAPTURE_S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CAPTURE_S3_SECRET_ACCESS_KEY"],
        region_name=os.getenv("CAPTURE_S3_REGION", "auto"),
    )
    client.put_object(Bucket=bucket, Key=key, Body=parquet_bytes)
    logger.info("Uploaded %d bytes to s3://%s/%s (%s)", len(parquet_bytes), bucket, key, endpoint_url)


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture a daily CoinCap snapshot to a bucket.")
    parser.add_argument("--date", help="Override the capture date (YYYY-MM-DD, UTC). Default: today.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and build parquet but skip the upload.")
    args = parser.parse_args()

    capture_date = _resolve_capture_date(args.date)
    key = bronze_assets_key(capture_date)
    logger.info("Capture date %s -> key %s", capture_date.isoformat(), key)

    parquet_bytes = _fetch_snapshot_parquet()

    if args.dry_run:
        logger.info("--dry-run: built %d parquet bytes, skipping upload.", len(parquet_bytes))
        return 0

    _upload_to_bucket(parquet_bytes, key)
    logger.info("Daily capture complete for %s.", capture_date.isoformat())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
