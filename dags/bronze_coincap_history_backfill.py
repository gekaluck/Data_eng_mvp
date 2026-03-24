"""Bronze history backfill DAG for CoinCap asset and market-cap history."""

from __future__ import annotations

import io
import json
import logging
import os
import subprocess
import sys
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime, duration

from schemas.coincap_history import (
    CoinCapAssetHistoryResponse,
    CoinCapAssetMarketCapHistoryResponse,
    CoinCapTotalMarketCapHistoryResponse,
)
from utils.coincap_api import format_coincap_request_error
from utils.run_dates import (
    bronze_history_backfill_key,
    date_to_unix_ms_end,
    date_to_unix_ms_start,
    resolve_int_param,
    resolve_optional_date_param,
)

logger = logging.getLogger(__name__)

COINCAP_API_BASE_URL = os.getenv("COINCAP_API_BASE_URL", "https://rest.coincap.io/v3").rstrip("/")
COINCAP_API_KEY = os.getenv("COINCAP_API_KEY", "").strip()
COINCAP_ASSET_HISTORY_PATH_TEMPLATE = os.getenv(
    "COINCAP_ASSET_HISTORY_PATH_TEMPLATE",
    "/assets/{id}/history",
)
COINCAP_ASSET_MARKETCAP_HISTORY_PATH_TEMPLATE = os.getenv(
    "COINCAP_ASSET_MARKETCAP_HISTORY_PATH_TEMPLATE",
    "/assets/{id}/marketcap-history",
)
COINCAP_TOTAL_MARKETCAP_HISTORY_PATH = os.getenv(
    "COINCAP_TOTAL_MARKETCAP_HISTORY_PATH",
    "/assets/totals/total-marketcap-history",
)
BRONZE_BUCKET = "bronze"
S3_CONN_ID = "minio_s3"
PLAN_PREFIX = "PLAN_JSON:"


def _build_headers() -> dict[str, str]:
    if COINCAP_API_KEY:
        return {"Authorization": f"Bearer {COINCAP_API_KEY}"}
    if "rest.coincap.io" in COINCAP_API_BASE_URL:
        raise RuntimeError(
            "COINCAP_API_KEY is required for the current CoinCap API. "
            "Set it in .env before running bronze_coincap_history_backfill."
        )
    return {}


def _coerce_history_payload(raw_json):
    if isinstance(raw_json, dict) and isinstance(raw_json.get("data"), list):
        return raw_json
    if isinstance(raw_json, list):
        return {"data": raw_json}
    raise ValueError(f"Unexpected history payload shape: {type(raw_json)!r}")


def _build_endpoint(path_template: str, coin_id: str | None = None) -> str:
    path = path_template.format(id=coin_id, slug=coin_id)
    return f"{COINCAP_API_BASE_URL}{path}"


def _fetch_json(url: str, headers: dict[str, str], params: dict[str, int | str]):
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(format_coincap_request_error(exc, url)) from exc


def _write_records_to_bronze(
    records: list[dict],
    s3_key: str,
) -> None:
    table = pa.Table.from_pylist(records)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    S3Hook(aws_conn_id=S3_CONN_ID).load_bytes(
        bytes_data=buffer.getvalue(),
        key=s3_key,
        bucket_name=BRONZE_BUCKET,
        replace=True,
    )
    logger.info("Uploaded %d records to s3://%s/%s", len(records), BRONZE_BUCKET, s3_key)


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
    dag_id="bronze_coincap_history_backfill",
    description="Fetch historical CoinCap asset and market-cap data into Bronze",
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
            description="Number of days to fetch before the anchor date.",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": duration(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=["bronze", "coincap", "backfill"],
)
def bronze_coincap_history_backfill():
    @task()
    def discover_backfill_plan(**context):
        """Read current Silver state and return the fixed backfill plan."""
        anchor_snapshot_date = resolve_optional_date_param(context, "anchor_snapshot_date")
        backfill_days = resolve_int_param(context, "backfill_days", default=60)

        plan_args = [
            sys.executable,
            "/opt/airflow/spark/silver_backfill_plan.py",
            anchor_snapshot_date.isoformat() if anchor_snapshot_date else "auto",
            str(backfill_days),
        ]
        result = subprocess.run(
            plan_args,
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

        plan = _extract_backfill_plan(result.stdout, result.stderr)
        logger.info(
            "Backfill plan resolved: anchor=%s window=%s..%s coins=%d",
            plan["anchor_snapshot_date"],
            plan["window_start_date"],
            plan["window_end_date"],
            len(plan["coin_ids"]),
        )
        return plan

    @task()
    def fetch_validate_upload_history(plan: dict):
        """Fetch the history endpoints for the resolved plan and upload Bronze parquet."""
        headers = _build_headers()
        window_start = date.fromisoformat(plan["window_start_date"])
        window_end = date.fromisoformat(plan["window_end_date"])
        anchor_snapshot_date = date.fromisoformat(plan["anchor_snapshot_date"])
        backfill_days = int(plan["backfill_days"])
        params = {
            "interval": "d1",
            "start": date_to_unix_ms_start(window_start),
            "end": date_to_unix_ms_end(window_end),
        }

        asset_history_records: list[dict] = []
        asset_market_cap_records: list[dict] = []

        for coin_id in plan["coin_ids"]:
            asset_history_url = _build_endpoint(
                COINCAP_ASSET_HISTORY_PATH_TEMPLATE,
                coin_id=coin_id,
            )
            asset_market_cap_url = _build_endpoint(
                COINCAP_ASSET_MARKETCAP_HISTORY_PATH_TEMPLATE,
                coin_id=coin_id,
            )

            asset_history_payload = _coerce_history_payload(
                _fetch_json(asset_history_url, headers=headers, params=params)
            )
            validated_asset_history = CoinCapAssetHistoryResponse.model_validate(
                asset_history_payload
            )
            asset_history_records.extend(
                {
                    "coin_id": coin_id,
                    "anchor_snapshot_date": plan["anchor_snapshot_date"],
                    "window_start_date": plan["window_start_date"],
                    "window_end_date": plan["window_end_date"],
                    **point.model_dump(by_alias=True),
                }
                for point in validated_asset_history.data
            )

            asset_market_cap_payload = _coerce_history_payload(
                _fetch_json(asset_market_cap_url, headers=headers, params=params)
            )
            validated_asset_market_cap = CoinCapAssetMarketCapHistoryResponse.model_validate(
                asset_market_cap_payload
            )
            asset_market_cap_records.extend(
                {
                    "coin_id": coin_id,
                    "anchor_snapshot_date": plan["anchor_snapshot_date"],
                    "window_start_date": plan["window_start_date"],
                    "window_end_date": plan["window_end_date"],
                    **point.model_dump(by_alias=True),
                }
                for point in validated_asset_market_cap.data
            )

        total_market_cap_payload = _coerce_history_payload(
            _fetch_json(
                _build_endpoint(COINCAP_TOTAL_MARKETCAP_HISTORY_PATH),
                headers=headers,
                params=params,
            )
        )
        validated_total_market_cap = CoinCapTotalMarketCapHistoryResponse.model_validate(
            total_market_cap_payload
        )
        total_market_cap_records = [
            {
                "anchor_snapshot_date": plan["anchor_snapshot_date"],
                "window_start_date": plan["window_start_date"],
                "window_end_date": plan["window_end_date"],
                **point.model_dump(by_alias=True),
            }
            for point in validated_total_market_cap.data
        ]

        if not asset_history_records:
            raise ValueError("Asset history backfill returned no rows")
        if not asset_market_cap_records:
            raise ValueError("Asset market-cap history backfill returned no rows")
        if not total_market_cap_records:
            raise ValueError("Total market-cap history backfill returned no rows")

        _write_records_to_bronze(
            asset_history_records,
            bronze_history_backfill_key(
                anchor_snapshot_date,
                backfill_days,
                "asset_history",
            ),
        )
        _write_records_to_bronze(
            asset_market_cap_records,
            bronze_history_backfill_key(
                anchor_snapshot_date,
                backfill_days,
                "asset_market_cap_history",
            ),
        )
        _write_records_to_bronze(
            total_market_cap_records,
            bronze_history_backfill_key(
                anchor_snapshot_date,
                backfill_days,
                "total_market_cap_history",
            ),
        )

    plan = discover_backfill_plan()
    bronze_backfill = fetch_validate_upload_history(plan)
    trigger_silver_backfill = TriggerDagRunOperator(
        task_id="trigger_silver_history_backfill",
        trigger_dag_id="silver_coincap_history_backfill",
        conf=plan,
        wait_for_completion=False,
    )

    plan >> bronze_backfill >> trigger_silver_backfill


bronze_coincap_history_backfill()
