"""Audit Bronze snapshots for mislabelled or duplicated fetches (H5).

Reads the provenance columns (`api_timestamp_ms`, `fetched_at_utc`) written by
`dags/utils/bronze_snapshot.py` and reports two signatures directly, instead of
inferring them from price coincidences the way I10 and I17 had to be:

- a snapshot fetched too long after (or before) the date it is labelled with —
  the "live response stored under a past date" defect;
- two dates sharing one CoinCap response timestamp — the same response copied
  under two labels.

Objects written before H5 landed carry neither column. They are counted and
reported as unauditable rather than silently treated as clean.

Run it from inside the stack, where MinIO is reachable:

    docker compose exec airflow-scheduler python /opt/airflow/scripts/audit_bronze_provenance.py

Exit code is 1 when anything is flagged, so it can be wired into a check later.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
from utils.bronze_snapshot import (  # noqa: E402
    DEFAULT_MAX_FETCH_LAG_HOURS,
    SnapshotProvenance,
    fetch_lag,
    find_duplicate_fetches,
    find_mislabelled_snapshots,
    read_snapshot_provenance,
    summarize_coverage,
)
from utils.capture_sync import CAPTURE_PREFIX, dates_from_keys  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("audit_bronze_provenance")

BRONZE_BUCKET = "bronze"


def _bronze_client():
    """S3 client for the local MinIO, using the same env vars the DAGs require."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )


def collect_provenance(client, bucket: str = BRONZE_BUCKET) -> list[SnapshotProvenance]:
    """Read the provenance of every Bronze `/assets` snapshot in the bucket."""
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=CAPTURE_PREFIX):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))

    # One object per date in this layout, so a date -> key map is enough.
    keys_by_date = {}
    for key in keys:
        for partition_date in dates_from_keys([key]):
            keys_by_date[partition_date] = key

    provenances: list[SnapshotProvenance] = []
    for partition_date in sorted(keys_by_date):
        body = client.get_object(Bucket=bucket, Key=keys_by_date[partition_date])["Body"].read()
        provenances.append(read_snapshot_provenance(body, partition_date))

    return provenances


def report(provenances: list[SnapshotProvenance], max_lag_hours: int) -> int:
    """Print the audit and return the process exit code."""
    logger.info(summarize_coverage(provenances))

    for provenance in provenances:
        lag = fetch_lag(provenance)
        if lag is not None:
            logger.debug(
                "%s fetched %s (lag %.1fh)",
                provenance.partition_date,
                provenance.fetched_at_utc.isoformat(),
                lag.total_seconds() / 3600,
            )

    mislabelled = find_mislabelled_snapshots(provenances, max_lag_hours=max_lag_hours)
    duplicates = find_duplicate_fetches(provenances)

    for finding in mislabelled:
        logger.error(
            "MISLABELLED %s: %s (fetched_at_utc=%s)",
            finding.partition_date,
            finding.reason,
            finding.fetched_at_utc.isoformat(),
        )
    for api_timestamp_ms, dates in duplicates:
        logger.error(
            "DUPLICATE FETCH: dates %s all carry api_timestamp_ms=%d — one response, several labels",
            ", ".join(d.isoformat() for d in dates),
            api_timestamp_ms,
        )

    if mislabelled or duplicates:
        logger.error(
            "Audit failed: %d mislabelled, %d duplicated.", len(mislabelled), len(duplicates)
        )
        return 1

    logger.info("Audit clean: no mislabelled or duplicated snapshots among the auditable dates.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--max-lag-hours",
        type=int,
        default=DEFAULT_MAX_FETCH_LAG_HOURS,
        help=f"How late a fetch may be for its partition date (default {DEFAULT_MAX_FETCH_LAG_HOURS}).",
    )
    args = parser.parse_args()

    provenances = collect_provenance(_bronze_client())
    if not provenances:
        logger.error("No Bronze snapshots found under s3://%s/%s", BRONZE_BUCKET, CAPTURE_PREFIX)
        return 1

    return report(provenances, max_lag_hours=args.max_lag_hours)


if __name__ == "__main__":
    raise SystemExit(main())
