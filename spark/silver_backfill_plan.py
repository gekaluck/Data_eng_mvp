"""Discover the Silver anchor date and coin universe for a history backfill."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime

try:
    from .silver_transform import build_spark_session
except ImportError:  # pragma: no cover - script execution path
    from silver_transform import build_spark_session

try:
    from utils.run_dates import resolve_backfill_window
except ImportError:  # pragma: no cover - script execution path
    sys.path.insert(0, "/opt/airflow/dags")
    from utils.run_dates import resolve_backfill_window


def main() -> None:
    if len(sys.argv) != 3:
        print(
            f"Usage: python {sys.argv[0]} <anchor_snapshot_date|auto> <backfill_days>",
            file=sys.stderr,
        )
        sys.exit(1)

    anchor_arg = sys.argv[1]
    backfill_days = int(sys.argv[2])

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]

    spark = build_spark_session(minio_endpoint, minio_user, minio_password)
    try:
        coins_df = spark.table("silver.crypto.coins").select("id").orderBy("id")
        coin_ids = [row["id"] for row in coins_df.collect()]
        if not coin_ids:
            raise ValueError("silver.crypto.coins is empty; there is nothing to backfill")

        if anchor_arg.lower() == "auto":
            anchor_row = (
                spark.table("silver.crypto.price_snapshots")
                .selectExpr("min(snapshot_date) as anchor_snapshot_date")
                .collect()[0]
            )
            anchor_snapshot_date = anchor_row["anchor_snapshot_date"]
        else:
            anchor_snapshot_date = datetime.strptime(anchor_arg, "%Y-%m-%d").date()

        if anchor_snapshot_date is None:
            raise ValueError(
                "silver.crypto.price_snapshots is empty; cannot infer an anchor date"
            )

        window_start_date, window_end_date = resolve_backfill_window(
            anchor_snapshot_date,
            backfill_days,
        )
        print(
            "PLAN_JSON:"
            + json.dumps(
                {
                    "coin_ids": coin_ids,
                    "anchor_snapshot_date": anchor_snapshot_date.isoformat(),
                    "backfill_days": backfill_days,
                    "window_start_date": window_start_date.isoformat(),
                    "window_end_date": window_end_date.isoformat(),
                }
            ),
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
