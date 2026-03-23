"""Silver transform for Bronze history backfill datasets."""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from .silver_transform import _create_tables_if_not_exist, build_spark_session
except ImportError:  # pragma: no cover - script execution path
    from silver_transform import _create_tables_if_not_exist, build_spark_session

try:
    from utils.run_dates import bronze_history_backfill_key
except ImportError:  # pragma: no cover - script execution path
    sys.path.insert(0, "/opt/airflow/dags")
    from utils.run_dates import bronze_history_backfill_key


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def bronze_history_backfill_path(
    anchor_snapshot_date: date,
    backfill_days: int,
    dataset_name: str,
) -> str:
    """Return the Bronze S3A path for a backfill dataset."""
    return "s3a://bronze/" + bronze_history_backfill_key(
        anchor_snapshot_date,
        backfill_days,
        dataset_name,
    )


def _optional_raw_col(df, column_name: str):
    return F.col(column_name) if column_name in df.columns else F.lit(None)


def _with_snapshot_date(df):
    event_ts = F.to_timestamp(
        F.from_unixtime(_optional_raw_col(df, "time").cast("double") / F.lit(1000.0))
    )
    return df.withColumn(
        "snapshot_date",
        F.coalesce(
            F.to_date(_optional_raw_col(df, "date")),
            F.to_date(event_ts),
        ),
    )


def read_history_bronze(
    spark: SparkSession,
    anchor_snapshot_date: date,
    backfill_days: int,
    dataset_name: str,
):
    """Read a Bronze history backfill parquet dataset."""
    bronze_path = bronze_history_backfill_path(
        anchor_snapshot_date,
        backfill_days,
        dataset_name,
    )
    logger.info("Reading Bronze history from %s", bronze_path)
    return spark.read.parquet(bronze_path)


def clean_asset_history(raw_df):
    """Cast Bronze asset history to the Silver price-snapshot shape."""
    dated_df = _with_snapshot_date(raw_df)
    return (
        dated_df.select(
            F.col("coin_id"),
            F.col("snapshot_date"),
            _optional_raw_col(dated_df, "priceUsd").cast("double").alias("price_usd"),
            _optional_raw_col(dated_df, "volumeUsd24Hr")
            .cast("double")
            .alias("volume_usd_24hr"),
        )
        .filter(F.col("snapshot_date").isNotNull())
        .dropDuplicates(["coin_id", "snapshot_date"])
    )


def clean_asset_market_cap_history(raw_df):
    """Cast Bronze asset market-cap history to a typed Silver shape."""
    dated_df = _with_snapshot_date(raw_df)
    return (
        dated_df.select(
            F.col("coin_id"),
            F.col("snapshot_date"),
            _optional_raw_col(dated_df, "marketCapUsd")
            .cast("double")
            .alias("market_cap_usd"),
        )
        .filter(F.col("snapshot_date").isNotNull())
        .dropDuplicates(["coin_id", "snapshot_date"])
    )


def clean_total_market_cap_history(raw_df):
    """Cast Bronze total market-cap history to a typed Silver shape."""
    dated_df = _with_snapshot_date(raw_df)
    return (
        dated_df.select(
            F.col("snapshot_date"),
            _optional_raw_col(dated_df, "totalMarketCapUsd")
            .cast("double")
            .alias("total_market_cap_usd"),
        )
        .filter(F.col("snapshot_date").isNotNull())
        .dropDuplicates(["snapshot_date"])
    )


def merge_price_snapshots(
    spark: SparkSession,
    price_history_df,
    asset_market_cap_df,
    catalog_name: str = "silver",
) -> None:
    """Merge backfilled history rows into the existing price_snapshots table."""
    snapshots_df = (
        price_history_df.join(
            asset_market_cap_df,
            on=["coin_id", "snapshot_date"],
            how="left",
        )
        .select(
            "coin_id",
            "snapshot_date",
            "price_usd",
            "market_cap_usd",
            "volume_usd_24hr",
            F.lit(None).cast("double").alias("change_percent_24hr"),
            F.lit(None).cast("double").alias("vwap_24hr"),
        )
        .dropDuplicates(["coin_id", "snapshot_date"])
    )
    snapshots_df.createOrReplaceTempView("history_price_snapshots")
    spark.sql(
        f"""
        MERGE INTO {catalog_name}.crypto.price_snapshots AS target
        USING history_price_snapshots AS source
        ON target.coin_id = source.coin_id
           AND target.snapshot_date = source.snapshot_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_asset_market_cap_history(
    spark: SparkSession,
    asset_market_cap_df,
    catalog_name: str = "silver",
) -> None:
    """Merge typed asset market-cap history into Silver."""
    history_df = asset_market_cap_df.select(
        "coin_id",
        "snapshot_date",
        F.col("market_cap_usd"),
    ).dropDuplicates(["coin_id", "snapshot_date"])
    history_df.createOrReplaceTempView("history_asset_market_caps")
    spark.sql(
        f"""
        MERGE INTO {catalog_name}.crypto.asset_market_cap_history AS target
        USING history_asset_market_caps AS source
        ON target.coin_id = source.coin_id
           AND target.snapshot_date = source.snapshot_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_total_market_cap_history(
    spark: SparkSession,
    total_market_cap_df,
    catalog_name: str = "silver",
) -> None:
    """Merge typed total market-cap history into Silver."""
    total_market_cap_df.createOrReplaceTempView("history_total_market_caps")
    spark.sql(
        f"""
        MERGE INTO {catalog_name}.crypto.total_market_cap_history AS target
        USING history_total_market_caps AS source
        ON target.snapshot_date = source.snapshot_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def validate_history_backfill_result(
    spark: SparkSession,
    window_start_date: date,
    window_end_date: date,
    catalog_name: str = "silver",
) -> None:
    """Ensure the target Silver tables now contain rows for the backfill window."""
    snapshot_count = (
        spark.table(f"{catalog_name}.crypto.price_snapshots")
        .filter(F.col("snapshot_date").between(window_start_date, window_end_date))
        .count()
    )
    asset_market_cap_count = (
        spark.table(f"{catalog_name}.crypto.asset_market_cap_history")
        .filter(F.col("snapshot_date").between(window_start_date, window_end_date))
        .count()
    )
    total_market_cap_count = (
        spark.table(f"{catalog_name}.crypto.total_market_cap_history")
        .filter(F.col("snapshot_date").between(window_start_date, window_end_date))
        .count()
    )

    if snapshot_count == 0:
        raise ValueError("price_snapshots backfill wrote no rows")
    if asset_market_cap_count == 0:
        raise ValueError("asset_market_cap_history backfill wrote no rows")
    if total_market_cap_count == 0:
        raise ValueError("total_market_cap_history backfill wrote no rows")


def transform_history_backfill(
    spark: SparkSession,
    anchor_snapshot_date: date,
    backfill_days: int,
    catalog_name: str = "silver",
) -> None:
    """Read Bronze history backfill datasets and merge them into Silver."""
    asset_history_raw = read_history_bronze(
        spark,
        anchor_snapshot_date,
        backfill_days,
        "asset_history",
    )
    asset_market_cap_raw = read_history_bronze(
        spark,
        anchor_snapshot_date,
        backfill_days,
        "asset_market_cap_history",
    )
    total_market_cap_raw = read_history_bronze(
        spark,
        anchor_snapshot_date,
        backfill_days,
        "total_market_cap_history",
    )

    price_history_df = clean_asset_history(asset_history_raw)
    asset_market_cap_df = clean_asset_market_cap_history(asset_market_cap_raw)
    total_market_cap_df = clean_total_market_cap_history(total_market_cap_raw)

    _create_tables_if_not_exist(spark, catalog_name=catalog_name)
    merge_price_snapshots(
        spark,
        price_history_df,
        asset_market_cap_df,
        catalog_name=catalog_name,
    )
    merge_asset_market_cap_history(
        spark,
        asset_market_cap_df,
        catalog_name=catalog_name,
    )
    merge_total_market_cap_history(
        spark,
        total_market_cap_df,
        catalog_name=catalog_name,
    )


def main() -> None:
    if len(sys.argv) != 3:
        print(
            f"Usage: python {sys.argv[0]} <anchor_snapshot_date> <backfill_days>",
            file=sys.stderr,
        )
        sys.exit(1)

    anchor_snapshot_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    backfill_days = int(sys.argv[2])
    window_start_date = anchor_snapshot_date.fromordinal(
        anchor_snapshot_date.toordinal() - backfill_days
    )
    window_end_date = anchor_snapshot_date.fromordinal(anchor_snapshot_date.toordinal() - 1)

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]

    spark = build_spark_session(minio_endpoint, minio_user, minio_password)
    try:
        transform_history_backfill(
            spark,
            anchor_snapshot_date,
            backfill_days,
        )
        validate_history_backfill_result(
            spark,
            window_start_date,
            window_end_date,
        )
        logger.info(
            "Silver history backfill complete for anchor=%s backfill_days=%s",
            anchor_snapshot_date,
            backfill_days,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
