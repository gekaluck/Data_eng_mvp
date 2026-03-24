"""Gold layer PySpark job for CoinCap Silver data."""

import logging
import os
import sys
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.silver_transform import SPARK_PACKAGES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

JUPYTER_SPARK_IVY_DIR = "/home/airflow/.ivy2"


def build_spark_session(
    minio_endpoint: str,
    minio_user: str,
    minio_password: str,
) -> SparkSession:
    """Create a local-mode SparkSession with Silver and Gold Iceberg catalogs."""
    return (
        SparkSession.builder
        .appName("golden_layer")
        .master("local[2]")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config(
            "spark.driver.extraJavaOptions",
            f"-Divy.cache.dir={JUPYTER_SPARK_IVY_DIR} -Divy.home={JUPYTER_SPARK_IVY_DIR}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver/iceberg")
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.type", "hadoop")
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold/iceberg")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def build_local_test_spark_session(
    silver_warehouse_path: str,
    gold_warehouse_path: str,
    silver_catalog_name: str = "test_silver",
    gold_catalog_name: str = "test_gold",
) -> SparkSession:
    """Create a file-backed SparkSession with separate Silver and Gold catalogs."""
    return (
        SparkSession.builder
        .appName("golden_layer_tests")
        .master("local[2]")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config(
            "spark.driver.extraJavaOptions",
            "-Divy.cache.dir=/opt/spark-ivy -Divy.home=/opt/spark-ivy",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{silver_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{silver_catalog_name}.type", "hadoop")
        .config(f"spark.sql.catalog.{silver_catalog_name}.warehouse", silver_warehouse_path)
        .config(f"spark.sql.catalog.{gold_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{gold_catalog_name}.type", "hadoop")
        .config(f"spark.sql.catalog.{gold_catalog_name}.warehouse", gold_warehouse_path)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _create_tables_if_not_exist(
    spark: SparkSession,
    gold_catalog_name: str = "gold",
) -> None:
    """Create the target Gold namespace and tables if they do not exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {gold_catalog_name}.crypto")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_catalog_name}.crypto.daily_snapshot (
            snapshot_date DATE NOT NULL COMMENT 'Logical snapshot date for the gold record',
            coin_id STRING NOT NULL COMMENT 'CoinCap identifier (e.g. bitcoin)',
            symbol STRING COMMENT 'Trading symbol (e.g. BTC)',
            name STRING COMMENT 'Full display name (e.g. Bitcoin)',
            coin_rank INT COMMENT 'Latest coin rank from Silver metadata',
            price_usd DOUBLE COMMENT 'Price in USD on the logical snapshot date',
            prev_price_usd DOUBLE COMMENT 'Previous available snapshot price in USD',
            price_change_pct DOUBLE COMMENT 'Percent price change vs the previous snapshot',
            price_change_rank INT COMMENT 'Cross-asset rank by price_change_pct for the day',
            market_cap_usd DOUBLE COMMENT 'Market capitalization in USD',
            volume_usd_24hr DOUBLE COMMENT '24hr trading volume in USD',
            vwap_24hr DOUBLE COMMENT '24hr volume-weighted average price'
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_catalog_name}.crypto.market_cap_rank_change (
            snapshot_date DATE NOT NULL COMMENT 'Logical snapshot date for the gold record',
            coin_id STRING NOT NULL COMMENT 'CoinCap identifier (e.g. bitcoin)',
            symbol STRING COMMENT 'Trading symbol (e.g. BTC)',
            name STRING COMMENT 'Full display name (e.g. Bitcoin)',
            price_usd DOUBLE COMMENT 'Price in USD on the logical snapshot date',
            market_cap_usd DOUBLE COMMENT 'Market capitalization in USD on the logical snapshot date',
            mc_rank INT COMMENT 'Market cap rank on the logical snapshot date',
            mc_rank_diff_14d INT COMMENT 'Rank improvement or decline versus 14 days earlier',
            mc_rank_diff_30d INT COMMENT 'Rank improvement or decline versus 30 days earlier',
            price_diff_14d_pct DOUBLE COMMENT 'Percent price change versus 14 days earlier',
            price_diff_30d_pct DOUBLE COMMENT 'Percent price change versus 30 days earlier'
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_catalog_name}.crypto.weekly_rolling_average (
            snapshot_date DATE NOT NULL COMMENT 'Logical snapshot date for the gold record',
            coin_id STRING NOT NULL COMMENT 'CoinCap identifier (e.g. bitcoin)',
            symbol STRING COMMENT 'Trading symbol (e.g. BTC)',
            name STRING COMMENT 'Full display name (e.g. Bitcoin)',
            price_usd DOUBLE COMMENT 'Price in USD on the logical snapshot date',
            market_cap_usd DOUBLE COMMENT 'Market capitalization in USD on the logical snapshot date',
            volume_usd_24hr DOUBLE COMMENT '24hr trading volume in USD on the logical snapshot date',
            vwap_24hr DOUBLE COMMENT '24hr volume-weighted average price on the logical snapshot date',
            wkly_roll_avg_price DOUBLE COMMENT '7-row rolling average of price_usd per coin',
            wkly_roll_avg_volume DOUBLE COMMENT '7-row rolling average of volume_usd_24hr per coin'
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def build_daily_snapshot(
    spark: SparkSession,
    logical_date: date,
    silver_catalog_name: str = "silver",
):
    """Build the first Gold table for the given logical date."""
    window_start = logical_date - timedelta(days=1)

    snapshots_df = (
        spark.table(f"{silver_catalog_name}.crypto.price_snapshots")
        .where(
            (F.col("snapshot_date") >= F.lit(window_start))
            & (F.col("snapshot_date") <= F.lit(logical_date))
        )
        .drop("change_percent_24hr")
    )

    price_window = Window.partitionBy("coin_id").orderBy("snapshot_date")
    ranked_window = Window.orderBy(F.desc("price_change_pct"))

    ranked_df = (
        snapshots_df
        .withColumn("prev_price_usd", F.lag("price_usd", 1).over(price_window))
        .withColumn(
            "price_change_pct",
            (F.col("price_usd") - F.col("prev_price_usd"))
            / F.col("prev_price_usd")
            * F.lit(100.0),
        )
        .where(F.col("prev_price_usd").isNotNull())
        .where(F.col("snapshot_date") == F.lit(logical_date))
        .withColumn("price_change_rank", F.rank().over(ranked_window))
    )

    coins_df = spark.table(f"{silver_catalog_name}.crypto.coins").select(
        F.col("id").alias("coin_id"),
        "symbol",
        "name",
        F.col("rank").alias("coin_rank"),
    )

    return ranked_df.join(coins_df, on="coin_id", how="left").select(
        "snapshot_date",
        "coin_id",
        "symbol",
        "name",
        "coin_rank",
        "price_usd",
        "prev_price_usd",
        "price_change_pct",
        "price_change_rank",
        "market_cap_usd",
        "volume_usd_24hr",
        "vwap_24hr",
    )


def build_market_cap_rank_change(
    spark: SparkSession,
    logical_date: date,
    silver_catalog_name: str = "silver",
):
    """Build 14d and 30d market-cap rank deltas for the logical date."""
    d14 = logical_date - timedelta(days=14)
    d30 = logical_date - timedelta(days=30)

    base_df = (
        spark.table(f"{silver_catalog_name}.crypto.price_snapshots")
        .where(F.col("snapshot_date").isin([logical_date, d14, d30]))
        .drop("change_percent_24hr", "volume_usd_24hr", "vwap_24hr")
    )

    mc_rank_window = Window.partitionBy("snapshot_date").orderBy(F.desc("market_cap_usd"))
    with_mc_rank_df = base_df.withColumn("mc_rank", F.rank().over(mc_rank_window))

    df_today = with_mc_rank_df.where(F.col("snapshot_date") == F.lit(logical_date))
    df_d14 = with_mc_rank_df.where(F.col("snapshot_date") == F.lit(d14)).select(
        "coin_id",
        F.col("price_usd").alias("price_usd_t_14"),
        F.col("market_cap_usd").alias("market_cap_usd_t_14"),
        F.col("mc_rank").alias("mc_rank_t_14"),
    )
    df_d30 = with_mc_rank_df.where(F.col("snapshot_date") == F.lit(d30)).select(
        "coin_id",
        F.col("price_usd").alias("price_usd_t_30"),
        F.col("market_cap_usd").alias("market_cap_usd_t_30"),
        F.col("mc_rank").alias("mc_rank_t_30"),
    )

    joined_df = df_today.join(df_d14, on="coin_id", how="left").join(
        df_d30,
        on="coin_id",
        how="left",
    )

    coins_df = spark.table(f"{silver_catalog_name}.crypto.coins").select(
        F.col("id").alias("coin_id"),
        "symbol",
        "name",
    )

    return (
        joined_df
        .withColumn(
            "mc_rank_diff_14d",
            (-F.col("mc_rank") + F.col("mc_rank_t_14")).cast("int"),
        )
        .withColumn(
            "mc_rank_diff_30d",
            (-F.col("mc_rank") + F.col("mc_rank_t_30")).cast("int"),
        )
        .withColumn(
            "price_diff_14d_pct",
            (F.col("price_usd") - F.col("price_usd_t_14"))
            / F.col("price_usd_t_14")
            * F.lit(100.0),
        )
        .withColumn(
            "price_diff_30d_pct",
            (F.col("price_usd") - F.col("price_usd_t_30"))
            / F.col("price_usd_t_30")
            * F.lit(100.0),
        )
        .drop(
            "price_usd_t_14",
            "price_usd_t_30",
            "mc_rank_t_14",
            "mc_rank_t_30",
            "market_cap_usd_t_14",
            "market_cap_usd_t_30",
        )
        .join(coins_df, on="coin_id", how="left")
        .select(
            "snapshot_date",
            "coin_id",
            "symbol",
            "name",
            "price_usd",
            "market_cap_usd",
            "mc_rank",
            "mc_rank_diff_14d",
            "mc_rank_diff_30d",
            "price_diff_14d_pct",
            "price_diff_30d_pct",
        )
    )


def build_weekly_rolling_average(
    spark: SparkSession,
    logical_date: date,
    silver_catalog_name: str = "silver",
):
    """Build rolling 7-day price and volume averages for the logical date."""
    d7 = logical_date - timedelta(days=7)
    rolling_window = Window.partitionBy("coin_id").orderBy("snapshot_date").rowsBetween(-6, 0)

    base_df = (
        spark.table(f"{silver_catalog_name}.crypto.price_snapshots")
        .where(
            (F.col("snapshot_date") > F.lit(d7))
            & (F.col("snapshot_date") <= F.lit(logical_date))
        )
        .drop("change_percent_24hr")
    )

    coins_df = spark.table(f"{silver_catalog_name}.crypto.coins").select(
        F.col("id").alias("coin_id"),
        "symbol",
        "name",
    )

    return (
        base_df
        .withColumn("wkly_roll_avg_price", F.avg("price_usd").over(rolling_window))
        .withColumn("wkly_roll_avg_volume", F.avg("volume_usd_24hr").over(rolling_window))
        .where(F.col("snapshot_date") == F.lit(logical_date))
        .join(coins_df, on="coin_id", how="left")
        .select(
            "snapshot_date",
            "coin_id",
            "symbol",
            "name",
            "price_usd",
            "market_cap_usd",
            "volume_usd_24hr",
            "vwap_24hr",
            "wkly_roll_avg_price",
            "wkly_roll_avg_volume",
        )
    )


def overwrite_daily_snapshot(
    daily_snapshot_df,
    gold_catalog_name: str = "gold",
) -> None:
    """Overwrite the target Gold partition for the built daily snapshot."""
    daily_snapshot_df.writeTo(
        f"{gold_catalog_name}.crypto.daily_snapshot"
    ).overwritePartitions()


def overwrite_market_cap_rank_change(
    market_cap_rank_change_df,
    gold_catalog_name: str = "gold",
) -> None:
    """Overwrite the target Gold partition for market-cap rank deltas."""
    market_cap_rank_change_df.writeTo(
        f"{gold_catalog_name}.crypto.market_cap_rank_change"
    ).overwritePartitions()


def overwrite_weekly_rolling_average(
    weekly_rolling_average_df,
    gold_catalog_name: str = "gold",
) -> None:
    """Overwrite the target Gold partition for weekly rolling averages."""
    weekly_rolling_average_df.writeTo(
        f"{gold_catalog_name}.crypto.weekly_rolling_average"
    ).overwritePartitions()


def transform(
    spark: SparkSession,
    logical_date: date,
    silver_catalog_name: str = "silver",
    gold_catalog_name: str = "gold",
) -> None:
    """Materialize Gold outputs for the provided logical date."""
    _create_tables_if_not_exist(spark, gold_catalog_name=gold_catalog_name)

    daily_snapshot_df = build_daily_snapshot(
        spark,
        logical_date,
        silver_catalog_name=silver_catalog_name,
    )
    market_cap_rank_change_df = build_market_cap_rank_change(
        spark,
        logical_date,
        silver_catalog_name=silver_catalog_name,
    )
    weekly_rolling_average_df = build_weekly_rolling_average(
        spark,
        logical_date,
        silver_catalog_name=silver_catalog_name,
    )

    overwrite_daily_snapshot(
        daily_snapshot_df,
        gold_catalog_name=gold_catalog_name,
    )
    overwrite_market_cap_rank_change(
        market_cap_rank_change_df,
        gold_catalog_name=gold_catalog_name,
    )
    overwrite_weekly_rolling_average(
        weekly_rolling_average_df,
        gold_catalog_name=gold_catalog_name,
    )

    logger.info(
        "Wrote %d rows into %s.crypto.daily_snapshot (date=%s)",
        daily_snapshot_df.count(),
        gold_catalog_name,
        logical_date,
    )
    logger.info(
        "Wrote %d rows into %s.crypto.market_cap_rank_change (date=%s)",
        market_cap_rank_change_df.count(),
        gold_catalog_name,
        logical_date,
    )
    logger.info(
        "Wrote %d rows into %s.crypto.weekly_rolling_average (date=%s)",
        weekly_rolling_average_df.count(),
        gold_catalog_name,
        logical_date,
    )


def validate_gold_result(
    spark: SparkSession,
    logical_date: date,
    gold_catalog_name: str = "gold",
) -> None:
    """Check that all Gold tables contain rows for the requested date."""
    daily_snapshot_count = (
        spark.table(f"{gold_catalog_name}.crypto.daily_snapshot")
        .filter(F.col("snapshot_date") == F.lit(logical_date))
        .count()
    )
    market_cap_rank_change_count = (
        spark.table(f"{gold_catalog_name}.crypto.market_cap_rank_change")
        .filter(F.col("snapshot_date") == F.lit(logical_date))
        .count()
    )
    weekly_rolling_average_count = (
        spark.table(f"{gold_catalog_name}.crypto.weekly_rolling_average")
        .filter(F.col("snapshot_date") == F.lit(logical_date))
        .count()
    )

    if daily_snapshot_count == 0:
        raise ValueError(
            f"{gold_catalog_name}.crypto.daily_snapshot has no rows for date {logical_date} after transformation"
        )
    if market_cap_rank_change_count == 0:
        raise ValueError(
            f"{gold_catalog_name}.crypto.market_cap_rank_change has no rows for date {logical_date} after transformation"
        )
    if weekly_rolling_average_count == 0:
        raise ValueError(
            f"{gold_catalog_name}.crypto.weekly_rolling_average has no rows for date {logical_date} after transformation"
        )


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <YYYY-MM-DD>", file=sys.stderr)
        sys.exit(1)

    logical_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]

    spark = build_spark_session(minio_endpoint, minio_user, minio_password)
    try:
        transform(spark, logical_date)
        validate_gold_result(spark, logical_date)
        logger.info("Gold transform complete for %s", logical_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
