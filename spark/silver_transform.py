"""Silver layer PySpark job for CoinCap Bronze data."""

import logging
import os
import sys
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    ]
)


def build_spark_session(
    minio_endpoint: str, minio_user: str, minio_password: str
) -> SparkSession:
    """Create a local-mode SparkSession with Iceberg and MinIO configured."""
    return (
        SparkSession.builder
        .appName("silver_coincap")
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
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver/iceberg")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def build_local_test_spark_session(
    warehouse_path: str,
    catalog_name: str = "test_silver",
) -> SparkSession:
    """Create a file-backed Iceberg SparkSession for tests."""
    return (
        SparkSession.builder
        .appName("silver_coincap_tests")
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
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def bronze_path_for_date(logical_date: date) -> str:
    """Return the default Bronze object path for a given date."""
    return (
        "s3a://bronze/crypto/assets/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        "assets.parquet"
    )


def _create_tables_if_not_exist(
    spark: SparkSession,
    catalog_name: str = "silver",
) -> None:
    """Create the target namespace and tables if they do not exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.crypto")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.crypto.coins (
            id STRING NOT NULL COMMENT 'CoinCap identifier (e.g. bitcoin)',
            symbol STRING COMMENT 'Trading symbol (e.g. BTC)',
            name STRING COMMENT 'Full display name (e.g. Bitcoin)',
            rank INT COMMENT 'Market-cap rank at last observation',
            supply DOUBLE COMMENT 'Circulating supply',
            max_supply DOUBLE COMMENT 'Maximum supply; null means uncapped'
        ) USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.crypto.price_snapshots (
            coin_id STRING NOT NULL COMMENT 'FK to crypto.coins.id',
            snapshot_date DATE NOT NULL COMMENT 'Logical execution date',
            price_usd DOUBLE COMMENT 'USD price at snapshot time',
            market_cap_usd DOUBLE COMMENT 'Total market cap in USD',
            volume_usd_24hr DOUBLE COMMENT 'Trading volume over last 24h',
            change_percent_24hr DOUBLE COMMENT 'Price change percent over last 24h',
            vwap_24hr DOUBLE COMMENT 'Volume-weighted average price over 24h'
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.crypto.asset_market_cap_history (
            coin_id STRING NOT NULL COMMENT 'FK to crypto.coins.id',
            snapshot_date DATE NOT NULL COMMENT 'Calendar date of the history point',
            market_cap_usd DOUBLE COMMENT 'Asset market cap in USD'
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.crypto.total_market_cap_history (
            snapshot_date DATE NOT NULL COMMENT 'Calendar date of the history point',
            total_market_cap_usd DOUBLE COMMENT 'Total crypto market cap in USD'
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def read_bronze(
    spark: SparkSession,
    logical_date: date,
    bronze_path: str | None = None,
):
    """Read the Bronze Parquet for the given date or explicit path."""
    bronze_path = bronze_path or bronze_path_for_date(logical_date)
    logger.info("Reading bronze from: %s", bronze_path)
    df = spark.read.parquet(bronze_path)
    logger.info("Read %d rows from bronze", df.count())
    return df


def clean_and_cast_bronze(raw_df):
    """Cast Bronze string fields to Silver types and rename to snake_case."""
    return raw_df.select(
        F.col("id"),
        F.col("symbol"),
        F.col("name"),
        F.col("rank").cast("int").alias("rank"),
        F.col("supply").cast("double").alias("supply"),
        F.col("maxSupply").cast("double").alias("max_supply"),
        F.col("priceUsd").cast("double").alias("price_usd"),
        F.col("marketCapUsd").cast("double").alias("market_cap_usd"),
        F.col("volumeUsd24Hr").cast("double").alias("volume_usd_24hr"),
        F.col("changePercent24Hr").cast("double").alias("change_percent_24hr"),
        F.col("vwap24Hr").cast("double").alias("vwap_24hr"),
    )


def validate_crucial_fields(cleaned_df) -> None:
    """Fail if crucial Silver fields are unexpectedly null after casting."""
    required_fields = ["id", "symbol", "name", "price_usd"]
    null_counts = (
        cleaned_df.select(
            [
                F.count(F.when(F.col(field).isNull(), field)).alias(f"{field}_nulls")
                for field in required_fields
            ]
        )
        .collect()[0]
        .asDict()
    )
    violations = {key: value for key, value in null_counts.items() if value > 0}
    if violations:
        raise ValueError(
            f"Null values found in crucial fields after casting: {violations}"
        )


def upsert_coins(
    spark: SparkSession,
    cleaned_df,
    catalog_name: str = "silver",
) -> None:
    """Upsert coin metadata into the target Iceberg table."""
    coins_df = cleaned_df.select("id", "symbol", "name", "rank", "supply", "max_supply")
    coins_df.createOrReplaceTempView("coins_today")
    spark.sql(
        f"""
        MERGE INTO {catalog_name}.crypto.coins AS target
        USING coins_today AS source ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    logger.info("Upserted %d rows into %s.crypto.coins", coins_df.count(), catalog_name)


def overwrite_price_snapshots(
    spark: SparkSession,
    cleaned_df,
    logical_date: date,
    catalog_name: str = "silver",
) -> None:
    """Overwrite the target date partition in price snapshots."""
    snapshots_df = cleaned_df.select(
        F.col("id").alias("coin_id"),
        F.lit(logical_date).cast("date").alias("snapshot_date"),
        "price_usd",
        "market_cap_usd",
        "volume_usd_24hr",
        "change_percent_24hr",
        "vwap_24hr",
    )
    snapshots_df.writeTo(f"{catalog_name}.crypto.price_snapshots").overwritePartitions()
    logger.info(
        "Wrote %d rows into %s.crypto.price_snapshots (date=%s)",
        snapshots_df.count(),
        catalog_name,
        logical_date,
    )


def transform(
    spark: SparkSession,
    logical_date: date,
    bronze_path: str | None = None,
    catalog_name: str = "silver",
) -> None:
    """Read Bronze, clean it, and write the Silver tables."""
    raw_df = read_bronze(spark, logical_date, bronze_path=bronze_path)
    cleaned_df = clean_and_cast_bronze(raw_df)
    validate_crucial_fields(cleaned_df)
    _create_tables_if_not_exist(spark, catalog_name=catalog_name)
    upsert_coins(spark, cleaned_df, catalog_name=catalog_name)
    overwrite_price_snapshots(
        spark,
        cleaned_df,
        logical_date,
        catalog_name=catalog_name,
    )


def validate_silver_result(
    spark: SparkSession,
    logical_date: date,
    catalog_name: str = "silver",
) -> None:
    """Check that the target Silver tables contain rows after the transform."""
    coins_count = spark.table(f"{catalog_name}.crypto.coins").count()
    snapshots_count = (
        spark.table(f"{catalog_name}.crypto.price_snapshots")
        .filter(F.col("snapshot_date") == F.lit(logical_date))
        .count()
    )

    if coins_count == 0:
        raise ValueError(f"{catalog_name}.crypto.coins is empty after transformation")
    if snapshots_count == 0:
        raise ValueError(
            f"{catalog_name}.crypto.price_snapshots has no rows for date {logical_date} after transformation"
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
        validate_silver_result(spark, logical_date)
        logger.info("Silver transform complete for %s", logical_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
