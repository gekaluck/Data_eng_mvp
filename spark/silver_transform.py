"""Silver layer PySpark job — transform CoinCap bronze data to Iceberg tables.

Reads the bronze Parquet for a given date from MinIO, type-casts and renames
columns (camelCase → snake_case), then writes to two Iceberg tables:

  silver.crypto.coins
      Grain: one row per coin (slowly-changing metadata).
      Uses MERGE INTO to upsert — existing coins are updated, new coins inserted.
      This avoids duplicate rows even when the job reruns.

  silver.crypto.price_snapshots
      Grain: one row per (coin_id, snapshot_date).
      Partitioned by snapshot_date. Uses overwritePartitions() so reruns for
      the same date are idempotent — they overwrite only that partition.

Why two tables?
    The raw API response mixes slow-changing metadata (name, symbol, rank) with
    fast-changing facts (price, volume). Keeping them in separate tables avoids
    repeating metadata on every row and makes each table's purpose clear.
    This is the core of entity-centric (3NF) modeling.

Usage:
    python silver_transform.py <YYYY-MM-DD>

Environment variables:
    MINIO_ROOT_USER      MinIO access key (same as AWS_ACCESS_KEY_ID in S3A)
    MINIO_ROOT_PASSWORD  MinIO secret key (same as AWS_SECRET_ACCESS_KEY in S3A)
    MINIO_ENDPOINT       MinIO URL (default: http://minio:9000)
"""

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

# ---------------------------------------------------------------------------
# Iceberg + S3A JARs
# ---------------------------------------------------------------------------
# These are downloaded from Maven Central on first run and cached in
# /opt/spark-ivy (a Docker volume). Subsequent runs reuse the cache.
#
# Version matrix:
#   PySpark 3.5.x bundles Hadoop 3.3.4 internally.
#   hadoop-aws must match that version to avoid classpath conflicts.
#   iceberg-spark-runtime-3.5_2.12 targets Spark 3.5 / Scala 2.12.
SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.367",
])


def build_spark_session(
    minio_endpoint: str, minio_user: str, minio_password: str
) -> SparkSession:
    """Create a local-mode SparkSession with Iceberg + MinIO/S3A configured."""
    return (
        SparkSession.builder
        .appName("silver_coincap")
        .master("local[2]")   # 2 cores — plenty for 20 rows of coin data
        .config("spark.jars.packages", SPARK_PACKAGES)
        # Cache directory for downloaded JARs (mapped to a Docker volume).
        # Without this, Spark would re-download ~300MB on every container restart.
        .config(
            "spark.driver.extraJavaOptions",
            "-Divy.cache.dir=/opt/spark-ivy -Divy.home=/opt/spark-ivy",
        )
        # Iceberg SQL extensions — required for CREATE TABLE, MERGE INTO, etc.
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Silver catalog — Hadoop type stores table metadata as JSON files
        # directly in MinIO (no separate catalog database needed).
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver/iceberg")
        # S3A filesystem: point PySpark at MinIO instead of real AWS S3.
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Disable the Spark web UI — unnecessary inside Airflow containers.
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _create_tables_if_not_exist(spark: SparkSession) -> None:
    """Create the silver namespace and tables on first run.

    CREATE TABLE IF NOT EXISTS makes this safe to call on every run.
    The table definitions are the authoritative schema — any mismatch
    between bronze strings and these types is caught during the cast step.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver.crypto")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.crypto.coins (
            id          STRING  NOT NULL COMMENT 'CoinCap identifier (e.g. bitcoin)',
            symbol      STRING          COMMENT 'Trading symbol (e.g. BTC)',
            name        STRING          COMMENT 'Full display name (e.g. Bitcoin)',
            rank        INT             COMMENT 'Market-cap rank at last observation',
            supply      DOUBLE          COMMENT 'Circulating supply',
            max_supply  DOUBLE          COMMENT 'Maximum supply — null means uncapped'
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.crypto.price_snapshots (
            coin_id             STRING  NOT NULL COMMENT 'FK → silver.crypto.coins.id',
            snapshot_date       DATE    NOT NULL COMMENT 'Logical execution date (UTC)',
            price_usd           DOUBLE          COMMENT 'USD price at snapshot time',
            market_cap_usd      DOUBLE          COMMENT 'Total market cap in USD',
            volume_usd_24hr     DOUBLE          COMMENT 'Trading volume over last 24 h',
            change_percent_24hr DOUBLE          COMMENT 'Price change % over last 24 h',
            vwap_24hr           DOUBLE          COMMENT 'Volume-weighted avg price (24 h)'
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
    """)


def transform(spark: SparkSession, logical_date: date) -> None:
    """Core transformation: bronze Parquet → two silver Iceberg tables."""
    bronze_path = (
        f"s3a://bronze/crypto/assets/"
        f"year={logical_date.year}/"
        f"month={logical_date.month:02d}/"
        f"day={logical_date.day:02d}/"
        f"assets.parquet"
    )

    logger.info("Reading bronze from: %s", bronze_path)
    raw_df = spark.read.parquet(bronze_path)
    row_count = raw_df.count()
    logger.info("Read %d rows from bronze", row_count)

    # --- Clean and type-cast ---
    # Bronze stores everything as strings (raw, no conversions).
    # Silver applies the real types and renames camelCase fields to snake_case.
    # Cast failures produce null (Spark's default) — we'll catch that in tests.
    cleaned_df = raw_df.select(
        F.col("id"),
        F.col("symbol"),
        F.col("name"),
        F.col("rank").cast("int"),
        F.col("supply").cast("double"),
        F.col("maxSupply").cast("double").alias("max_supply"),
        F.col("priceUsd").cast("double").alias("price_usd"),
        F.col("marketCapUsd").cast("double").alias("market_cap_usd"),
        F.col("volumeUsd24Hr").cast("double").alias("volume_usd_24hr"),
        F.col("changePercent24Hr").cast("double").alias("change_percent_24hr"),
        F.col("vwap24Hr").cast("double").alias("vwap_24hr"),
    )

    _create_tables_if_not_exist(spark)

    # --- Table 1: silver.crypto.coins (upsert) ---
    # MERGE INTO updates existing rows (rank changes over time) and inserts
    # new coins. This keeps the table accurate without duplicates.
    coins_df = cleaned_df.select(
        "id", "symbol", "name", "rank", "supply", "max_supply"
    )
    coins_df.createOrReplaceTempView("coins_today")

    spark.sql("""
        MERGE INTO silver.crypto.coins AS target
        USING coins_today AS source ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info("Upserted %d rows into silver.crypto.coins", coins_df.count())

    # --- Table 2: silver.crypto.price_snapshots (partition overwrite) ---
    # overwritePartitions() replaces only the partition for snapshot_date,
    # so rerunning for the same date won't create duplicate rows.
    snapshots_df = cleaned_df.select(
        F.col("id").alias("coin_id"),
        F.lit(logical_date).cast("date").alias("snapshot_date"),
        "price_usd",
        "market_cap_usd",
        "volume_usd_24hr",
        "change_percent_24hr",
        "vwap_24hr",
    )

    snapshots_df.writeTo("silver.crypto.price_snapshots").overwritePartitions()
    logger.info(
        "Wrote %d rows into silver.crypto.price_snapshots (date=%s)",
        snapshots_df.count(),
        logical_date,
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
        logger.info("Silver transform complete for %s", logical_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
