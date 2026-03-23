"""Helpers for browsing local Iceberg catalogs from JupyterLab."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from spark.silver_transform import SPARK_PACKAGES

JUPYTER_SPARK_IVY_DIR = "/home/airflow/.ivy2"


def build_browser_spark_session(
    minio_endpoint: str,
    minio_user: str,
    minio_password: str,
) -> SparkSession:
    """Create a Spark session with both Silver and Gold Iceberg catalogs."""
    return (
        SparkSession.builder
        .appName("lakehouse_browser")
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


def connect_browser_spark() -> SparkSession:
    """Create a Spark session from the current container environment."""
    return build_browser_spark_session(
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        minio_user=os.environ["MINIO_ROOT_USER"],
        minio_password=os.environ["MINIO_ROOT_PASSWORD"],
    )


def list_namespaces(spark: SparkSession, catalog_name: str = "silver"):
    """Return namespaces for a catalog as a Pandas DataFrame."""
    return spark.sql(f"SHOW NAMESPACES IN {catalog_name}").toPandas()


def list_tables(
    spark: SparkSession,
    catalog_name: str = "silver",
    namespace: str = "crypto",
):
    """Return tables in a namespace as a Pandas DataFrame."""
    return spark.sql(f"SHOW TABLES IN {catalog_name}.{namespace}").toPandas()


def preview_table(
    spark: SparkSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
    limit: int = 20,
):
    """Return a limited preview of a table as a Pandas DataFrame."""
    query = (
        f"SELECT * FROM {catalog_name}.{namespace}.{table_name} "
        f"LIMIT {int(limit)}"
    )
    return spark.sql(query).toPandas()


def describe_table(
    spark: SparkSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
):
    """Return a table schema description as a Pandas DataFrame."""
    return spark.sql(
        f"DESCRIBE {catalog_name}.{namespace}.{table_name}"
    ).toPandas()
