"""Shared runtime Iceberg catalog configuration for local Spark jobs."""

from __future__ import annotations

import os
from collections.abc import Iterable


SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
        "org.postgresql:postgresql:42.7.5",
    ]
)

CATALOG_DATABASE_ENV_VARS = {
    "silver": "ICEBERG_SILVER_CATALOG_DB",
    "gold": "ICEBERG_GOLD_CATALOG_DB",
}

CATALOG_WAREHOUSES = {
    "silver": "s3a://silver/iceberg",
    "gold": "s3a://gold/iceberg",
}


def catalog_database(catalog_name: str) -> str:
    """Return the Postgres database used by a runtime Iceberg catalog."""
    env_var = CATALOG_DATABASE_ENV_VARS[catalog_name]
    return os.environ.get(env_var, f"iceberg_{catalog_name}")


def catalog_jdbc_uri(catalog_name: str) -> str:
    """Build the JDBC URI for a runtime Iceberg catalog."""
    host = os.environ.get("ICEBERG_CATALOG_HOST", "postgres")
    port = os.environ.get("ICEBERG_CATALOG_PORT", "5432")
    database = catalog_database(catalog_name)
    return f"jdbc:postgresql://{host}:{port}/{database}"


def configure_runtime_catalog(builder, catalog_name: str):
    """Attach a JDBC-backed Iceberg catalog to a Spark session builder."""
    return (
        builder.config(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog_name}.type", "jdbc")
        .config(f"spark.sql.catalog.{catalog_name}.uri", catalog_jdbc_uri(catalog_name))
        .config(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            CATALOG_WAREHOUSES[catalog_name],
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.jdbc.user",
            os.environ.get("POSTGRES_USER", "airflow"),
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.jdbc.password",
            os.environ.get("POSTGRES_PASSWORD", ""),
        )
        .config(f"spark.sql.catalog.{catalog_name}.jdbc.schema-version", "V1")
    )


def configure_runtime_spark_builder(
    builder,
    minio_endpoint: str,
    minio_user: str,
    minio_password: str,
    catalog_names: Iterable[str],
    ivy_dir: str,
):
    """Apply shared Spark, S3A, and Iceberg runtime configuration."""
    builder = (
        builder.config("spark.jars.packages", SPARK_PACKAGES)
        .config(
            "spark.driver.extraJavaOptions",
            f"-Divy.cache.dir={ivy_dir} -Divy.home={ivy_dir}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.ui.enabled", "false")
    )

    for catalog_name in catalog_names:
        builder = configure_runtime_catalog(builder, catalog_name)

    return builder
