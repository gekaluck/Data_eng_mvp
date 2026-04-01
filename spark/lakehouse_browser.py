"""Helpers for browsing local Iceberg catalogs from JupyterLab."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from spark.iceberg_runtime import configure_runtime_spark_builder

JUPYTER_SPARK_IVY_DIR = "/home/airflow/.ivy2"


def build_browser_spark_session(
    minio_endpoint: str,
    minio_user: str,
    minio_password: str,
) -> SparkSession:
    """Create a Spark session with both Silver and Gold Iceberg catalogs."""
    builder = (
        SparkSession.builder.appName("lakehouse_browser")
        .master("local[2]")
    )
    return configure_runtime_spark_builder(
        builder,
        minio_endpoint=minio_endpoint,
        minio_user=minio_user,
        minio_password=minio_password,
        catalog_names=["silver", "gold"],
        ivy_dir=JUPYTER_SPARK_IVY_DIR,
    ).getOrCreate()


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


def query_sql(spark: SparkSession, sql_text: str):
    """Run arbitrary SQL and return the result as a Pandas DataFrame."""
    return spark.sql(sql_text).toPandas()


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


def count_rows(
    spark: SparkSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
):
    """Return the row count for a table as a one-row Pandas DataFrame."""
    return spark.sql(
        f"""
        SELECT count(*) AS row_count
        FROM {catalog_name}.{namespace}.{table_name}
        """
    ).toPandas()


def date_range(
    spark: SparkSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
    date_col: str = "snapshot_date",
):
    """Return the min/max date and row count for a date-based table."""
    return spark.sql(
        f"""
        SELECT
            min({date_col}) AS min_date,
            max({date_col}) AS max_date,
            count(*) AS row_count
        FROM {catalog_name}.{namespace}.{table_name}
        """
    ).toPandas()


def null_summary(
    spark: SparkSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
    columns: list[str] | None = None,
):
    """Return null counts for the selected columns in a table."""
    df = spark.table(f"{catalog_name}.{namespace}.{table_name}")
    target_columns = columns or df.columns
    expressions = [
        f"sum(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) AS {column_name}_nulls"
        for column_name in target_columns
    ]
    return spark.sql(
        f"""
        SELECT
            {", ".join(expressions)}
        FROM {catalog_name}.{namespace}.{table_name}
        """
    ).toPandas()


def distinct_values(
    spark: SparkSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
    column_name: str,
    limit: int = 50,
):
    """Return distinct values for a column, sorted ascending."""
    return spark.sql(
        f"""
        SELECT DISTINCT {column_name}
        FROM {catalog_name}.{namespace}.{table_name}
        ORDER BY {column_name}
        LIMIT {int(limit)}
        """
    ).toPandas()
