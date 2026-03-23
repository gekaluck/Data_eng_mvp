"""Spark-based tests for the Silver history backfill transform."""

from datetime import date

import pytest
from pyspark.sql import functions as F

import spark.silver_history_backfill as history_backfill
from spark.silver_transform import _create_tables_if_not_exist, build_local_test_spark_session


CATALOG_NAME = "test_silver_history"
ANCHOR_DATE = date(2026, 3, 15)
WINDOW_START = date(2026, 1, 14)
WINDOW_END = date(2026, 3, 14)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse_dir = tmp_path_factory.mktemp("history_iceberg_warehouse")
    spark = build_local_test_spark_session(
        warehouse_path=warehouse_dir.as_uri(),
        catalog_name=CATALOG_NAME,
    )
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def reset_tables(spark):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.crypto.total_market_cap_history")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.crypto.asset_market_cap_history")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.crypto.price_snapshots")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.crypto.coins")
    spark.sql(f"DROP NAMESPACE IF EXISTS {CATALOG_NAME}.crypto")
    yield


@pytest.fixture
def asset_history_rows():
    return [
        {
            "coin_id": "bitcoin",
            "priceUsd": "63000.10",
            "volumeUsd24Hr": "1200000000.0",
            "time": 1768348800000,
            "date": "2026-01-14T00:00:00.000Z",
        },
        {
            "coin_id": "ethereum",
            "priceUsd": "3400.50",
            "volumeUsd24Hr": "600000000.0",
            "time": 1768348800000,
            "date": "2026-01-14T00:00:00.000Z",
        },
    ]


@pytest.fixture
def asset_market_cap_rows():
    return [
        {
            "coin_id": "bitcoin",
            "marketCapUsd": "1234567890.0",
            "time": 1768348800000,
            "date": "2026-01-14T00:00:00.000Z",
        },
        {
            "coin_id": "ethereum",
            "marketCapUsd": "723456789.0",
            "time": 1768348800000,
            "date": "2026-01-14T00:00:00.000Z",
        },
    ]


@pytest.fixture
def total_market_cap_rows():
    return [
        {
            "totalMarketCapUsd": "2200000000000.0",
            "time": 1768348800000,
            "date": "2026-01-14T00:00:00.000Z",
        }
    ]


def test_clean_asset_history_builds_snapshot_dates(spark, asset_history_rows):
    cleaned_df = history_backfill.clean_asset_history(
        spark.createDataFrame(asset_history_rows)
    )
    rows = {row["coin_id"]: row.asDict() for row in cleaned_df.collect()}

    assert cleaned_df.columns == [
        "coin_id",
        "snapshot_date",
        "price_usd",
        "volume_usd_24hr",
    ]
    assert rows["bitcoin"]["snapshot_date"] == WINDOW_START
    assert rows["ethereum"]["price_usd"] == pytest.approx(3400.5)


def test_transform_history_backfill_merges_into_all_target_tables(
    spark,
    tmp_path,
    monkeypatch,
    asset_history_rows,
    asset_market_cap_rows,
    total_market_cap_rows,
):
    bronze_paths = {}
    for dataset_name, rows in {
        "asset_history": asset_history_rows,
        "asset_market_cap_history": asset_market_cap_rows,
        "total_market_cap_history": total_market_cap_rows,
    }.items():
        dataset_path = tmp_path / dataset_name
        spark.createDataFrame(rows).write.mode("overwrite").parquet(str(dataset_path))
        bronze_paths[dataset_name] = str(dataset_path)

    def fake_read_history_bronze(_spark, _anchor_date, _backfill_days, dataset_name):
        return spark.read.parquet(bronze_paths[dataset_name])

    monkeypatch.setattr(history_backfill, "read_history_bronze", fake_read_history_bronze)

    _create_tables_if_not_exist(spark, catalog_name=CATALOG_NAME)
    spark.sql(
        f"""
        INSERT INTO {CATALOG_NAME}.crypto.coins VALUES
        ('bitcoin', 'BTC', 'Bitcoin', 1, 19500000.0, 21000000.0),
        ('ethereum', 'ETH', 'Ethereum', 2, 120000000.0, NULL)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {CATALOG_NAME}.crypto.price_snapshots VALUES
        ('bitcoin', DATE '2026-03-15', 65000.0, 1250000000.0, 1300000000.0, 1.5, 64800.0)
        """
    )

    history_backfill.transform_history_backfill(
        spark,
        ANCHOR_DATE,
        60,
        catalog_name=CATALOG_NAME,
    )
    history_backfill.validate_history_backfill_result(
        spark,
        WINDOW_START,
        WINDOW_END,
        catalog_name=CATALOG_NAME,
    )

    snapshots = spark.table(f"{CATALOG_NAME}.crypto.price_snapshots")
    snapshot_rows = {
        (row["coin_id"], row["snapshot_date"]): row.asDict()
        for row in snapshots.collect()
    }
    asset_market_caps = {
        (row["coin_id"], row["snapshot_date"]): row.asDict()
        for row in spark.table(f"{CATALOG_NAME}.crypto.asset_market_cap_history").collect()
    }
    total_market_caps = {
        row["snapshot_date"]: row.asDict()
        for row in spark.table(f"{CATALOG_NAME}.crypto.total_market_cap_history").collect()
    }

    assert ("bitcoin", WINDOW_START) in snapshot_rows
    assert ("bitcoin", ANCHOR_DATE) in snapshot_rows
    assert snapshot_rows[("bitcoin", WINDOW_START)]["market_cap_usd"] == pytest.approx(
        1234567890.0
    )
    assert asset_market_caps[("ethereum", WINDOW_START)]["market_cap_usd"] == pytest.approx(
        723456789.0
    )
    assert total_market_caps[WINDOW_START]["total_market_cap_usd"] == pytest.approx(
        2200000000000.0
    )
    assert (
        snapshots.filter(F.col("snapshot_date") == F.lit(ANCHOR_DATE)).count() == 1
    )
