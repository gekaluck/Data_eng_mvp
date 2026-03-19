"""Spark-based tests for the Silver transform."""

from datetime import date

import pytest

from spark.silver_transform import (
    build_local_test_spark_session,
    clean_and_cast_bronze,
    transform,
    validate_crucial_fields,
    validate_silver_result,
)


CATALOG_NAME = "test_silver"
TARGET_DATE = date(2026, 3, 15)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse_dir = tmp_path_factory.mktemp("iceberg_warehouse")
    spark = build_local_test_spark_session(
        warehouse_path=warehouse_dir.as_uri(),
        catalog_name=CATALOG_NAME,
    )
    yield spark
    spark.stop()


@pytest.fixture
def bronze_rows():
    return [
        {
            "id": "bitcoin",
            "rank": "1",
            "symbol": "BTC",
            "name": "Bitcoin",
            "supply": "19500000.0",
            "maxSupply": "21000000.0",
            "marketCapUsd": "1234567890.12",
            "volumeUsd24Hr": "9876543210.99",
            "priceUsd": "63000.1234567890",
            "changePercent24Hr": "2.3456789012345678",
            "vwap24Hr": "62500.0",
        },
        {
            "id": "ethereum",
            "rank": "2",
            "symbol": "ETH",
            "name": "Ethereum",
            "supply": "120000000.0",
            "maxSupply": None,
            "marketCapUsd": "723456789.12",
            "volumeUsd24Hr": "1876543210.99",
            "priceUsd": "3400.5",
            "changePercent24Hr": "-1.5",
            "vwap24Hr": "3450.0",
        },
    ]


@pytest.fixture(autouse=True)
def reset_tables(spark):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.crypto.price_snapshots")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.crypto.coins")
    spark.sql(f"DROP NAMESPACE IF EXISTS {CATALOG_NAME}.crypto")
    yield


def test_clean_and_cast_bronze_casts_strings_to_expected_types(spark, bronze_rows):
    raw_df = spark.createDataFrame(bronze_rows)

    cleaned_df = clean_and_cast_bronze(raw_df)
    rows = {row["id"]: row.asDict() for row in cleaned_df.collect()}

    assert cleaned_df.columns == [
        "id",
        "symbol",
        "name",
        "rank",
        "supply",
        "max_supply",
        "price_usd",
        "market_cap_usd",
        "volume_usd_24hr",
        "change_percent_24hr",
        "vwap_24hr",
    ]
    assert rows["bitcoin"]["rank"] == 1
    assert rows["bitcoin"]["price_usd"] == pytest.approx(63000.123456789)
    assert rows["ethereum"]["max_supply"] is None


def test_validate_crucial_fields_rejects_bad_casts(spark, bronze_rows):
    bad_rows = [dict(bronze_rows[0], priceUsd="not-a-number")]
    cleaned_df = clean_and_cast_bronze(spark.createDataFrame(bad_rows))

    with pytest.raises(ValueError, match="price_usd_nulls"):
        validate_crucial_fields(cleaned_df)


def test_transform_writes_both_silver_tables(spark, bronze_rows, tmp_path):
    bronze_path = tmp_path / "bronze.parquet"
    spark.createDataFrame(bronze_rows).write.mode("overwrite").parquet(str(bronze_path))

    transform(
        spark,
        TARGET_DATE,
        bronze_path=str(bronze_path),
        catalog_name=CATALOG_NAME,
    )
    validate_silver_result(spark, TARGET_DATE, catalog_name=CATALOG_NAME)

    coins = {
        row["id"]: row.asDict()
        for row in spark.table(f"{CATALOG_NAME}.crypto.coins").collect()
    }
    snapshots = {
        row["coin_id"]: row.asDict()
        for row in spark.table(f"{CATALOG_NAME}.crypto.price_snapshots").collect()
    }

    assert set(coins) == {"bitcoin", "ethereum"}
    assert coins["bitcoin"]["rank"] == 1
    assert snapshots["bitcoin"]["snapshot_date"] == TARGET_DATE
    assert snapshots["ethereum"]["price_usd"] == pytest.approx(3400.5)
