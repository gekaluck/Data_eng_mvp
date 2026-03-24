"""Spark-based tests for the Gold transform."""

from datetime import date

import pytest

from spark.gold_transform import (
    build_local_test_spark_session,
    transform,
    validate_gold_result,
)


SILVER_CATALOG_NAME = "test_silver_gold"
GOLD_CATALOG_NAME = "test_gold"
TARGET_DATE = date(2026, 3, 20)
PREVIOUS_DATE = date(2026, 3, 19)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    silver_warehouse_dir = tmp_path_factory.mktemp("silver_warehouse")
    gold_warehouse_dir = tmp_path_factory.mktemp("gold_warehouse")
    spark = build_local_test_spark_session(
        silver_warehouse_path=silver_warehouse_dir.as_uri(),
        gold_warehouse_path=gold_warehouse_dir.as_uri(),
        silver_catalog_name=SILVER_CATALOG_NAME,
        gold_catalog_name=GOLD_CATALOG_NAME,
    )
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def reset_tables(spark):
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_CATALOG_NAME}.crypto.weekly_rolling_average")
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_CATALOG_NAME}.crypto.market_cap_rank_change")
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_CATALOG_NAME}.crypto.daily_snapshot")
    spark.sql(f"DROP NAMESPACE IF EXISTS {GOLD_CATALOG_NAME}.crypto")
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_CATALOG_NAME}.crypto.price_snapshots")
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_CATALOG_NAME}.crypto.coins")
    spark.sql(f"DROP NAMESPACE IF EXISTS {SILVER_CATALOG_NAME}.crypto")
    yield


def _create_silver_seed_tables(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_CATALOG_NAME}.crypto")
    spark.sql(
        f"""
        CREATE TABLE {SILVER_CATALOG_NAME}.crypto.coins (
            id STRING NOT NULL,
            symbol STRING,
            name STRING,
            rank INT,
            supply DOUBLE,
            max_supply DOUBLE
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE {SILVER_CATALOG_NAME}.crypto.price_snapshots (
            coin_id STRING NOT NULL,
            snapshot_date DATE NOT NULL,
            price_usd DOUBLE,
            market_cap_usd DOUBLE,
            volume_usd_24hr DOUBLE,
            change_percent_24hr DOUBLE,
            vwap_24hr DOUBLE
        ) USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def test_transform_builds_ranked_daily_snapshot_for_logical_date(spark):
    _create_silver_seed_tables(spark)

    spark.createDataFrame(
        [
            {
                "id": "bitcoin",
                "symbol": "BTC",
                "name": "Bitcoin",
                "rank": 1,
                "supply": 19500000.0,
                "max_supply": 21000000.0,
            },
            {
                "id": "ethereum",
                "symbol": "ETH",
                "name": "Ethereum",
                "rank": 2,
                "supply": 120000000.0,
                "max_supply": None,
            },
            {
                "id": "dogecoin",
                "symbol": "DOGE",
                "name": "Dogecoin",
                "rank": 8,
                "supply": 1000000000.0,
                "max_supply": None,
            },
        ]
    ).writeTo(f"{SILVER_CATALOG_NAME}.crypto.coins").append()

    spark.createDataFrame(
        [
            {
                "coin_id": "bitcoin",
                "snapshot_date": date(2026, 2, 18),
                "price_usd": 90.0,
                "market_cap_usd": 900.0,
                "volume_usd_24hr": 350.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 89.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": date(2026, 2, 18),
                "price_usd": 230.0,
                "market_cap_usd": 2300.0,
                "volume_usd_24hr": 550.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 225.0,
            },
            {
                "coin_id": "bitcoin",
                "snapshot_date": date(2026, 3, 6),
                "price_usd": 95.0,
                "market_cap_usd": 950.0,
                "volume_usd_24hr": 360.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 94.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": date(2026, 3, 6),
                "price_usd": 220.0,
                "market_cap_usd": 2200.0,
                "volume_usd_24hr": 540.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 215.0,
            },
            {
                "coin_id": "bitcoin",
                "snapshot_date": date(2026, 3, 14),
                "price_usd": 101.0,
                "market_cap_usd": 990.0,
                "volume_usd_24hr": 395.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 100.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": date(2026, 3, 14),
                "price_usd": 205.0,
                "market_cap_usd": 2050.0,
                "volume_usd_24hr": 505.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 200.0,
            },
            {
                "coin_id": "bitcoin",
                "snapshot_date": date(2026, 3, 15),
                "price_usd": 102.0,
                "market_cap_usd": 995.0,
                "volume_usd_24hr": 398.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 101.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": date(2026, 3, 15),
                "price_usd": 206.0,
                "market_cap_usd": 2060.0,
                "volume_usd_24hr": 506.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 201.0,
            },
            {
                "coin_id": "bitcoin",
                "snapshot_date": date(2026, 3, 16),
                "price_usd": 103.0,
                "market_cap_usd": 1005.0,
                "volume_usd_24hr": 402.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 102.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": date(2026, 3, 16),
                "price_usd": 207.0,
                "market_cap_usd": 2070.0,
                "volume_usd_24hr": 507.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 202.0,
            },
            {
                "coin_id": "bitcoin",
                "snapshot_date": date(2026, 3, 17),
                "price_usd": 104.0,
                "market_cap_usd": 1010.0,
                "volume_usd_24hr": 405.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 103.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": date(2026, 3, 17),
                "price_usd": 208.0,
                "market_cap_usd": 2080.0,
                "volume_usd_24hr": 508.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 203.0,
            },
            {
                "coin_id": "bitcoin",
                "snapshot_date": date(2026, 3, 18),
                "price_usd": 105.0,
                "market_cap_usd": 1020.0,
                "volume_usd_24hr": 410.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 104.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": date(2026, 3, 18),
                "price_usd": 209.0,
                "market_cap_usd": 2090.0,
                "volume_usd_24hr": 509.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 204.0,
            },
            {
                "coin_id": "bitcoin",
                "snapshot_date": PREVIOUS_DATE,
                "price_usd": 100.0,
                "market_cap_usd": 1000.0,
                "volume_usd_24hr": 400.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 98.0,
            },
            {
                "coin_id": "bitcoin",
                "snapshot_date": TARGET_DATE,
                "price_usd": 120.0,
                "market_cap_usd": 1200.0,
                "volume_usd_24hr": 450.0,
                "change_percent_24hr": 20.0,
                "vwap_24hr": 110.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": PREVIOUS_DATE,
                "price_usd": 200.0,
                "market_cap_usd": 2000.0,
                "volume_usd_24hr": 500.0,
                "change_percent_24hr": 0.0,
                "vwap_24hr": 190.0,
            },
            {
                "coin_id": "ethereum",
                "snapshot_date": TARGET_DATE,
                "price_usd": 210.0,
                "market_cap_usd": 2200.0,
                "volume_usd_24hr": 520.0,
                "change_percent_24hr": 5.0,
                "vwap_24hr": 205.0,
            },
            {
                "coin_id": "dogecoin",
                "snapshot_date": TARGET_DATE,
                "price_usd": 0.12,
                "market_cap_usd": 300.0,
                "volume_usd_24hr": 120.0,
                "change_percent_24hr": 1.0,
                "vwap_24hr": 0.11,
            },
        ]
    ).writeTo(f"{SILVER_CATALOG_NAME}.crypto.price_snapshots").append()

    transform(
        spark,
        TARGET_DATE,
        silver_catalog_name=SILVER_CATALOG_NAME,
        gold_catalog_name=GOLD_CATALOG_NAME,
    )
    validate_gold_result(
        spark,
        TARGET_DATE,
        gold_catalog_name=GOLD_CATALOG_NAME,
    )

    rows = {
        row["coin_id"]: row.asDict()
        for row in spark.table(f"{GOLD_CATALOG_NAME}.crypto.daily_snapshot").collect()
    }
    rank_change_rows = {
        row["coin_id"]: row.asDict()
        for row in spark.table(f"{GOLD_CATALOG_NAME}.crypto.market_cap_rank_change").collect()
    }
    weekly_avg_rows = {
        row["coin_id"]: row.asDict()
        for row in spark.table(f"{GOLD_CATALOG_NAME}.crypto.weekly_rolling_average").collect()
    }

    assert set(rows) == {"bitcoin", "ethereum"}
    assert rows["bitcoin"]["symbol"] == "BTC"
    assert rows["bitcoin"]["coin_rank"] == 1
    assert rows["bitcoin"]["prev_price_usd"] == pytest.approx(100.0)
    assert rows["bitcoin"]["price_change_pct"] == pytest.approx(20.0)
    assert rows["bitcoin"]["price_change_rank"] == 1
    assert rows["ethereum"]["prev_price_usd"] == pytest.approx(200.0)
    assert rows["ethereum"]["price_change_pct"] == pytest.approx(5.0)
    assert rows["ethereum"]["price_change_rank"] == 2

    assert set(rank_change_rows) == {"bitcoin", "ethereum", "dogecoin"}
    assert rank_change_rows["bitcoin"]["mc_rank"] == 2
    assert rank_change_rows["bitcoin"]["mc_rank_diff_14d"] == 0
    assert rank_change_rows["bitcoin"]["mc_rank_diff_30d"] == 0
    assert rank_change_rows["bitcoin"]["price_diff_14d_pct"] == pytest.approx(
        (120.0 - 95.0) / 95.0 * 100.0
    )
    assert rank_change_rows["bitcoin"]["price_diff_30d_pct"] == pytest.approx(
        (120.0 - 90.0) / 90.0 * 100.0
    )
    assert rank_change_rows["ethereum"]["mc_rank"] == 1
    assert rank_change_rows["ethereum"]["mc_rank_diff_14d"] == 0
    assert rank_change_rows["ethereum"]["mc_rank_diff_30d"] == 0
    assert rank_change_rows["dogecoin"]["mc_rank_diff_14d"] is None
    assert rank_change_rows["dogecoin"]["price_diff_30d_pct"] is None

    assert set(weekly_avg_rows) == {"bitcoin", "ethereum", "dogecoin"}
    assert weekly_avg_rows["bitcoin"]["wkly_roll_avg_price"] == pytest.approx(105.0)
    assert weekly_avg_rows["bitcoin"]["wkly_roll_avg_volume"] == pytest.approx(408.5714285714)
    assert weekly_avg_rows["ethereum"]["wkly_roll_avg_price"] == pytest.approx(206.4285714286)
    assert weekly_avg_rows["ethereum"]["wkly_roll_avg_volume"] == pytest.approx(507.8571428571)
    assert weekly_avg_rows["dogecoin"]["wkly_roll_avg_price"] == pytest.approx(0.12)
