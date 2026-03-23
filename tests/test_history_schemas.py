"""Unit tests for CoinCap history backfill schemas."""

import pytest
from pydantic import ValidationError

from schemas.coincap_history import (
    CoinCapAssetHistoryResponse,
    CoinCapAssetMarketCapHistoryResponse,
    CoinCapTotalMarketCapHistoryResponse,
)


def test_asset_history_response_accepts_price_points():
    response = CoinCapAssetHistoryResponse.model_validate(
        {
            "data": [
                {
                    "priceUsd": "65000.12",
                    "volumeUsd24Hr": "1200000000.34",
                    "time": 1710115200000,
                    "date": "2026-03-11T00:00:00.000Z",
                }
            ],
            "timestamp": 1710115200000,
        }
    )

    assert response.data[0].priceUsd == "65000.12"


def test_asset_history_response_rejects_points_without_price():
    with pytest.raises(ValidationError):
        CoinCapAssetHistoryResponse.model_validate(
            {"data": [{"time": 1710115200000}]}
        )


def test_asset_market_cap_history_accepts_multiple_field_variants():
    response = CoinCapAssetMarketCapHistoryResponse.model_validate(
        {"data": [{"marketcapUsd": "123456789.01", "time": 1710115200000}]}
    )

    assert response.data[0].marketCapUsd == "123456789.01"


def test_total_market_cap_history_accepts_total_and_generic_field_names():
    response = CoinCapTotalMarketCapHistoryResponse.model_validate(
        {
            "data": [
                {"total_market_cap_usd": "2200000000000.45", "time": 1710115200000},
                {"marketCapUsd": "2210000000000.45", "time": 1710201600000},
            ]
        }
    )

    assert response.data[0].totalMarketCapUsd == "2200000000000.45"
    assert response.data[1].totalMarketCapUsd == "2210000000000.45"
