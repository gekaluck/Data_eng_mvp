"""Unit tests for CoinCap Pydantic schemas."""

import pytest
from pydantic import ValidationError
from schemas.coincap import CoinCapAsset, CoinCapAssetsResponse


# --- Sample data matching the CoinCap API shape ---

VALID_ASSET = {
    "id": "bitcoin",
    "rank": "1",
    "symbol": "BTC",
    "name": "Bitcoin",
    "supply": "19500000.0000000000000000",
    "maxSupply": "21000000.0000000000000000",
    "marketCapUsd": "1234567890.12",
    "volumeUsd24Hr": "9876543210.99",
    "priceUsd": "63000.1234567890",
    "changePercent24Hr": "2.3456789012345678",
    "vwap24Hr": "62500.0000000000",
    "explorer": "https://blockchain.info/",
}


class TestCoinCapAsset:
    """Tests for the CoinCapAsset model."""

    def test_valid_asset(self):
        asset = CoinCapAsset.model_validate(VALID_ASSET)
        assert asset.id == "bitcoin"
        assert asset.symbol == "BTC"
        assert asset.rank == "1"

    def test_nullable_fields_with_none(self):
        """maxSupply, vwap24Hr, explorer can be null (e.g., ETH pre-merge)."""
        data = {**VALID_ASSET, "maxSupply": None, "vwap24Hr": None, "explorer": None}
        asset = CoinCapAsset.model_validate(data)
        assert asset.maxSupply is None
        assert asset.vwap24Hr is None
        assert asset.explorer is None

    def test_nullable_fields_missing(self):
        """Optional fields can be omitted entirely."""
        data = {k: v for k, v in VALID_ASSET.items() if k not in ("maxSupply", "vwap24Hr", "explorer")}
        asset = CoinCapAsset.model_validate(data)
        assert asset.maxSupply is None

    def test_missing_required_field_raises(self):
        """Removing a required field should fail validation."""
        data = {k: v for k, v in VALID_ASSET.items() if k != "priceUsd"}
        with pytest.raises(ValidationError):
            CoinCapAsset.model_validate(data)

    def test_all_numeric_fields_are_strings(self):
        """Bronze stores raw strings — no numeric conversion."""
        asset = CoinCapAsset.model_validate(VALID_ASSET)
        assert isinstance(asset.priceUsd, str)
        assert isinstance(asset.supply, str)
        assert isinstance(asset.marketCapUsd, str)


class TestCoinCapAssetsResponse:
    """Tests for the response wrapper."""

    def test_valid_response(self):
        response = CoinCapAssetsResponse.model_validate({
            "data": [VALID_ASSET],
            "timestamp": 1700000000000,
        })
        assert len(response.data) == 1
        assert response.timestamp == 1700000000000

    def test_empty_data_list(self):
        """API could theoretically return an empty list."""
        response = CoinCapAssetsResponse.model_validate({
            "data": [],
            "timestamp": 1700000000000,
        })
        assert len(response.data) == 0

    def test_missing_timestamp_raises(self):
        with pytest.raises(ValidationError):
            CoinCapAssetsResponse.model_validate({"data": [VALID_ASSET]})

    def test_multiple_assets(self):
        eth = {**VALID_ASSET, "id": "ethereum", "symbol": "ETH", "name": "Ethereum"}
        response = CoinCapAssetsResponse.model_validate({
            "data": [VALID_ASSET, eth],
            "timestamp": 1700000000000,
        })
        assert len(response.data) == 2
        assert response.data[1].symbol == "ETH"
