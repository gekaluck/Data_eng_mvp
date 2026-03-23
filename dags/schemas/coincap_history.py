"""Pydantic models for CoinCap history endpoints used by the backfill pipeline."""

from typing import Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


class CoinCapTimedPoint(BaseModel):
    """Base history point shared across CoinCap history-style endpoints."""

    model_config = ConfigDict(extra="allow")

    time: int
    date: Optional[str] = None


class CoinCapAssetHistoryPoint(CoinCapTimedPoint):
    """A single point from the asset history endpoint."""

    priceUsd: str | float | int | None = None
    volumeUsd24Hr: str | float | int | None = None

    @model_validator(mode="after")
    def validate_price_present(self):
        if self.priceUsd is None:
            raise ValueError("Asset history point must include priceUsd")
        return self


class CoinCapAssetMarketCapHistoryPoint(CoinCapTimedPoint):
    """A single point from the asset market-cap history endpoint."""

    marketCapUsd: str | float | int = Field(
        validation_alias=AliasChoices(
            "marketCapUsd",
            "marketcapUsd",
            "marketCap",
            "marketcap",
        ),
        serialization_alias="marketCapUsd",
    )


class CoinCapTotalMarketCapHistoryPoint(CoinCapTimedPoint):
    """A single point from the total market-cap history endpoint."""

    totalMarketCapUsd: str | float | int = Field(
        validation_alias=AliasChoices(
            "totalMarketCapUsd",
            "total_market_cap_usd",
            "marketCapUsd",
            "marketcapUsd",
            "marketCap",
            "marketcap",
        ),
        serialization_alias="totalMarketCapUsd",
    )


class CoinCapAssetHistoryResponse(BaseModel):
    """Wrapper for the asset history response."""

    data: list[CoinCapAssetHistoryPoint]
    timestamp: Optional[int] = None


class CoinCapAssetMarketCapHistoryResponse(BaseModel):
    """Wrapper for the asset market-cap history response."""

    data: list[CoinCapAssetMarketCapHistoryPoint]
    timestamp: Optional[int] = None


class CoinCapTotalMarketCapHistoryResponse(BaseModel):
    """Wrapper for the total market-cap history response."""

    data: list[CoinCapTotalMarketCapHistoryPoint]
    timestamp: Optional[int] = None
