"""Pydantic V2 models for CoinCap API responses.

These models validate the raw JSON from CoinCap before we write to Bronze.
All numeric fields stay as strings — Bronze stores raw data as-is.
Type conversions happen in the Silver layer.
"""

from typing import Optional
from pydantic import BaseModel


class CoinCapAsset(BaseModel):
    """A single asset from the CoinCap /v2/assets endpoint."""

    id: str
    rank: str
    symbol: str
    name: str
    supply: str
    maxSupply: Optional[str] = None  # null for some coins (e.g., ETH pre-merge)
    marketCapUsd: str
    volumeUsd24Hr: str
    priceUsd: str
    changePercent24Hr: str
    vwap24Hr: Optional[str] = None  # can be null for low-volume assets
    explorer: Optional[str] = None  # some coins lack an explorer URL


class CoinCapAssetsResponse(BaseModel):
    """Wrapper for the full /v2/assets API response."""

    data: list[CoinCapAsset]
    timestamp: int
