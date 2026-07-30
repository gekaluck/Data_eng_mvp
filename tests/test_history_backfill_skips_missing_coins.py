"""A coin with no CoinCap history must be skipped, not fatal.

`gamecredits` sits in Silver's coin universe but CoinCap returns 404 for its history
forever. Before this, that one coin aborted the whole backfill *after* it had already
spent credits on every coin ahead of it in the list — and the Airflow retry then spent
them again on the way to the same 404.
"""

import pytest
import requests

from bronze_coincap_history_backfill import CoinCapHistoryNotFound, _fetch_json


class _FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text
        self.headers: dict[str, str] = {}

    def raise_for_status(self):
        raise requests.exceptions.HTTPError(
            f"{self.status_code} Client Error", response=self
        )

    def json(self):  # pragma: no cover - never reached for error responses
        return {}


def test_a_404_raises_the_skippable_error_type(monkeypatch):
    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(404, "Asset history not found."),
    )

    with pytest.raises(CoinCapHistoryNotFound):
        _fetch_json("https://example.invalid/assets/gamecredits/history", {}, {})


def test_other_http_errors_stay_fatal(monkeypatch):
    """Only 404 means 'this coin has no history'. A 500 is a real failure."""
    monkeypatch.setattr(
        requests, "get", lambda *args, **kwargs: _FakeResponse(500, "boom")
    )

    with pytest.raises(RuntimeError) as excinfo:
        _fetch_json("https://example.invalid/assets/bitcoin/history", {}, {})

    assert not isinstance(excinfo.value, CoinCapHistoryNotFound)
