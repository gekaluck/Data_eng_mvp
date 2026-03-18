"""Tests for manual/scheduled target date resolution helpers."""

from datetime import date

import pytest
from pendulum import datetime

from utils.run_dates import bronze_assets_key, resolve_target_date


class DummyDagRun:
    def __init__(self, conf=None):
        self.conf = conf or {}


def test_resolve_target_date_defaults_to_logical_date():
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun(),
    }
    assert resolve_target_date(context) == date(2026, 3, 17)


def test_resolve_target_date_uses_manual_override():
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun({"target_date": "2026-03-15"}),
    }
    assert resolve_target_date(context) == date(2026, 3, 15)


def test_resolve_target_date_rejects_invalid_override():
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun({"target_date": "03/15/2026"}),
    }
    with pytest.raises(ValueError):
        resolve_target_date(context)


def test_bronze_assets_key_builds_expected_partition_path():
    assert (
        bronze_assets_key(date(2026, 3, 15))
        == "crypto/assets/year=2026/month=03/day=15/assets.parquet"
    )
