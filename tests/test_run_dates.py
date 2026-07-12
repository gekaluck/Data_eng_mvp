"""Tests for target date, backfill window, and Bronze key helpers."""

from datetime import date, timedelta

import pytest
from pendulum import datetime

from utils.run_dates import (
    MAX_GOLD_RANGE_DAYS,
    bronze_assets_key,
    bronze_history_backfill_key,
    date_to_unix_ms_end,
    date_to_unix_ms_start,
    resolve_backfill_window,
    resolve_target_date,
    resolve_target_dates,
)


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


def test_resolve_target_dates_falls_back_to_single_date():
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun({"target_date": "2026-03-15"}),
    }
    assert resolve_target_dates(context) == [date(2026, 3, 15)]


def test_resolve_target_dates_expands_inclusive_range():
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun({"start_date": "2026-07-04", "end_date": "2026-07-07"}),
    }
    assert resolve_target_dates(context) == [
        date(2026, 7, 4),
        date(2026, 7, 5),
        date(2026, 7, 6),
        date(2026, 7, 7),
    ]


def test_resolve_target_dates_single_day_range():
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun({"start_date": "2026-07-09", "end_date": "2026-07-09"}),
    }
    assert resolve_target_dates(context) == [date(2026, 7, 9)]


def test_resolve_target_dates_requires_both_range_bounds():
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun({"start_date": "2026-07-04"}),
    }
    with pytest.raises(ValueError, match="both start_date and end_date"):
        resolve_target_dates(context)


def test_resolve_target_dates_rejects_reversed_range():
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun({"start_date": "2026-07-07", "end_date": "2026-07-04"}),
    }
    with pytest.raises(ValueError, match="on or after"):
        resolve_target_dates(context)


def test_resolve_target_dates_enforces_safety_cap():
    start = date(2026, 1, 1)
    end = start + timedelta(days=MAX_GOLD_RANGE_DAYS)  # one day past the cap
    context = {
        "logical_date": datetime(2026, 3, 17, 12, 30, 0),
        "dag_run": DummyDagRun(
            {"start_date": start.isoformat(), "end_date": end.isoformat()}
        ),
    }
    with pytest.raises(ValueError, match="safety cap"):
        resolve_target_dates(context)


def test_bronze_assets_key_builds_expected_partition_path():
    assert (
        bronze_assets_key(date(2026, 3, 15))
        == "crypto/assets/year=2026/month=03/day=15/assets.parquet"
    )


def test_resolve_backfill_window_returns_inclusive_pre_anchor_range():
    assert resolve_backfill_window(date(2026, 3, 15), 60) == (
        date(2026, 1, 14),
        date(2026, 3, 14),
    )


def test_resolve_backfill_window_rejects_non_positive_day_counts():
    with pytest.raises(ValueError, match="backfill_days"):
        resolve_backfill_window(date(2026, 3, 15), 0)


def test_date_to_unix_ms_helpers_cover_full_day():
    start_ms = date_to_unix_ms_start(date(2026, 3, 15))
    end_ms = date_to_unix_ms_end(date(2026, 3, 15))

    assert start_ms == 1773532800000
    assert end_ms == 1773619199999


def test_bronze_history_backfill_key_builds_expected_partition_path():
    assert (
        bronze_history_backfill_key(date(2026, 3, 15), 60, "asset_history")
        == "crypto/history_backfill/anchor_date=2026-03-15/window_days=60/asset_history.parquet"
    )
