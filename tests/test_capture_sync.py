"""Tests for planning the cloud-capture -> Bronze sync."""

from datetime import date

import pytest

from utils.capture_sync import (
    MAX_SYNC_DATES,
    CaptureNotFresh,
    check_capture_freshness,
    dates_from_keys,
    plan_sync,
)


def _key(target_date: date) -> str:
    return (
        f"crypto/assets/year={target_date.year}/month={target_date.month:02d}/"
        f"day={target_date.day:02d}/assets.parquet"
    )


def test_dates_from_keys_parses_the_bronze_layout():
    keys = [_key(date(2026, 7, 27)), _key(date(2026, 7, 28))]
    assert dates_from_keys(keys) == {date(2026, 7, 27), date(2026, 7, 28)}


def test_dates_from_keys_ignores_unrelated_objects():
    """The capture bucket may hold diagnostics or other prefixes; skip them."""
    keys = [
        _key(date(2026, 7, 28)),
        "crypto/assets/_diag.txt",
        "some/other/prefix/file.parquet",
        "crypto/assets/year=nope/month=07/day=28/assets.parquet",
    ]
    assert dates_from_keys(keys) == {date(2026, 7, 28)}


def test_dates_from_keys_handles_no_keys():
    """list_keys returns None for an empty bucket."""
    assert dates_from_keys(None) == set()
    assert dates_from_keys([]) == set()


def test_plan_sync_returns_only_missing_dates_sorted():
    captured = {date(2026, 7, 26), date(2026, 7, 27), date(2026, 7, 28)}
    local = {date(2026, 7, 26)}
    assert plan_sync(captured, local) == [date(2026, 7, 27), date(2026, 7, 28)]


def test_plan_sync_is_idempotent_when_bronze_is_current():
    captured = {date(2026, 7, 27), date(2026, 7, 28)}
    assert plan_sync(captured, captured) == []


def test_plan_sync_overwrite_recopies_existing_dates():
    captured = {date(2026, 7, 27), date(2026, 7, 28)}
    assert plan_sync(captured, captured, overwrite=True) == [
        date(2026, 7, 27),
        date(2026, 7, 28),
    ]


def test_plan_sync_respects_window_bounds():
    captured = {date(2026, 7, d) for d in (25, 26, 27, 28)}
    assert plan_sync(captured, set(), start_date=date(2026, 7, 26)) == [
        date(2026, 7, 26),
        date(2026, 7, 27),
        date(2026, 7, 28),
    ]
    assert plan_sync(captured, set(), end_date=date(2026, 7, 26)) == [
        date(2026, 7, 25),
        date(2026, 7, 26),
    ]
    assert plan_sync(
        captured, set(), start_date=date(2026, 7, 26), end_date=date(2026, 7, 27)
    ) == [date(2026, 7, 26), date(2026, 7, 27)]


def test_plan_sync_ignores_local_dates_the_bucket_lacks():
    """Bronze may hold locally-fetched days that were never captured; leave them alone."""
    captured = {date(2026, 7, 28)}
    local = {date(2026, 1, 1), date(2026, 1, 2)}
    assert plan_sync(captured, local) == [date(2026, 7, 28)]


def test_plan_sync_caps_runaway_windows():
    captured = {date.fromordinal(date(2026, 1, 1).toordinal() + i) for i in range(MAX_SYNC_DATES + 1)}
    with pytest.raises(ValueError, match="above the"):
        plan_sync(captured, set())


TODAY = date(2026, 7, 30)


def test_freshness_passes_when_today_was_captured():
    check_capture_freshness({date(2026, 7, 29), TODAY}, today=TODAY)


def test_freshness_tolerates_one_missed_day():
    """A single late or missed capture is normal; two days of silence is not."""
    check_capture_freshness({date(2026, 7, 28)}, today=TODAY, max_age_days=2)


def test_freshness_fails_once_the_newest_date_is_too_old():
    with pytest.raises(CaptureNotFresh, match="stale"):
        check_capture_freshness({date(2026, 7, 27)}, today=TODAY, max_age_days=2)


def test_freshness_distinguishes_an_empty_bucket_from_a_stale_one():
    """Different causes, different fixes: wrong bucket vs a capture that died."""
    with pytest.raises(CaptureNotFresh, match="no snapshots at all"):
        check_capture_freshness(set(), today=TODAY)


def test_freshness_names_the_newest_date_it_found():
    """The message has to say how far behind we are, not just that we are behind."""
    with pytest.raises(CaptureNotFresh, match="2026-07-20"):
        check_capture_freshness({date(2026, 7, 20)}, today=TODAY, max_age_days=2)


def test_freshness_check_can_be_disabled():
    """The escape hatch for deliberately re-syncing an old date."""
    check_capture_freshness({date(2026, 1, 1)}, today=TODAY, max_age_days=0)
