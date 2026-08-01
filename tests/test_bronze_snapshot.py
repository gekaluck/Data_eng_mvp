"""Tests for the shared Bronze snapshot builder and its provenance audit (H5)."""

import io
from datetime import date, datetime, timezone

import pyarrow.parquet as pq
import pytest

from utils.bronze_snapshot import (
    API_TIMESTAMP_COLUMN,
    FETCHED_AT_COLUMN,
    SNAPSHOT_SCHEMA,
    SnapshotProvenance,
    build_snapshot_parquet,
    find_duplicate_fetches,
    find_mislabelled_snapshots,
    read_snapshot_provenance,
    summarize_coverage,
)

FETCHED_AT = datetime(2026, 7, 28, 0, 30, tzinfo=timezone.utc)


def _raw_json(timestamp_ms: int = 1_753_660_200_000, count: int = 2) -> dict:
    """A minimal but complete /assets response, in CoinCap's all-strings shape."""
    return {
        "timestamp": timestamp_ms,
        "data": [
            {
                "id": f"coin-{index}",
                "rank": str(index + 1),
                "symbol": f"C{index}",
                "name": f"Coin {index}",
                "supply": "1000.0",
                "maxSupply": None,
                "marketCapUsd": "5000.0",
                "volumeUsd24Hr": "250.0",
                "priceUsd": "5.0",
                "changePercent24Hr": "1.5",
                "vwap24Hr": "4.9",
                "explorer": None,
            }
            for index in range(count)
        ],
    }


def _read(parquet_bytes: bytes):
    return pq.read_table(io.BytesIO(parquet_bytes))


def test_snapshot_carries_the_provenance_columns():
    table = _read(build_snapshot_parquet(_raw_json(), fetched_at=FETCHED_AT))

    assert table.column(API_TIMESTAMP_COLUMN).to_pylist() == [1_753_660_200_000] * 2
    assert table.column(FETCHED_AT_COLUMN).to_pylist() == [FETCHED_AT] * 2


def test_snapshot_keeps_the_declared_schema():
    """The schema is the compatibility contract between the two writers."""
    table = _read(build_snapshot_parquet(_raw_json(), fetched_at=FETCHED_AT))
    assert table.schema.names == SNAPSHOT_SCHEMA.names
    assert table.schema.types == SNAPSHOT_SCHEMA.types


def test_snapshot_keeps_coincap_values_as_strings():
    table = _read(build_snapshot_parquet(_raw_json(), fetched_at=FETCHED_AT))
    assert table.column("priceUsd").to_pylist() == ["5.0", "5.0"]
    assert table.column("maxSupply").to_pylist() == [None, None]


def test_both_writers_produce_identical_bytes_for_one_response():
    """The DAG and the cloud capture share this builder; same input, same object."""
    first = build_snapshot_parquet(_raw_json(), fetched_at=FETCHED_AT)
    second = build_snapshot_parquet(_raw_json(), fetched_at=FETCHED_AT)
    assert first == second


def test_snapshot_rejects_a_response_missing_the_timestamp():
    raw = _raw_json()
    del raw["timestamp"]
    with pytest.raises(Exception):
        build_snapshot_parquet(raw, fetched_at=FETCHED_AT)


def test_read_snapshot_provenance_round_trips():
    parquet_bytes = build_snapshot_parquet(_raw_json(), fetched_at=FETCHED_AT)
    provenance = read_snapshot_provenance(parquet_bytes, date(2026, 7, 28))

    assert provenance == SnapshotProvenance(
        partition_date=date(2026, 7, 28),
        api_timestamp_ms=1_753_660_200_000,
        fetched_at_utc=FETCHED_AT,
    )
    assert provenance.is_known


def test_read_snapshot_provenance_tolerates_pre_h5_objects():
    """Most of the existing history has no provenance columns; that isn't an error."""
    parquet_bytes = build_snapshot_parquet(_raw_json(), fetched_at=FETCHED_AT)
    table = _read(parquet_bytes).drop([API_TIMESTAMP_COLUMN, FETCHED_AT_COLUMN])
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")

    provenance = read_snapshot_provenance(buffer.getvalue(), date(2026, 7, 28))
    assert provenance.api_timestamp_ms is None
    assert provenance.fetched_at_utc is None
    assert not provenance.is_known


def _provenance(partition_date: date, fetched_at: datetime, api_timestamp_ms: int = 1) -> SnapshotProvenance:
    return SnapshotProvenance(
        partition_date=partition_date,
        api_timestamp_ms=api_timestamp_ms,
        fetched_at_utc=fetched_at,
    )


def test_mislabelled_accepts_a_same_day_cloud_capture():
    """The capture runs at 00:30 UTC on the day it labels — a half-hour lag."""
    healthy = _provenance(date(2026, 7, 28), datetime(2026, 7, 28, 0, 30, tzinfo=timezone.utc))
    assert find_mislabelled_snapshots([healthy]) == []


def test_mislabelled_accepts_a_next_day_scheduled_run():
    """A scheduled local DAG run fires after its logical day ends — ~24h of lag."""
    healthy = _provenance(date(2026, 7, 28), datetime(2026, 7, 29, 0, 5, tzinfo=timezone.utc))
    assert find_mislabelled_snapshots([healthy]) == []


def test_mislabelled_flags_the_i10_signature():
    """I10: catch-up runs on 07-24 wrote live prices under the 07-22 label."""
    findings = find_mislabelled_snapshots(
        [_provenance(date(2026, 7, 22), datetime(2026, 7, 24, 2, 8, tzinfo=timezone.utc))]
    )
    assert [f.partition_date for f in findings] == [date(2026, 7, 22)]
    assert findings[0].lag_hours == pytest.approx(50.13, abs=0.1)


def test_mislabelled_flags_the_i17_signature():
    """I17: the 07-18 and 07-19 objects were both fetched on 07-20."""
    findings = find_mislabelled_snapshots(
        [
            _provenance(date(2026, 7, 18), datetime(2026, 7, 20, 20, 24, tzinfo=timezone.utc)),
            _provenance(date(2026, 7, 19), datetime(2026, 7, 20, 20, 24, tzinfo=timezone.utc)),
        ]
    )
    assert [f.partition_date for f in findings] == [date(2026, 7, 18), date(2026, 7, 19)]


def test_mislabelled_flags_a_fetch_before_its_own_date():
    findings = find_mislabelled_snapshots(
        [_provenance(date(2026, 7, 28), datetime(2026, 7, 27, 23, 0, tzinfo=timezone.utc))]
    )
    assert findings[0].reason == "fetched before its partition date began"


def test_mislabelled_skips_dates_without_provenance():
    assert find_mislabelled_snapshots([SnapshotProvenance(partition_date=date(2026, 4, 7))]) == []


def test_mislabelled_threshold_is_configurable():
    late = _provenance(date(2026, 7, 28), datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc))
    assert find_mislabelled_snapshots([late], max_lag_hours=36) == []
    assert len(find_mislabelled_snapshots([late], max_lag_hours=24)) == 1


def test_duplicate_fetches_groups_dates_sharing_one_response():
    provenances = [
        _provenance(date(2026, 7, 18), FETCHED_AT, api_timestamp_ms=111),
        _provenance(date(2026, 7, 19), FETCHED_AT, api_timestamp_ms=111),
        _provenance(date(2026, 7, 20), FETCHED_AT, api_timestamp_ms=222),
    ]
    assert find_duplicate_fetches(provenances) == [(111, [date(2026, 7, 18), date(2026, 7, 19)])]


def test_duplicate_fetches_is_quiet_on_healthy_history():
    provenances = [
        _provenance(date(2026, 7, 18), FETCHED_AT, api_timestamp_ms=111),
        _provenance(date(2026, 7, 19), FETCHED_AT, api_timestamp_ms=222),
    ]
    assert find_duplicate_fetches(provenances) == []


def test_coverage_summary_counts_unauditable_dates():
    provenances = [
        SnapshotProvenance(partition_date=date(2026, 4, 7)),
        _provenance(date(2026, 7, 28), FETCHED_AT),
    ]
    summary = summarize_coverage(provenances)
    assert "1 of 2" in summary
    assert "2026-07-28" in summary
