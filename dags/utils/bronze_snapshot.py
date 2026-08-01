"""Build the Bronze `/assets` snapshot Parquet, and audit where it came from.

Two writers produce Bronze snapshots — the local DAG (`dags/bronze_coincap.py`) and
the cloud capture (`scripts/capture_daily_snapshot.py`) — and the sync between them
is a plain object copy, so their output has to have the same shape (D026/D027). They
both call `build_snapshot_parquet` here rather than each building a table of their
own, which is what keeps that true.

**Provenance columns (H5).** Until now Bronze stored only the asset rows, so an
object carried no record of *when* it was fetched. That is why I10 and I17 — live
prices written under a past date's label — had to be inferred from a price
coincidence instead of detected. Every snapshot now also carries:

- `api_timestamp_ms` — CoinCap's own response timestamp, validated all along by
  `CoinCapAssetsResponse.timestamp` and previously discarded.
- `fetched_at_utc` — our wall clock at fetch time.

Both are constant across the rows of one snapshot; a per-file scalar has no home in
a flat Parquet table, and a repeated small column costs nothing after compression.

They are snake_case on purpose: every other Bronze column mirrors CoinCap's own
camelCase field name, so the case difference marks the two columns we added.

The audit helpers below are pure functions over provenance records, so the two
incident signatures they encode are unit-testable without a bucket.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from schemas.coincap import CoinCapAssetsResponse

logger = logging.getLogger(__name__)

# Columns this module adds on top of the raw CoinCap fields.
API_TIMESTAMP_COLUMN = "api_timestamp_ms"
FETCHED_AT_COLUMN = "fetched_at_utc"

# Declared rather than inferred: an explicit schema is the compatibility contract
# between the two writers. Field order matches `CoinCapAsset`'s declaration order,
# with the provenance columns appended. All CoinCap fields stay strings — Bronze
# stores raw values and Silver does the casting.
SNAPSHOT_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("rank", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("name", pa.string()),
        pa.field("supply", pa.string()),
        pa.field("maxSupply", pa.string()),
        pa.field("marketCapUsd", pa.string()),
        pa.field("volumeUsd24Hr", pa.string()),
        pa.field("priceUsd", pa.string()),
        pa.field("changePercent24Hr", pa.string()),
        pa.field("vwap24Hr", pa.string()),
        pa.field("explorer", pa.string()),
        pa.field(API_TIMESTAMP_COLUMN, pa.int64()),
        pa.field(FETCHED_AT_COLUMN, pa.timestamp("us", tz="UTC")),
    ]
)

# How long after the start of its partition date a snapshot may legitimately be
# fetched. The cloud capture runs at 00:30 UTC on the day it labels (~0.5h), and a
# scheduled local DAG run fires just after its logical day ends (~24h). 36h leaves
# room for both plus cron drift, while still catching both known incidents: I10's
# 07-22 object was fetched ~50h late and I17's 07-18 object ~68h late.
DEFAULT_MAX_FETCH_LAG_HOURS = 36


def build_snapshot_parquet(raw_json: dict, fetched_at: datetime | None = None) -> bytes:
    """Validate a CoinCap `/assets` response and serialize it as Bronze Parquet.

    `fetched_at` defaults to now (UTC); pass it explicitly only in tests or when a
    caller already recorded the moment of the request.
    """
    validated = CoinCapAssetsResponse.model_validate(raw_json)
    logger.info("Validated %d assets", len(validated.data))

    fetched_at = fetched_at or datetime.now(timezone.utc)
    records = [
        {
            **asset.model_dump(),
            API_TIMESTAMP_COLUMN: validated.timestamp,
            FETCHED_AT_COLUMN: fetched_at,
        }
        for asset in validated.data
    ]
    table = pa.Table.from_pylist(records, schema=SNAPSHOT_SCHEMA)

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    parquet_bytes = buffer.getvalue()
    logger.info(
        "Built %d parquet bytes (api_timestamp_ms=%d, fetched_at_utc=%s)",
        len(parquet_bytes),
        validated.timestamp,
        fetched_at.isoformat(),
    )
    return parquet_bytes


@dataclass(frozen=True)
class SnapshotProvenance:
    """When one Bronze partition's snapshot was actually fetched.

    `api_timestamp_ms` and `fetched_at_utc` are None for objects written before H5
    landed — most of the existing history. Those dates can't be audited; the audit
    reports them as unknown rather than as clean.
    """

    partition_date: date
    api_timestamp_ms: int | None = None
    fetched_at_utc: datetime | None = None

    @property
    def is_known(self) -> bool:
        return self.fetched_at_utc is not None or self.api_timestamp_ms is not None


@dataclass(frozen=True)
class MislabelledSnapshot:
    """A snapshot whose fetch time is too far from the date it claims to be."""

    partition_date: date
    fetched_at_utc: datetime
    lag_hours: float
    reason: str


def read_snapshot_provenance(parquet_bytes: bytes, partition_date: date) -> SnapshotProvenance:
    """Read the provenance columns out of one Bronze snapshot.

    Reads only those two columns, and tolerates their absence so the audit can run
    across the whole history including pre-H5 objects.
    """
    table = pq.read_table(io.BytesIO(parquet_bytes))
    columns = set(table.column_names)

    api_timestamp_ms = None
    fetched_at_utc = None
    if API_TIMESTAMP_COLUMN in columns and table.num_rows:
        api_timestamp_ms = table.column(API_TIMESTAMP_COLUMN)[0].as_py()
    if FETCHED_AT_COLUMN in columns and table.num_rows:
        fetched_at_utc = table.column(FETCHED_AT_COLUMN)[0].as_py()

    return SnapshotProvenance(
        partition_date=partition_date,
        api_timestamp_ms=api_timestamp_ms,
        fetched_at_utc=fetched_at_utc,
    )


def find_mislabelled_snapshots(
    provenances: list[SnapshotProvenance],
    max_lag_hours: int = DEFAULT_MAX_FETCH_LAG_HOURS,
) -> list[MislabelledSnapshot]:
    """Flag snapshots fetched outside the window their partition date allows.

    This is the direct form of the I10/I17 signature. A live `/assets` response
    stored under a past date's label shows up as a fetch time hours or days after
    that day started; a fetch time *before* the day started can't be that day's data
    at all. Previously the only way to see this was to notice that two dates shared
    a price to the last decimal.
    """
    findings: list[MislabelledSnapshot] = []

    for provenance in sorted(provenances, key=lambda p: p.partition_date):
        if provenance.fetched_at_utc is None:
            continue

        day_start = datetime.combine(provenance.partition_date, time.min, tzinfo=timezone.utc)
        lag_hours = (provenance.fetched_at_utc - day_start).total_seconds() / 3600

        if lag_hours < 0:
            reason = "fetched before its partition date began"
        elif lag_hours > max_lag_hours:
            reason = f"fetched {lag_hours:.1f}h after its partition date began (limit {max_lag_hours}h)"
        else:
            continue

        findings.append(
            MislabelledSnapshot(
                partition_date=provenance.partition_date,
                fetched_at_utc=provenance.fetched_at_utc,
                lag_hours=lag_hours,
                reason=reason,
            )
        )

    return findings


def find_duplicate_fetches(provenances: list[SnapshotProvenance]) -> list[tuple[int, list[date]]]:
    """Group dates that share one CoinCap response timestamp.

    Two partition dates carrying the same `api_timestamp_ms` means one API response
    was stored twice under different labels — the copy form of I10, as opposed to
    two separate catch-up fetches seconds apart (which `find_mislabelled_snapshots`
    catches instead). Cheap to check and impossible to produce legitimately, since
    CoinCap stamps every response with the millisecond it was generated.
    """
    by_timestamp: dict[int, list[date]] = {}
    for provenance in provenances:
        if provenance.api_timestamp_ms is None:
            continue
        by_timestamp.setdefault(provenance.api_timestamp_ms, []).append(provenance.partition_date)

    return [
        (api_timestamp_ms, sorted(dates))
        for api_timestamp_ms, dates in sorted(by_timestamp.items())
        if len(dates) > 1
    ]


def summarize_coverage(provenances: list[SnapshotProvenance]) -> str:
    """One line on how much of Bronze can be audited at all."""
    known = [p for p in provenances if p.is_known]
    unknown = len(provenances) - len(known)
    oldest_known = min((p.partition_date for p in known), default=None)

    return (
        f"{len(known)} of {len(provenances)} Bronze date(s) carry provenance columns "
        f"({unknown} predate H5 and cannot be audited)"
        + (f"; earliest auditable date is {oldest_known.isoformat()}" if oldest_known else "")
    )


def fetch_lag(provenance: SnapshotProvenance) -> timedelta | None:
    """How long after its partition date began the snapshot was fetched."""
    if provenance.fetched_at_utc is None:
        return None
    day_start = datetime.combine(provenance.partition_date, time.min, tzinfo=timezone.utc)
    return provenance.fetched_at_utc - day_start
