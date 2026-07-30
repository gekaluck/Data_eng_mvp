"""Plan the sync of cloud-captured snapshots into local Bronze.

The cloud capture (`scripts/capture_daily_snapshot.py`) writes one Parquet per day
to an S3 bucket using the *same* key layout as Bronze, so bringing a day local is a
plain object copy — no transformation and no re-validation (the capture already ran
the Pydantic contract). See D026.

The planning logic (which dates to copy) is pure and testable without touching S3.
The copy itself is at the bottom; it imports the Airflow S3 hook lazily so this
module stays importable — and unit-testable — outside an Airflow process.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Bronze/capture share this prefix and layout:
#   crypto/assets/year=2026/month=07/day=28/assets.parquet
CAPTURE_PREFIX = "crypto/assets/"
_KEY_DATE_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")

# A sync feeds a Gold range rebuild downstream, which caps at MAX_GOLD_RANGE_DAYS.
# Cap here too so an unexpectedly huge diff fails loudly at planning time rather
# than halfway through copying objects.
MAX_SYNC_DATES = 366

BRONZE_BUCKET = "bronze"
BRONZE_CONN_ID = "minio_s3"
CAPTURE_CONN_ID = "capture_s3"

# How stale the newest captured date may get before the sync calls the upstream dead.
# Two days tolerates one missed capture plus GitHub's scheduled-run drift, and still
# notices within a day of a real outage. Set CAPTURE_MAX_AGE_DAYS=0 to disable the
# check — the escape hatch for re-syncing an old date long after the fact.
DEFAULT_CAPTURE_MAX_AGE_DAYS = 2


class CaptureNotFresh(RuntimeError):
    """The capture bucket is empty or has stopped receiving files."""


def dates_from_keys(keys: list[str] | None) -> set[date]:
    """Extract partition dates from Bronze-layout object keys.

    Keys that don't match the layout are ignored — the capture bucket may hold
    unrelated objects (diagnostics, other prefixes) and they aren't our business.
    """
    found: set[date] = set()
    for key in keys or []:
        match = _KEY_DATE_RE.search(key)
        if match:
            year, month, day = (int(part) for part in match.groups())
            found.add(date(year, month, day))
    return found


def plan_sync(
    captured_dates: set[date],
    local_dates: set[date],
    start_date: date | None = None,
    end_date: date | None = None,
    overwrite: bool = False,
) -> list[date]:
    """Return the sorted dates to copy from the capture bucket into Bronze.

    By default this is the set difference (captured but not yet local), which makes
    the sync idempotent: re-running it copies nothing. `overwrite=True` re-copies
    dates that already exist locally — the escape hatch for a Bronze object that got
    corrupted or written with the wrong contents.

    `start_date` / `end_date` narrow the candidate window; either bound may be given
    alone.
    """
    candidates = captured_dates if overwrite else captured_dates - local_dates

    if start_date is not None:
        candidates = {d for d in candidates if d >= start_date}
    if end_date is not None:
        candidates = {d for d in candidates if d <= end_date}

    if len(candidates) > MAX_SYNC_DATES:
        raise ValueError(
            f"Sync would copy {len(candidates)} dates, above the {MAX_SYNC_DATES}-date cap. "
            "Narrow the window with start_date/end_date."
        )

    return sorted(candidates)


def check_capture_freshness(
    captured_dates: set[date],
    today: date,
    max_age_days: int = DEFAULT_CAPTURE_MAX_AGE_DAYS,
) -> None:
    """Raise `CaptureNotFresh` when the capture bucket has gone quiet.

    This closes the blind spot behind I1. When the CoinCap key expires or GitHub
    disables the cron (it does that after 60 days of repo inactivity), files simply
    stop arriving. The sync then finds nothing new and the run *skips* — which is
    also the correct response when there is genuinely nothing new yet. A dead
    upstream and a quiet one look identical from here, so this is the only place
    that can tell them apart: by asking how old the newest captured date is.

    `max_age_days <= 0` disables the check, for re-syncing an old date deliberately.
    """
    if max_age_days <= 0:
        logger.info("Capture freshness check disabled (max_age_days=%d).", max_age_days)
        return

    if not captured_dates:
        raise CaptureNotFresh(
            "The capture bucket holds no snapshots at all. Either CAPTURE_S3_BUCKET "
            "points at the wrong bucket, or the cloud capture has never succeeded. "
            "Check the 'Daily CoinCap capture' GitHub Actions workflow."
        )

    newest = max(captured_dates)
    age_days = (today - newest).days
    if age_days > max_age_days:
        raise CaptureNotFresh(
            f"The capture bucket is stale: its newest snapshot is {newest.isoformat()}, "
            f"{age_days} days old (limit {max_age_days}). Files have stopped arriving. "
            "Check that the 'Daily CoinCap capture' workflow is still enabled — GitHub "
            "disables scheduled workflows after 60 days of repo inactivity — and that "
            "the CoinCap key has not expired."
        )

    logger.info(
        "Capture bucket is fresh: newest snapshot %s is %d day(s) old (limit %d).",
        newest.isoformat(),
        age_days,
        max_age_days,
    )


def _resolve_bool_param(context: dict[str, Any], param_name: str, default: bool = False) -> bool:
    """Read a boolean dag_run.conf override, tolerating the string forms the UI sends."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    raw_value = conf.get(param_name, default)

    if isinstance(raw_value, bool):
        return raw_value
    return str(raw_value).strip().lower() in ("true", "1", "yes")


def sync_captured_to_bronze(context: dict[str, Any]) -> dict[str, Any]:
    """Copy missing captured snapshots into Bronze; return what was synced.

    The return value is the handoff to the rest of the pipeline: the orchestrator
    reads `start_date`/`end_date` off it via XCom and hands that range to Silver and
    Gold, so a catch-up processes exactly the days it just pulled down.
    """
    # Imported here so the planning helpers above stay usable without Airflow.
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    from utils.run_dates import bronze_assets_key, resolve_optional_date_param

    capture_bucket = os.getenv("CAPTURE_S3_BUCKET", "").strip()
    if not capture_bucket:
        raise RuntimeError(
            "CAPTURE_S3_BUCKET is not set. Set it in .env so the sync knows which "
            "bucket the cloud capture writes to."
        )

    start_date = resolve_optional_date_param(context, "start_date")
    end_date = resolve_optional_date_param(context, "end_date")
    overwrite = _resolve_bool_param(context, "overwrite")

    capture_hook = S3Hook(aws_conn_id=CAPTURE_CONN_ID)
    bronze_hook = S3Hook(aws_conn_id=BRONZE_CONN_ID)

    captured_dates = dates_from_keys(
        capture_hook.list_keys(bucket_name=capture_bucket, prefix=CAPTURE_PREFIX)
    )
    local_dates = dates_from_keys(
        bronze_hook.list_keys(bucket_name=BRONZE_BUCKET, prefix=CAPTURE_PREFIX)
    )
    logger.info(
        "Capture bucket has %d date(s); Bronze has %d.", len(captured_dates), len(local_dates)
    )

    # Deliberately before the "nothing to sync" path below: a bucket that stopped
    # receiving files must fail, not skip quietly like a genuinely up-to-date one.
    check_capture_freshness(
        captured_dates,
        today=datetime.now(timezone.utc).date(),
        max_age_days=int(
            os.getenv("CAPTURE_MAX_AGE_DAYS", str(DEFAULT_CAPTURE_MAX_AGE_DAYS))
        ),
    )

    dates_to_sync = plan_sync(
        captured_dates,
        local_dates,
        start_date=start_date,
        end_date=end_date,
        overwrite=overwrite,
    )

    if not dates_to_sync:
        logger.info("Bronze is already current with the capture bucket — nothing to sync.")
        return {"synced_dates": [], "count": 0, "start_date": None, "end_date": None}

    logger.info(
        "Syncing %d date(s): %s",
        len(dates_to_sync),
        ", ".join(d.isoformat() for d in dates_to_sync),
    )
    for target_date in dates_to_sync:
        key = bronze_assets_key(target_date)
        body = capture_hook.get_key(key=key, bucket_name=capture_bucket).get()["Body"].read()
        bronze_hook.load_bytes(
            bytes_data=body,
            key=key,
            bucket_name=BRONZE_BUCKET,
            replace=True,
        )
        logger.info("Synced %s (%d bytes) -> s3://%s/%s", target_date, len(body), BRONZE_BUCKET, key)

    return {
        "synced_dates": [d.isoformat() for d in dates_to_sync],
        "count": len(dates_to_sync),
        "start_date": dates_to_sync[0].isoformat(),
        "end_date": dates_to_sync[-1].isoformat(),
    }
