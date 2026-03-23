"""Helpers for aligning DAG runs to target dates and history backfill windows."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any


def resolve_target_date(context: dict[str, Any]) -> date:
    """Return the partition date for this run.

    Scheduled runs default to the DAG's logical date. Manual runs can override the
    target date via `dag_run.conf.target_date` in YYYY-MM-DD format.
    """
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    target_date = conf.get("target_date")

    if not target_date:
        return context["logical_date"].date()

    if isinstance(target_date, date):
        return target_date

    try:
        return datetime.strptime(str(target_date), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            "Invalid target_date. Use YYYY-MM-DD when triggering the DAG manually."
        ) from exc


def resolve_optional_date_param(
    context: dict[str, Any],
    param_name: str,
) -> date | None:
    """Return an optional YYYY-MM-DD date from dag_run.conf, or None when absent."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    raw_value = conf.get(param_name)

    if raw_value in (None, ""):
        return None

    if isinstance(raw_value, date):
        return raw_value

    try:
        return datetime.strptime(str(raw_value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"Invalid {param_name}. Use YYYY-MM-DD when triggering the DAG manually."
        ) from exc


def resolve_int_param(
    context: dict[str, Any],
    param_name: str,
    default: int,
) -> int:
    """Return an integer dag_run.conf override, falling back to a default."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    raw_value = conf.get(param_name, default)

    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {param_name}. Use an integer value.") from exc

    if value < 1:
        raise ValueError(f"Invalid {param_name}. Use a value >= 1.")

    return value


def bronze_assets_key(target_date: date) -> str:
    """Build the Bronze S3 object key for a given partition date."""
    return (
        "crypto/assets/"
        f"year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
        "assets.parquet"
    )


def resolve_backfill_window(anchor_snapshot_date: date, backfill_days: int) -> tuple[date, date]:
    """Return the inclusive backfill window ending the day before the anchor date."""
    if backfill_days < 1:
        raise ValueError("backfill_days must be >= 1")

    return (
        anchor_snapshot_date - timedelta(days=backfill_days),
        anchor_snapshot_date - timedelta(days=1),
    )


def date_to_unix_ms_start(target_date: date) -> int:
    """Convert a date to a UTC start-of-day unix timestamp in milliseconds."""
    return int(datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)


def date_to_unix_ms_end(target_date: date) -> int:
    """Convert a date to a UTC end-of-day unix timestamp in milliseconds."""
    next_day_start = datetime.combine(
        target_date + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    return int(next_day_start.timestamp() * 1000) - 1


def bronze_history_backfill_key(
    anchor_snapshot_date: date,
    backfill_days: int,
    dataset_name: str,
) -> str:
    """Build the Bronze S3 object key for a history backfill dataset."""
    return (
        "crypto/history_backfill/"
        f"anchor_date={anchor_snapshot_date.isoformat()}/"
        f"window_days={backfill_days}/"
        f"{dataset_name}.parquet"
    )
