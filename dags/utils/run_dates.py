"""Helpers for aligning scheduled and manual DAG runs to a target partition date."""

from __future__ import annotations

from datetime import date, datetime
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


def bronze_assets_key(target_date: date) -> str:
    """Build the Bronze S3 object key for a given partition date."""
    return (
        "crypto/assets/"
        f"year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
        "assets.parquet"
    )
