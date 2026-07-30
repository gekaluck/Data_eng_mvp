"""Copy cloud-captured daily snapshots into local Bronze (MinIO).

Phase 2 of the autonomous capture design (D026/D027). The cloud capture writes one
Parquet per day to an S3 bucket in Bronze's own key layout; this DAG pulls down the
days the local lakehouse is missing, so a laptop that was off for a week catches up
in one run.

The copy is byte-for-byte — no transformation, no re-validation. The capture already
applied the Pydantic contract, and Bronze's whole job is to preserve source shape.

Standalone here for manual "just pull what's new" runs. The regular orchestrator runs
the same logic inline so it can feed the caught-up range to Silver and Gold.
"""

import logging

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime, duration

from utils.capture_sync import sync_captured_to_bronze

logger = logging.getLogger(__name__)


@dag(
    dag_id="bronze_capture_sync",
    description="Copy cloud-captured CoinCap snapshots from the capture bucket into Bronze",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "start_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD lower bound on which captured dates to sync.",
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD upper bound on which captured dates to sync.",
        ),
        "overwrite": Param(
            default=False,
            type="boolean",
            description=(
                "Re-copy dates that already exist in Bronze. Off by default so the "
                "sync is idempotent; turn on to repair a bad local partition."
            ),
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": duration(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["bronze", "coincap", "capture"],
)
def bronze_capture_sync():

    @task()
    def sync_from_capture_bucket(**context) -> dict:
        return sync_captured_to_bronze(context)

    sync_from_capture_bucket()


bronze_capture_sync()
