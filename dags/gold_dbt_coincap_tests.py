"""dbt Gold test DAG to validate Gold models in Trino."""

import logging
import os
import subprocess

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime, duration

from utils.run_dates import resolve_target_dates

logger = logging.getLogger(__name__)

REQUIRED_ENVVARS = ["DBT_TRINO_HOST", "DBT_TRINO_PORT", "DBT_TRINO_USER"]
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_SELECT_MODELS = ["daily_snapshot", "mc_rank_change", "wkly_roll_avg"]


def validate_envvars(envvars: dict[str, str]) -> None:
    """Ensure required environment variables exist before launching dbt."""
    missing_envvars = [var for var in REQUIRED_ENVVARS if not envvars.get(var)]
    if missing_envvars:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing_envvars)}"
        )


@dag(
    dag_id="gold_dbt_coincap_tests",
    description="Run dbt tests for CoinCap Gold models against Trino",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "target_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD override for a single manual run.",
        ),
        "start_date": Param(
            default=None,
            type=["null", "string"],
            description=(
                "Optional YYYY-MM-DD start of an inclusive range (requires end_date). "
                "Tests every date the dbt Gold build just covered."
            ),
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD end of an inclusive range (requires start_date).",
        ),
    },
    default_args={
        "retries": 1,
        "retry_delay": duration(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["gold", "dbt", "test", "coincap"],
)
def gold_dbt_coincap_tests():
    @task()
    def run_dbt_gold_tests(**context):
        """Run dbt tests for the Gold models, once per resolved target date.

        Every failing date is reported rather than just the first, so one bad day in
        a catch-up window doesn't hide the state of the rest.
        """
        target_dates = resolve_target_dates(context)
        validate_envvars(os.environ)

        logger.info(
            "dbt Gold tests will run for %d date(s): %s",
            len(target_dates),
            ", ".join(d.isoformat() for d in target_dates),
        )

        failures: list[str] = []
        for target_date in target_dates:
            target_date_str = target_date.strftime("%Y-%m-%d")
            logger.info("Starting dbt Gold tests for %s", target_date_str)

            dbt_vars = '{"snapshot_date": "%s"}' % target_date_str
            cmd = [
                "dbt",
                "test",
                "--project-dir",
                DBT_PROJECT_DIR,
                "--profiles-dir",
                DBT_PROJECT_DIR,
                "--select",
                *DBT_SELECT_MODELS,
                "--vars",
                dbt_vars,
            ]

            try:
                result = subprocess.run(
                    cmd,
                    env=os.environ.copy(),
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=900,
                )
            except FileNotFoundError as exc:
                raise RuntimeError("Failed to start dbt subprocess") from exc
            except subprocess.TimeoutExpired:
                logger.error("dbt subprocess timed out for %s", target_date_str)
                failures.append(target_date_str)
                continue

            if result.stdout:
                logger.info("dbt stdout (%s):\n%s", target_date_str, result.stdout)
            if result.stderr:
                logger.warning("dbt stderr (%s):\n%s", target_date_str, result.stderr)

            if result.returncode != 0:
                logger.error(
                    "dbt Gold tests failed for %s (exit code %d).",
                    target_date_str,
                    result.returncode,
                )
                failures.append(target_date_str)
            else:
                logger.info("dbt Gold tests complete for %s", target_date_str)

        if failures:
            raise RuntimeError(
                f"dbt Gold tests failed for {len(failures)} of {len(target_dates)} "
                f"date(s): {', '.join(failures)}. See dbt output above for details."
            )

    run_dbt_gold_tests()


gold_dbt_coincap_tests()
