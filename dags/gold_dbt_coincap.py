"""dbt Gold layer DAG to build Gold models in Trino."""

import logging
import os
import subprocess

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime, duration

from utils.run_dates import resolve_target_date

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
    dag_id="gold_dbt_coincap_assets",
    description="Build CoinCap Gold models with dbt against Trino",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "target_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional YYYY-MM-DD override for manual runs.",
        )
    },
    default_args={
        "retries": 2,
        "retry_delay": duration(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["gold", "dbt", "coincap"],
)
def gold_dbt_coincap_assets():
    @task()
    def run_dbt_gold(**context):
        """Run dbt Gold models for the resolved target date."""
        target_date = resolve_target_date(context)
        target_date_str = target_date.strftime("%Y-%m-%d")

        logger.info("Starting dbt Gold build for %s", target_date_str)
        validate_envvars(os.environ)

        dbt_vars = '{"snapshot_date": "%s"}' % target_date_str
        cmd = [
            "dbt",
            "run",
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
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("dbt subprocess timed out") from exc

        if result.stdout:
            logger.info("dbt stdout:\n%s", result.stdout)
        if result.stderr:
            logger.warning("dbt stderr:\n%s", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"dbt Gold build failed (exit code {result.returncode}). "
                "See dbt output above for details."
            )

        logger.info("dbt Gold build complete for %s", target_date_str)

    run_dbt_gold()


gold_dbt_coincap_assets()


