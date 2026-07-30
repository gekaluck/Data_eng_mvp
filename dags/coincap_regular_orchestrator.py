"""Orchestrate the regular CoinCap Sync -> Silver -> Gold pipeline.

The daily CoinCap call now happens in the cloud (D026), so this flow no longer
fetches: it syncs whatever the cloud capture has accumulated into Bronze, then
processes exactly the dates it pulled down (D027). A laptop that was off for a week
catches up in a single run instead of leaving a permanent hole.

`bronze_coincap_assets` stays in the repo for manual one-off fetches but is
deliberately not chained here — two daily writers to the same Bronze key would race
and spend two CoinCap credits for one day of data.
"""

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration

from utils.capture_sync import sync_captured_to_bronze
from utils.downstream_guard import assert_downstream_dags_ready

SYNC_TASK_ID = "sync_captured_snapshots"
GUARD_TASK_ID = "check_downstream_dags_ready"

# Every DAG this orchestrator triggers. The guard task checks all of them up front so
# a paused Gold DAG fails the run before Silver spends minutes of Spark on work whose
# results nothing downstream will consume.
DOWNSTREAM_DAG_IDS = [
    "silver_coincap_assets",
    "gold_coincap_assets",
    "gold_dbt_coincap_assets",
    "gold_dbt_coincap_tests",
]

# A trigger task that runs longer than this is not working, it is stuck. Generous
# enough for a multi-day catch-up (Spark Gold builds date by date), short enough that
# a hang surfaces the same day instead of after eight (I9). Without it,
# `wait_for_completion=True` polls forever.
TRIGGER_EXECUTION_TIMEOUT = duration(hours=2)

# Downstream DAGs process the exact window the sync just landed. Pulled from the
# sync task's XCom so the range is discovered, never assumed.
_SYNCED = f"ti.xcom_pull(task_ids='{SYNC_TASK_ID}')"
TRIGGER_CONF = {
    "start_date": f"{{{{ {_SYNCED}['start_date'] }}}}",
    "end_date": f"{{{{ {_SYNCED}['end_date'] }}}}",
    "source_dag_id": "{{ dag.dag_id }}",
    "source_run_id": "{{ run_id }}",
}


@dag(
    dag_id="coincap_regular_orchestrator",
    description="Sync cloud-captured snapshots, then run Silver and Gold over the caught-up range",
    # 01:30 UTC — an hour after the cloud capture's 00:30 UTC cron, so the day's
    # snapshot is already in the bucket when we sync. `@daily` (00:00 UTC) would run
    # *before* the capture and always process the previous day, adding a needless
    # ~24h lag. The hour of headroom absorbs GitHub's scheduled-run drift.
    schedule="30 1 * * *",
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
            description="Re-sync dates already present in Bronze (repairs a bad partition).",
        ),
    },
    default_args={
        "retries": 1,
        "retry_delay": duration(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["orchestrator", "coincap", "regular"],
)
def coincap_regular_orchestrator():

    @task(task_id=SYNC_TASK_ID)
    def sync_captured_snapshots(**context) -> dict:
        """Pull down captured days Bronze doesn't have yet; skip the run if none.

        Skipping on an empty sync is deliberate: with nothing new to process,
        re-running Silver and Gold would burn minutes of Spark to rewrite identical
        partitions. A skipped run is also an honest signal in the grid that the
        cloud capture produced nothing.
        """
        result = sync_captured_to_bronze(context)
        if result["count"] == 0:
            raise AirflowSkipException(
                "Bronze is already current with the capture bucket — nothing to process."
            )
        return result

    @task(task_id=GUARD_TASK_ID)
    def check_downstream_dags_ready() -> dict:
        """Fail loudly now if a DAG we are about to trigger is paused (I9).

        Runs after the sync so a day with nothing to process stays a quiet skip —
        a paused Gold DAG only matters when there is data waiting for it.
        """
        return assert_downstream_dags_ready(DOWNSTREAM_DAG_IDS)

    def _trigger(task_id: str, trigger_dag_id: str) -> TriggerDagRunOperator:
        """Build a trigger task that fails rather than waits forever."""
        return TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id=trigger_dag_id,
            conf=TRIGGER_CONF,
            wait_for_completion=True,
            poke_interval=15,
            execution_timeout=TRIGGER_EXECUTION_TIMEOUT,
            # Re-running the orchestrator for a date it already handled would other-
            # wise collide with the existing run id and fail on a duplicate key. This
            # makes a retry or a manual replay just work.
            reset_dag_run=True,
        )

    synced = sync_captured_snapshots()
    downstream_ready = check_downstream_dags_ready()

    trigger_silver = _trigger("trigger_silver_assets", "silver_coincap_assets")
    trigger_spark_gold = _trigger("trigger_gold_assets", "gold_coincap_assets")
    trigger_dbt_gold = _trigger("trigger_gold_dbt_assets", "gold_dbt_coincap_assets")
    trigger_dbt_gold_tests = _trigger("trigger_gold_dbt_tests", "gold_dbt_coincap_tests")

    synced >> downstream_ready >> trigger_silver
    trigger_silver >> trigger_spark_gold
    trigger_silver >> trigger_dbt_gold >> trigger_dbt_gold_tests
    # The dbt tests now compare the two Gold implementations against each other (H2),
    # so they must wait for the Spark branch as well. Left parallel, a slower Spark
    # Gold would make the comparison test fail on a date it simply had not built yet.
    trigger_spark_gold >> trigger_dbt_gold_tests


coincap_regular_orchestrator()
