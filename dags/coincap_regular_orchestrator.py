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

SYNC_TASK_ID = "sync_captured_snapshots"

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

    synced = sync_captured_snapshots()

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_assets",
        trigger_dag_id="silver_coincap_assets",
        conf=TRIGGER_CONF,
        wait_for_completion=True,
        poke_interval=15,
    )
    trigger_spark_gold = TriggerDagRunOperator(
        task_id="trigger_gold_assets",
        trigger_dag_id="gold_coincap_assets",
        conf=TRIGGER_CONF,
        wait_for_completion=True,
        poke_interval=15,
    )
    trigger_dbt_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dbt_assets",
        trigger_dag_id="gold_dbt_coincap_assets",
        conf=TRIGGER_CONF,
        wait_for_completion=True,
        poke_interval=15,
    )
    trigger_dbt_gold_tests = TriggerDagRunOperator(
        task_id="trigger_gold_dbt_tests",
        trigger_dag_id="gold_dbt_coincap_tests",
        conf=TRIGGER_CONF,
        wait_for_completion=True,
        poke_interval=15,
    )

    synced >> trigger_silver
    trigger_silver >> trigger_spark_gold
    trigger_silver >> trigger_dbt_gold >> trigger_dbt_gold_tests


coincap_regular_orchestrator()
