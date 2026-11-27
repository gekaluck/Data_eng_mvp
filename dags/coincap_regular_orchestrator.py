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

# This schedule is only meaningful *relative to the capture's*, so both are stated here.
# `.github/workflows/daily-capture.yml` owns the capture cron — GitHub reads that file and
# nothing reads the mirror below. `test_orchestrator_mirrors_the_real_capture_cron` parses
# the workflow and fails if the two disagree, so moving the real cron cannot leave this
# comment quietly wrong; the gap assertion measures against the workflow's value.
#
# GitHub's scheduled runs are best-effort and late, never early, and the lateness keeps
# growing: ~3.5h in July 2026, a 4.6h median in September, 7.5h in late November, with a
# worst case of 9.8h (the 00:30 UTC cron started at 10:15 UTC on 11-24). Twice a fixed
# offset was chosen to clear it, and twice the drift outgrew it, leaving every layer a
# day behind with nothing failing (I14 at one hour, I20 and I30 at five).
#
# So the orchestrator no longer guesses when the capture lands. It polls hourly
# instead (D047). The sync already skips cheaply when Bronze is current, so most runs are a
# bucket listing and a skip with no Spark. The run after the capture lands picks it up,
# at most an hour later, however late GitHub was. The same-UTC-day constraint
# (the capture labels its partition from the wall clock at fetch time) no longer
# needs a buffer: whichever hourly run sees the file processes it under its own label.
CAPTURE_CRON_UTC = "30 0 * * *"
ORCHESTRATOR_CRON_UTC = "15 * * * *"

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

# Downstream DAGs process exactly the dates the sync just landed, pulled from the sync
# task's XCom so they are discovered, never assumed. This is the date *list*, not the
# start/end of it: the capture can miss a day (GitHub dropped the 2026-10-11 run), and
# a start..end range would ask Silver to wait for a partition that will never exist.
# That stalled a 107-day catch-up for good, because Bronze was then already current
# and no later sync offered those dates again (I29).
_SYNCED = f"ti.xcom_pull(task_ids='{SYNC_TASK_ID}')"
TRIGGER_CONF = {
    "dates": f"{{{{ {_SYNCED}['synced_dates'] | join(',') }}}}",
    "source_dag_id": "{{ dag.dag_id }}",
    "source_run_id": "{{ run_id }}",
}


@dag(
    dag_id="coincap_regular_orchestrator",
    description="Sync cloud-captured snapshots, then run Silver and Gold over the caught-up range",
    schedule=ORCHESTRATOR_CRON_UTC,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # One run at a time. When the stack starts after a long gap, the scheduler can
    # create more than one run at once; on 2026-11-26 two runs synced the same 107
    # dates and triggered Silver over them twice, in parallel (I29). The hourly
    # schedule (D047) depends on this as well: a multi-hour catch-up must queue the
    # next poll, not run beside it.
    max_active_runs=1,
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
