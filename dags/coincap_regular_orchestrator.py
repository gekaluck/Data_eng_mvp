"""Orchestrate the regular CoinCap Bronze -> Silver -> Gold pipeline."""

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration


TRIGGER_CONF = {
    "target_date": "{{ dag_run.conf.get('target_date', ds) if dag_run and dag_run.conf else ds }}",
    "source_dag_id": "{{ dag.dag_id }}",
    "source_run_id": "{{ run_id }}",
}


@dag(
    dag_id="coincap_regular_orchestrator",
    description="Run the regular CoinCap Bronze, Silver, and Gold flows end to end",
    schedule="@daily",
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
        "retries": 1,
        "retry_delay": duration(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["orchestrator", "coincap", "regular"],
)
def coincap_regular_orchestrator():
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_assets",
        trigger_dag_id="bronze_coincap_assets",
        conf=TRIGGER_CONF,
        wait_for_completion=True,
        poke_interval=15,
    )
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

    trigger_bronze >> trigger_silver
    trigger_silver >> trigger_spark_gold
    trigger_silver >> trigger_dbt_gold >> trigger_dbt_gold_tests


coincap_regular_orchestrator()
