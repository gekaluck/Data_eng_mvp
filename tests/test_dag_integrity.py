"""DAG integrity tests - verify DAGs load without import errors."""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    """Load all DAGs from the default dags_folder."""
    return DagBag(include_examples=False)


class TestDagIntegrity:
    """Ensure all DAGs parse and have expected structure."""

    def test_no_import_errors(self, dagbag):
        """No DAG file should have import errors."""
        assert dagbag.import_errors == {}, (
            f"DAG import errors: {dagbag.import_errors}"
        )

    def test_bronze_dag_exists(self, dagbag):
        """The bronze_coincap_assets DAG should be present."""
        assert "bronze_coincap_assets" in dagbag.dags

    def test_bronze_dag_tasks(self, dagbag):
        """Bronze DAG should only fetch data when run directly."""
        dag = dagbag.dags["bronze_coincap_assets"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"fetch_validate_upload"}

    def test_bronze_dag_task_order(self, dagbag):
        """Bronze DAG should have no downstream chaining when run directly."""
        dag = dagbag.dags["bronze_coincap_assets"]
        fetch_task = dag.get_task("fetch_validate_upload")
        assert fetch_task.downstream_list == []

    def test_bronze_dag_has_target_date_param(self, dagbag):
        """Bronze DAG should expose a manual target_date override."""
        dag = dagbag.dags["bronze_coincap_assets"]
        assert "target_date" in dag.params

    def test_bronze_dag_tags(self, dagbag):
        """DAG should be tagged for filtering in the UI."""
        dag = dagbag.dags["bronze_coincap_assets"]
        assert "bronze" in dag.tags
        assert "coincap" in dag.tags

    def test_bronze_history_backfill_dag_exists(self, dagbag):
        """The Bronze history backfill DAG should be present."""
        assert "bronze_coincap_history_backfill" in dagbag.dags

    def test_bronze_history_backfill_dag_tasks(self, dagbag):
        """Bronze history backfill DAG should plan, fetch/upload, then trigger Silver."""
        dag = dagbag.dags["bronze_coincap_history_backfill"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {
            "discover_backfill_plan",
            "fetch_validate_upload_history",
            "trigger_silver_history_backfill",
        }

    def test_bronze_history_backfill_dag_params(self, dagbag):
        """Bronze history backfill DAG should expose anchor and day-count params."""
        dag = dagbag.dags["bronze_coincap_history_backfill"]
        assert "anchor_snapshot_date" in dag.params
        assert "backfill_days" in dag.params

    def test_bronze_history_backfill_dag_tags(self, dagbag):
        """Bronze history backfill DAG should be tagged for UI filtering."""
        dag = dagbag.dags["bronze_coincap_history_backfill"]
        assert "bronze" in dag.tags
        assert "coincap" in dag.tags
        assert "backfill" in dag.tags

    def test_hello_world_dag_exists(self, dagbag):
        """The M1 hello_world DAG should still load fine."""
        assert "hello_world" in dagbag.dags

    def test_silver_dag_exists(self, dagbag):
        """The silver_coincap_assets DAG should be present."""
        assert "silver_coincap_assets" in dagbag.dags

    def test_silver_dag_tasks(self, dagbag):
        """Silver DAG must wait for Bronze, then run the Spark transform when run directly."""
        dag = dagbag.dags["silver_coincap_assets"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"wait_for_bronze", "run_silver_transform"}

    def test_silver_dag_task_order(self, dagbag):
        """Silver sensor should be upstream of the transform task."""
        dag = dagbag.dags["silver_coincap_assets"]
        wait_task = dag.get_task("wait_for_bronze")
        assert "run_silver_transform" in {t.task_id for t in wait_task.downstream_list}

    def test_silver_dag_has_target_date_param(self, dagbag):
        """Silver DAG should expose a manual target_date override."""
        dag = dagbag.dags["silver_coincap_assets"]
        assert "target_date" in dag.params

    def test_silver_dag_tags(self, dagbag):
        """Silver DAG should be tagged for filtering in the UI."""
        dag = dagbag.dags["silver_coincap_assets"]
        assert "silver" in dag.tags
        assert "coincap" in dag.tags

    def test_silver_history_backfill_dag_exists(self, dagbag):
        """The Silver history backfill DAG should be present."""
        assert "silver_coincap_history_backfill" in dagbag.dags

    def test_silver_history_backfill_dag_tasks(self, dagbag):
        """Silver history backfill DAG must load a triggered plan, then wait, then run Spark."""
        dag = dagbag.dags["silver_coincap_history_backfill"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {
            "load_triggered_backfill_plan",
            "wait_for_bronze_history_backfill",
            "run_silver_history_backfill",
        }

    def test_silver_history_backfill_dag_task_order(self, dagbag):
        """Bronze history sensor must be upstream of the Spark backfill task."""
        dag = dagbag.dags["silver_coincap_history_backfill"]
        wait_task = dag.get_task("wait_for_bronze_history_backfill")
        assert "run_silver_history_backfill" in {
            t.task_id for t in wait_task.downstream_list
        }

    def test_silver_history_backfill_dag_has_manual_rerun_params(self, dagbag):
        """Silver history backfill DAG should support manual reruns from the UI."""
        dag = dagbag.dags["silver_coincap_history_backfill"]
        assert "anchor_snapshot_date" in dag.params
        assert "backfill_days" in dag.params
        assert "coin_ids" in dag.params

    def test_silver_history_backfill_dag_tags(self, dagbag):
        """Silver history backfill DAG should be tagged for filtering in the UI."""
        dag = dagbag.dags["silver_coincap_history_backfill"]
        assert "silver" in dag.tags
        assert "coincap" in dag.tags
        assert "backfill" in dag.tags

    def test_gold_dag_exists(self, dagbag):
        """The gold_coincap_assets DAG should be present."""
        assert "gold_coincap_assets" in dagbag.dags

    def test_gold_dag_has_one_task(self, dagbag):
        """Gold DAG should run the Spark transform task only."""
        dag = dagbag.dags["gold_coincap_assets"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"run_gold_transform"}

    def test_gold_dag_has_target_date_param(self, dagbag):
        """Gold DAG should expose a manual target_date override."""
        dag = dagbag.dags["gold_coincap_assets"]
        assert "target_date" in dag.params

    def test_gold_dag_tags(self, dagbag):
        """Gold DAG should be tagged for filtering in the UI."""
        dag = dagbag.dags["gold_coincap_assets"]
        assert "gold" in dag.tags
        assert "coincap" in dag.tags

    def test_dbt_gold_dag_exists(self, dagbag):
        """The gold_dbt_coincap_assets DAG should be present."""
        assert "gold_dbt_coincap_assets" in dagbag.dags

    def test_dbt_gold_dag_has_one_task(self, dagbag):
        """dbt Gold DAG should run the dbt task only."""
        dag = dagbag.dags["gold_dbt_coincap_assets"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"run_dbt_gold"}

    def test_dbt_gold_dag_has_target_date_param(self, dagbag):
        """dbt Gold DAG should expose a manual target_date override."""
        dag = dagbag.dags["gold_dbt_coincap_assets"]
        assert "target_date" in dag.params

    def test_dbt_gold_dag_tags(self, dagbag):
        """dbt Gold DAG should be tagged for filtering in the UI."""
        dag = dagbag.dags["gold_dbt_coincap_assets"]
        assert "gold" in dag.tags
        assert "dbt" in dag.tags
        assert "coincap" in dag.tags

    def test_orchestrator_dag_exists(self, dagbag):
        """The regular CoinCap orchestrator DAG should be present."""
        assert "coincap_regular_orchestrator" in dagbag.dags

    def test_orchestrator_dag_tasks(self, dagbag):
        """The orchestrator should chain Bronze and Silver, then fan out to both Gold DAGs."""
        dag = dagbag.dags["coincap_regular_orchestrator"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {
            "trigger_bronze_assets",
            "trigger_silver_assets",
            "trigger_gold_assets",
            "trigger_gold_dbt_assets",
        }

    def test_orchestrator_dag_task_order(self, dagbag):
        """The orchestrator should run Bronze, then Silver, then both Gold branches."""
        dag = dagbag.dags["coincap_regular_orchestrator"]
        bronze_task = dag.get_task("trigger_bronze_assets")
        assert "trigger_silver_assets" in {t.task_id for t in bronze_task.downstream_list}
        silver_task = dag.get_task("trigger_silver_assets")
        downstream_ids = {t.task_id for t in silver_task.downstream_list}
        assert "trigger_gold_assets" in downstream_ids
        assert "trigger_gold_dbt_assets" in downstream_ids
