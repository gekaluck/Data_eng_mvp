"""DAG integrity tests — verify DAGs load without import errors."""

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

    def test_bronze_dag_has_one_task(self, dagbag):
        """Single-task DAG — fetch_validate_upload only."""
        dag = dagbag.dags["bronze_coincap_assets"]
        assert len(dag.tasks) == 1

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

    # --- Silver DAG (M3) ---

    def test_silver_dag_exists(self, dagbag):
        """The silver_coincap_assets DAG should be present."""
        assert "silver_coincap_assets" in dagbag.dags

    def test_silver_dag_has_two_tasks(self, dagbag):
        """Silver DAG must have exactly: wait_for_bronze + run_silver_transform."""
        dag = dagbag.dags["silver_coincap_assets"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"wait_for_bronze", "run_silver_transform"}

    def test_silver_dag_task_order(self, dagbag):
        """wait_for_bronze must be upstream of run_silver_transform."""
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


