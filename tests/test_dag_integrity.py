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

    def test_bronze_dag_tags(self, dagbag):
        """DAG should be tagged for filtering in the UI."""
        dag = dagbag.dags["bronze_coincap_assets"]
        assert "bronze" in dag.tags
        assert "coincap" in dag.tags

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

    def test_silver_dag_tags(self, dagbag):
        """Silver DAG should be tagged for filtering in the UI."""
        dag = dagbag.dags["silver_coincap_assets"]
        assert "silver" in dag.tags
        assert "coincap" in dag.tags
