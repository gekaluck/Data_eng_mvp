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
