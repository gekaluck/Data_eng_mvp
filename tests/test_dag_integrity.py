"""DAG integrity tests - verify DAGs load without import errors."""

import re
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# The capture workflow, from either the repo root (host) or the container's mount.
CAPTURE_WORKFLOW_CANDIDATES = (
    Path(__file__).resolve().parent.parent / ".github" / "workflows" / "daily-capture.yml",
    Path("/opt/airflow/.github/workflows/daily-capture.yml"),
)
_CRON_RE = re.compile(r"""^\s*-\s*cron:\s*["']([^"']+)["']""", re.MULTILINE)


def _capture_cron_from_workflow(candidates=CAPTURE_WORKFLOW_CANDIDATES) -> str:
    """Read the capture's schedule from the file GitHub actually obeys.

    Deliberately fails rather than skips when the workflow can't be found: a test
    that quietly stops running is how the schedule coupling drifts in the first place.
    A regex avoids a PyYAML dependency for one line of a file we control.
    """
    for path in candidates:
        if path.is_file():
            crons = _CRON_RE.findall(path.read_text(encoding="utf-8"))
            assert len(crons) == 1, f"expected exactly one cron in {path}, found {crons}"
            return crons[0].strip()

    raise AssertionError(
        "Could not read .github/workflows/daily-capture.yml from "
        + " or ".join(str(p) for p in candidates)
        + ". In Docker it comes from the read-only .github mount in docker-compose.yml."
    )


def _write_workflow(directory: Path, cron: str) -> Path:
    path = directory / "daily-capture.yml"
    path.write_text(f'on:\n  schedule:\n    - cron: "{cron}"\n', encoding="utf-8")
    return path


def test_capture_cron_parser_reads_the_schedule(tmp_path):
    """The schedule assertions are only worth anything if this really parses the file."""
    assert _capture_cron_from_workflow([_write_workflow(tmp_path, "15 3 * * *")]) == "15 3 * * *"


def test_capture_cron_parser_fails_when_the_workflow_is_missing(tmp_path):
    """A missing workflow must fail loudly rather than skip the coupling checks."""
    with pytest.raises(AssertionError, match="Could not read"):
        _capture_cron_from_workflow([tmp_path / "nope.yml"])


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

    def test_dbt_gold_dag_builds_then_publishes_artifacts(self, dagbag):
        """dbt Gold DAG should publish metadata only after the model build succeeds."""
        dag = dagbag.dags["gold_dbt_coincap_assets"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"run_dbt_gold", "publish_dbt_artifacts"}
        assert dag.get_task("run_dbt_gold").downstream_task_ids == {
            "publish_dbt_artifacts"
        }

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

    def test_dbt_gold_test_dag_exists(self, dagbag):
        """The gold_dbt_coincap_tests DAG should be present."""
        assert "gold_dbt_coincap_tests" in dagbag.dags

    def test_dbt_gold_test_dag_has_one_task(self, dagbag):
        """dbt Gold test DAG should run the dbt test task only."""
        dag = dagbag.dags["gold_dbt_coincap_tests"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"run_dbt_gold_tests"}

    def test_dbt_gold_test_dag_has_target_date_param(self, dagbag):
        """dbt Gold test DAG should expose a manual target_date override."""
        dag = dagbag.dags["gold_dbt_coincap_tests"]
        assert "target_date" in dag.params

    def test_dbt_gold_test_dag_tags(self, dagbag):
        """dbt Gold test DAG should be tagged for filtering in the UI."""
        dag = dagbag.dags["gold_dbt_coincap_tests"]
        assert "gold" in dag.tags
        assert "dbt" in dag.tags
        assert "test" in dag.tags
        assert "coincap" in dag.tags

    def test_orchestrator_dag_exists(self, dagbag):
        """The regular CoinCap orchestrator DAG should be present."""
        assert "coincap_regular_orchestrator" in dagbag.dags

    def test_orchestrator_dag_tasks(self, dagbag):
        """The orchestrator syncs captured snapshots, then runs Silver and both Gold branches."""
        dag = dagbag.dags["coincap_regular_orchestrator"]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {
            "sync_captured_snapshots",
            "check_downstream_dags_ready",
            "trigger_silver_assets",
            "trigger_gold_assets",
            "trigger_gold_dbt_assets",
            "trigger_gold_dbt_tests",
        }

    def test_every_trigger_task_has_an_execution_timeout(self, dagbag):
        """An untimed `wait_for_completion=True` waits forever.

        That is I9: two Gold DAGs were paused, their triggered runs sat in `queued`,
        and the orchestrator polled them for 8 days while nothing alerted. A timeout
        turns an indefinite wait into a failed task.
        """
        dag = dagbag.dags["coincap_regular_orchestrator"]
        trigger_tasks = [
            task for task in dag.tasks if isinstance(task, TriggerDagRunOperator)
        ]
        assert trigger_tasks, "expected the orchestrator to trigger downstream DAGs"
        for task in trigger_tasks:
            assert task.execution_timeout is not None, (
                f"{task.task_id} can wait forever; give it an execution_timeout"
            )

    def test_orchestrator_checks_downstream_dags_before_triggering(self, dagbag):
        """The paused check must run before the first trigger, not alongside it."""
        dag = dagbag.dags["coincap_regular_orchestrator"]
        guard_task = dag.get_task("check_downstream_dags_ready")
        assert "trigger_silver_assets" in {t.task_id for t in guard_task.downstream_list}

    def test_orchestrator_does_not_fetch_from_coincap(self, dagbag):
        """The daily CoinCap call belongs to the cloud capture now (D027).

        Chaining the local Bronze fetch here again would put two writers on the same
        Bronze key and spend two API credits for one day of data.
        """
        dag = dagbag.dags["coincap_regular_orchestrator"]
        assert "trigger_bronze_assets" not in {t.task_id for t in dag.tasks}

    def test_orchestrator_dag_task_order(self, dagbag):
        """Sync, then the paused check, then Silver, then Gold, with dbt tests last."""
        dag = dagbag.dags["coincap_regular_orchestrator"]
        sync_task = dag.get_task("sync_captured_snapshots")
        assert "check_downstream_dags_ready" in {t.task_id for t in sync_task.downstream_list}
        silver_task = dag.get_task("trigger_silver_assets")
        downstream_ids = {t.task_id for t in silver_task.downstream_list}
        assert "trigger_gold_assets" in downstream_ids
        assert "trigger_gold_dbt_assets" in downstream_ids
        dbt_gold_task = dag.get_task("trigger_gold_dbt_assets")
        assert "trigger_gold_dbt_tests" in {
            t.task_id for t in dbt_gold_task.downstream_list
        }

    def test_capture_sync_dag_exists(self, dagbag):
        """The standalone sync DAG should be present for manual catch-up runs."""
        assert "bronze_capture_sync" in dagbag.dags

    def test_orchestrator_mirrors_the_real_capture_cron(self):
        """The DAG's copy of the capture cron must match the workflow that owns it.

        The workflow file is the authoritative schedule — GitHub reads it, nothing
        reads the DAG's constant. Without this check the gap test below would compare
        two values from the same file and keep passing after someone moved the real
        capture, which is exactly how I20 happened.
        """
        from coincap_regular_orchestrator import CAPTURE_CRON_UTC

        assert _capture_cron_from_workflow() == CAPTURE_CRON_UTC, (
            "CAPTURE_CRON_UTC in dags/coincap_regular_orchestrator.py no longer matches "
            ".github/workflows/daily-capture.yml. Update it, and re-check the orchestrator's "
            "own schedule against it (D034)."
        )

    def test_orchestrator_runs_well_after_the_capture_cron(self, dagbag):
        """The orchestrator must leave room for GitHub's scheduled-run drift.

        This coupling has failed twice. I14: the orchestrator ran at 00:00 UTC, before
        the 00:30 UTC capture, and processed the previous day forever. I20: drift grew
        to ~3.5h and swallowed the one-hour buffer that fixed I14, with the same
        result and nothing failing. Both times the schedules were correct in isolation.

        Four hours is the floor, not the target — the DAG uses five. The capture side
        is read from the workflow file, so moving the real cron fails this test.
        """
        from coincap_regular_orchestrator import ORCHESTRATOR_CRON_UTC

        def _minutes(cron: str) -> int:
            minute, hour = cron.split()[:2]
            return int(hour) * 60 + int(minute)

        gap_minutes = _minutes(ORCHESTRATOR_CRON_UTC) - _minutes(_capture_cron_from_workflow())
        assert gap_minutes >= 4 * 60, (
            f"the orchestrator runs {gap_minutes}min after the capture cron; "
            "GitHub's drift is routinely hours, so the sync would miss the same day's snapshot"
        )
        assert dagbag.dags["coincap_regular_orchestrator"].schedule_interval == ORCHESTRATOR_CRON_UTC

    def test_range_capable_dags_accept_start_and_end_date(self, dagbag):
        """Every DAG in the catch-up path must accept the range the sync discovers.

        If one of these silently ignored start_date/end_date it would process only a
        single day of a multi-day catch-up, leaving the layers out of step.
        """
        for dag_id in (
            "silver_coincap_assets",
            "gold_coincap_assets",
            "gold_dbt_coincap_assets",
            "gold_dbt_coincap_tests",
        ):
            params = dagbag.dags[dag_id].params
            assert "start_date" in params, f"{dag_id} is missing the start_date param"
            assert "end_date" in params, f"{dag_id} is missing the end_date param"
