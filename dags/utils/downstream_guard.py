"""Refuse to trigger a downstream DAG that cannot possibly run.

Triggering a **paused** DAG does not fail. It creates a run that sits in `queued`
forever, because the scheduler never picks up runs of a paused DAG. Meanwhile
`TriggerDagRunOperator(wait_for_completion=True)` polls that run with no timeout and
no alert. That is exactly how I9 hid an 8-day Gold outage: Bronze and Silver stayed
green and produced fresh data daily while 30 orphaned queued runs piled up.

Checking before we trigger turns a silent indefinite hang into an immediate failure
that names the DAG to unpause. The `execution_timeout` on the trigger tasks is the
backstop for every *other* way a wait can hang; this is the one we can diagnose.

The check itself is a pure function so it can be unit-tested without an Airflow
process or a database, in the same style as `plan_sync` in `capture_sync.py`.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class DownstreamDagNotReady(RuntimeError):
    """A downstream DAG is paused or unknown, so triggering it would hang."""


def check_downstream_dags(
    dag_ids: list[str],
    paused_by_dag_id: dict[str, bool | None],
) -> None:
    """Raise `DownstreamDagNotReady` unless every dag_id is known and unpaused.

    `paused_by_dag_id` maps a dag_id to its paused flag; a missing key or a `None`
    value means Airflow has no record of that DAG. An unknown DAG is treated as
    fatally as a paused one — triggering it would fail obscurely later rather than
    clearly now.
    """
    unknown = [
        dag_id
        for dag_id in dag_ids
        if paused_by_dag_id.get(dag_id) is None
    ]
    paused = [dag_id for dag_id in dag_ids if paused_by_dag_id.get(dag_id) is True]

    if not unknown and not paused:
        return

    problems = []
    if paused:
        problems.append(
            f"paused: {', '.join(paused)} — unpause in the Airflow UI or run "
            f"`airflow dags unpause {paused[0]}`. Triggering a paused DAG queues a run "
            "the scheduler will never start (incident I9)."
        )
    if unknown:
        problems.append(
            f"unknown to Airflow: {', '.join(unknown)} — check for a DAG import error."
        )

    raise DownstreamDagNotReady(
        "Refusing to trigger downstream DAGs. " + " ".join(problems)
    )


def assert_downstream_dags_ready(dag_ids: list[str]) -> dict[str, bool | None]:
    """Look the DAGs up in Airflow's metadata DB and apply `check_downstream_dags`.

    Returns the paused flags it read, so the task logs a record of what it saw.
    """
    # Imported lazily so the pure check above stays importable outside Airflow.
    from airflow.models import DagModel

    paused_by_dag_id: dict[str, bool | None] = {}
    for dag_id in dag_ids:
        dag_model = DagModel.get_dagmodel(dag_id)
        paused_by_dag_id[dag_id] = None if dag_model is None else bool(dag_model.is_paused)

    logger.info("Downstream DAG paused flags: %s", paused_by_dag_id)
    check_downstream_dags(dag_ids, paused_by_dag_id)
    return paused_by_dag_id
