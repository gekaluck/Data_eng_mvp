"""Tests for the paused-downstream-DAG guard (H1, incident I9)."""

import pytest

from utils.downstream_guard import DownstreamDagNotReady, check_downstream_dags

DOWNSTREAM = ["silver_coincap_assets", "gold_coincap_assets"]


def test_all_unpaused_dags_pass():
    check_downstream_dags(DOWNSTREAM, {dag_id: False for dag_id in DOWNSTREAM})


def test_a_paused_dag_raises_and_names_it():
    """The message must name the DAG — I9's failure said nothing useful for 8 days."""
    with pytest.raises(DownstreamDagNotReady) as excinfo:
        check_downstream_dags(
            DOWNSTREAM,
            {"silver_coincap_assets": False, "gold_coincap_assets": True},
        )

    message = str(excinfo.value)
    assert "paused: gold_coincap_assets" in message
    assert "unpause" in message


def test_every_paused_dag_is_reported_not_just_the_first():
    with pytest.raises(DownstreamDagNotReady) as excinfo:
        check_downstream_dags(DOWNSTREAM, {dag_id: True for dag_id in DOWNSTREAM})

    message = str(excinfo.value)
    for dag_id in DOWNSTREAM:
        assert dag_id in message


def test_a_dag_airflow_has_never_heard_of_raises():
    """A missing DAG is usually an import error; triggering it fails obscurely later."""
    with pytest.raises(DownstreamDagNotReady) as excinfo:
        check_downstream_dags(DOWNSTREAM, {"silver_coincap_assets": False})

    assert "gold_coincap_assets" in str(excinfo.value)
    assert "unknown" in str(excinfo.value)


def test_a_none_paused_flag_counts_as_unknown():
    """`DagModel.get_dagmodel` returns None for a DAG that isn't registered."""
    with pytest.raises(DownstreamDagNotReady):
        check_downstream_dags(
            DOWNSTREAM,
            {"silver_coincap_assets": False, "gold_coincap_assets": None},
        )
