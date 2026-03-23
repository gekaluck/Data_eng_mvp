"""Unit tests for parsing tagged backfill plan output from noisy subprocess logs."""

import dags.silver_coincap_history_backfill as silver_backfill_dag
from dags.bronze_coincap_history_backfill import _extract_backfill_plan as extract_bronze_plan
from dags.silver_coincap_history_backfill import _extract_backfill_plan as extract_silver_plan


PLAN_PAYLOAD = (
    'PLAN_JSON:{"coin_ids":["bitcoin"],"anchor_snapshot_date":"2026-03-15",'
    '"backfill_days":60,"window_start_date":"2026-01-14","window_end_date":"2026-03-14"}'
)


def test_extract_backfill_plan_ignores_non_json_stdout_noise():
    stdout = "\n".join(
        [
            ":: loading settings :: url = jar:file:/.../ivysettings.xml",
            PLAN_PAYLOAD,
            "some trailing spark noise",
        ]
    )
    plan = extract_bronze_plan(stdout, "")

    assert plan["coin_ids"] == ["bitcoin"]
    assert plan["anchor_snapshot_date"] == "2026-03-15"


def test_extract_backfill_plan_can_read_tagged_payload_for_silver_dag():
    stderr = "warning line\n" + PLAN_PAYLOAD
    plan = extract_silver_plan("", stderr)

    assert plan["backfill_days"] == 60
    assert plan["window_end_date"] == "2026-03-14"


def test_bronze_history_backfill_exists_parses_anchor_date_and_checks_all_keys(monkeypatch):
    checked_keys = []

    class DummyHook:
        def __init__(self, aws_conn_id):
            assert aws_conn_id == "minio_s3"

        def check_for_key(self, key, bucket_name):
            checked_keys.append((bucket_name, key))
            return True

    monkeypatch.setattr(silver_backfill_dag, "S3Hook", DummyHook)

    result = silver_backfill_dag.bronze_history_backfill_exists(
        {
            "anchor_snapshot_date": "2026-03-15",
            "backfill_days": 60,
        }
    )

    assert result is True
    assert len(checked_keys) == 3
    assert all(bucket_name == "bronze" for bucket_name, _ in checked_keys)
