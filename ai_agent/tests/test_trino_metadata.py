"""Tests for fixed-shape, allow-listed Iceberg metadata queries."""

from datetime import datetime, timezone

import pytest

from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.trino_metadata import (
    IcebergMetadataAdapter,
    QueryResult,
    TrinoDbApiRunner,
)
from ai_agent.tests.metadata_fixtures import make_allow_list

TABLE = "gold.crypto_dbt.daily_snapshot"


class SequenceRunner:
    def __init__(self, results):
        self.results = list(results)
        self.queries = []

    def execute(self, sql):
        self.queries.append(sql)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_get_table_schema_reads_only_fixed_metadata_relations():
    committed_at = datetime(2026, 8, 6, 17, 29, tzinfo=timezone.utc)
    runner = SequenceRunner(
        [
            QueryResult(
                columns=("Column", "Type", "Extra", "Comment"),
                rows=(
                    ("snapshot_date", "date", "not null", None),
                    ("coin_id", "varchar", "", "Live comment"),
                ),
            ),
            QueryResult(
                columns=("Column", "Type", "Extra", "Comment"),
                rows=(("partition", "row(snapshot_date date, bucket_id integer)", "", ""),),
            ),
            QueryResult(
                columns=("row_count", "size_bytes"),
                rows=((3379, 514486),),
            ),
            QueryResult(columns=("committed_at",), rows=((committed_at,),)),
            QueryResult(columns=("sort_order_id",), rows=((0,),)),
        ]
    )
    adapter = IcebergMetadataAdapter(runner, make_allow_list())

    schema = adapter.get_table_schema(TABLE)

    assert schema.columns[0].nullable is False
    assert schema.columns[1].nullable is None
    assert schema.columns[1].comment == "Live comment"
    assert schema.partition_spec == ("snapshot_date", "bucket_id")
    assert schema.sort_order == ()
    assert schema.stats.row_count == 3379
    assert schema.stats.size_bytes == 514486
    assert schema.stats.last_updated == committed_at
    assert runner.queries[0] == 'DESCRIBE "gold"."crypto_dbt"."daily_snapshot"'
    assert all("SELECT *" not in query for query in runner.queries)
    assert all("daily_snapshot" in query for query in runner.queries)
    assert "ELSE -record_count" in runner.queries[2]


def test_non_default_sort_id_is_not_misrepresented_as_a_column_order():
    runner = SequenceRunner(
        [
            QueryResult(columns=(), rows=(("id", "bigint", "", None),)),
            QueryResult(columns=(), rows=()),
            QueryResult(columns=(), rows=((1, 10),)),
            QueryResult(columns=(), rows=()),
            QueryResult(columns=(), rows=((2,),)),
        ]
    )

    schema = IcebergMetadataAdapter(runner, make_allow_list()).get_table_schema(TABLE)

    assert schema.sort_order == ()
    assert "non-default sort-order ID" in schema.warnings[0]


def test_get_snapshots_bounds_limit_and_normalizes_summary():
    committed_at = datetime(2026, 8, 6, 17, 29, tzinfo=timezone.utc)
    runner = SequenceRunner(
        [
            QueryResult(
                columns=("snapshot_id", "committed_at", "operation", "summary"),
                rows=((42, committed_at, "append", {"added-records": 20}),),
            )
        ]
    )
    adapter = IcebergMetadataAdapter(runner, make_allow_list())

    snapshots = adapter.get_table_snapshots(TABLE, limit=3)

    assert snapshots.snapshots[0].snapshot_id == 42
    assert snapshots.snapshots[0].summary == {"added-records": "20"}
    assert runner.queries == [
        (
            'SELECT snapshot_id, committed_at, operation, summary FROM '
            '"gold"."crypto_dbt"."daily_snapshot$snapshots" '
            "ORDER BY committed_at DESC LIMIT 3"
        )
    ]

    for invalid in (0, 101, True, "2"):
        with pytest.raises(GuardrailError) as error:
            adapter.get_table_snapshots(TABLE, limit=invalid)
        assert error.value.code is ErrorCode.PARSE_ERROR


def test_disallowed_table_fails_before_any_query():
    runner = SequenceRunner([])
    adapter = IcebergMetadataAdapter(runner, make_allow_list())

    with pytest.raises(GuardrailError) as error:
        adapter.get_table_schema("gold.crypto_dbt.hidden")

    assert error.value.code is ErrorCode.TABLE_NOT_ALLOWED
    assert runner.queries == []


def test_runner_failure_becomes_retryable_structured_engine_error():
    runner = SequenceRunner([RuntimeError("coordinator unavailable")])

    with pytest.raises(GuardrailError) as error:
        IcebergMetadataAdapter(runner, make_allow_list()).get_table_schema(TABLE)

    assert error.value.code is ErrorCode.ENGINE_ERROR
    assert error.value.retryable is True
    assert "coordinator unavailable" in error.value.message


def test_malformed_metadata_becomes_non_retryable_structured_engine_error():
    runner = SequenceRunner(
        [
            QueryResult(columns=(), rows=(("id",),)),
            QueryResult(columns=(), rows=()),
            QueryResult(columns=(), rows=((1, 10),)),
            QueryResult(columns=(), rows=()),
            QueryResult(columns=(), rows=()),
        ]
    )

    with pytest.raises(GuardrailError) as error:
        IcebergMetadataAdapter(runner, make_allow_list()).get_table_schema(TABLE)

    assert error.value.code is ErrorCode.ENGINE_ERROR
    assert error.value.retryable is False
    assert "incompatible metadata" in error.value.message


def test_dbapi_runner_reads_local_environment_without_user_override(monkeypatch):
    monkeypatch.setenv("AI_TRINO_HOST", "trino.example")
    monkeypatch.setenv("AI_TRINO_PORT", "8443")
    monkeypatch.setenv("AI_TRINO_SCHEME", "https")
    monkeypatch.setenv("AI_TRINO_REQUEST_TIMEOUT_SECONDS", "4.5")
    monkeypatch.setenv("AI_TRINO_USER", "admin")

    runner = TrinoDbApiRunner.from_env()

    assert runner._host == "trino.example"
    assert runner._port == 8443
    assert runner._http_scheme == "https"
    assert runner._request_timeout_seconds == 4.5
    assert runner._source == "ai-metadata-tools"


def test_dbapi_runner_accepts_a_fixed_tool_source():
    runner = TrinoDbApiRunner.from_env(source="ai-explain-query")

    assert runner._source == "ai-explain-query"
