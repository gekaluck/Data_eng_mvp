"""Tests for composition of live schema truth and dbt annotations."""

from datetime import datetime, timezone

from ai_agent.mcp_server.metadata_models import (
    ColumnSchema,
    TableSchema,
    TableStats,
)
from ai_agent.mcp_server.metadata_tools import MetadataTools
from ai_agent.tests.metadata_fixtures import make_dbt_adapter


class FakeIceberg:
    def get_table_schema(self, table):
        return TableSchema(
            table=table,
            columns=(
                ColumnSchema(name="snapshot_date", type="date"),
                ColumnSchema(name="coin_id", type="varchar", comment="Live wins."),
                ColumnSchema(name="live_only", type="bigint"),
            ),
            stats=TableStats(
                row_count=1,
                size_bytes=10,
                last_updated=datetime(2026, 8, 6, tzinfo=timezone.utc),
            ),
        )


def test_schema_keeps_live_shape_and_uses_dbt_only_as_annotation():
    tools = MetadataTools(make_dbt_adapter(), FakeIceberg())

    schema = tools.get_table_schema("gold.crypto_dbt.daily_snapshot")

    assert [column.name for column in schema.columns] == [
        "snapshot_date",
        "coin_id",
        "live_only",
    ]
    assert schema.columns[0].comment == "UTC snapshot date."
    assert schema.columns[1].comment == "Live wins."
    assert "Live columns missing dbt descriptions: live_only" in schema.warnings
