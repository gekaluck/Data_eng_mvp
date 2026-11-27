"""In-memory MCP tools for offline eval tests: canned results keyed by exact SQL."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from ai_agent.agent_service.mcp_tools import ToolCallError
from ai_agent.mcp_server.metadata_models import (
    ColumnDocs,
    ColumnSchema,
    ModelDocs,
    TableSchema,
    TableSnapshot,
    TableSnapshots,
    TableStats,
    TableSummary,
)
from ai_agent.mcp_server.query_executor import (
    DATA_CAVEATS,
    ExecutedQuery,
    QueryExecutionStats,
)
from ai_agent.mcp_server.query_explainer import QueryExplanation

TABLE = "gold.crypto_dbt.daily_snapshot"
NOW = datetime(2026, 11, 26, tzinfo=timezone.utc)


def executed(columns, rows, *, truncated=False, tables=(TABLE,)):
    return ExecutedQuery(
        columns=tuple(columns),
        rows=tuple(tuple(row) for row in rows),
        truncated=truncated,
        tables=tables,
        stats=QueryExecutionStats(rows_read=20, bytes_read=1024, elapsed_ms=5),
        query_id="q-1",
        caveats=DATA_CAVEATS,
    )


class CannedTools:
    """Answers execute_query from a {sql: ExecutedQuery} map; everything else is fixed."""

    def __init__(self, results: dict[str, ExecutedQuery]) -> None:
        self.results = results
        self.calls: list[tuple[str, object]] = []

    async def list_tables(self):
        self.calls.append(("list_tables", None))
        return (TableSummary(table=TABLE, description="Daily snapshot."),)

    async def get_table_schema(self, table):
        self.calls.append(("get_table_schema", table))
        return TableSchema(
            table=table,
            columns=(ColumnSchema(name="price_usd", type="double"),),
            stats=TableStats(row_count=10, size_bytes=100, last_updated=NOW),
        )

    async def get_table_snapshots(self, table, *, limit=2):
        self.calls.append(("get_table_snapshots", table))
        return TableSnapshots(
            table=table,
            snapshots=(TableSnapshot(snapshot_id=42, committed_at=NOW, operation="append"),),
        )

    async def get_model_docs(self, model):
        self.calls.append(("get_model_docs", model))
        return ModelDocs(
            model="daily_snapshot",
            table=TABLE,
            description="Daily snapshot.",
            columns=(ColumnDocs(name="price_usd", description="Price."),),
            tests=(),
        )

    async def sample_rows(self, table, *, n, request_id, profile):
        raise AssertionError("not used")

    async def explain_query(self, sql, *, request_id, profile):
        self.calls.append(("explain_query", sql))
        return QueryExplanation(sql=sql, tables=(TABLE,), valid=sql in self.results)

    async def execute_query(self, sql, *, request_id, profile, max_rows=100):
        self.calls.append(("execute_query", sql))
        if sql not in self.results:
            raise ToolCallError("ENGINE_ERROR", f"no canned result for {sql}")
        return self.results[sql]


class CannedGateway:
    def __init__(self, tools: CannedTools) -> None:
        self.tools = tools

    @asynccontextmanager
    async def session(self):
        yield self.tools
