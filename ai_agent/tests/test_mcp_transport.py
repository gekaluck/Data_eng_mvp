"""Contract tests for shared MCP metadata tool registration and frontends."""

import asyncio
import json
from datetime import datetime, timezone

import pytest
from mcp.types import CallToolResult

from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.metadata_models import (
    ColumnDocs,
    ColumnSchema,
    LineageResult,
    ModelDocs,
    TableSchema,
    TableSnapshot,
    TableSnapshots,
    TableStats,
    TableSummary,
)
from ai_agent.mcp_server.transport import (
    HttpSettings,
    SERVER_NAME,
    create_mcp_server,
    parse_args,
)


TABLE = "gold.crypto_dbt.daily_snapshot"
TOOL_NAMES = {
    "list_tables",
    "get_table_schema",
    "get_table_snapshots",
    "get_lineage",
    "get_model_docs",
}


class FakeMetadataTools:
    def __init__(self):
        self.calls = []
        self.failure = None

    def _check(self, name, arguments):
        self.calls.append((name, arguments))
        if self.failure:
            raise self.failure

    def list_tables(self, *, schema=None, tag=None):
        self._check("list_tables", {"schema": schema, "tag": tag})
        return (
            TableSummary(table=TABLE, description="Daily snapshots", tags=("market",)),
        )

    def get_table_schema(self, table):
        self._check("get_table_schema", {"table": table})
        return TableSchema(
            table=table,
            columns=(ColumnSchema(name="snapshot_date", type="date"),),
            stats=TableStats(
                row_count=10,
                size_bytes=100,
                last_updated=datetime(2026, 8, 7, tzinfo=timezone.utc),
            ),
        )

    def get_table_snapshots(self, table, *, limit=10):
        self._check("get_table_snapshots", {"table": table, "limit": limit})
        return TableSnapshots(
            table=table,
            snapshots=(
                TableSnapshot(
                    snapshot_id=42,
                    committed_at=datetime(2026, 8, 7, tzinfo=timezone.utc),
                    operation="append",
                ),
            ),
        )

    def get_lineage(self, model, *, direction, depth=1):
        self._check(
            "get_lineage",
            {"model": model, "direction": direction, "depth": depth},
        )
        return LineageResult(
            model="daily_snapshot",
            direction=direction,
            depth=depth,
            nodes=(),
        )

    def get_model_docs(self, model):
        self._check("get_model_docs", {"model": model})
        return ModelDocs(
            model="daily_snapshot",
            table=TABLE,
            description="Daily snapshots",
            columns=(ColumnDocs(name="snapshot_date", description="UTC date"),),
            tests=(),
        )


def _server(fake=None):
    return create_mcp_server(fake or FakeMetadataTools(), http=HttpSettings())


def test_registers_exactly_five_read_only_tools_with_structured_schemas():
    server = _server()

    tools = asyncio.run(server.list_tools())

    assert server.name == SERVER_NAME
    assert {tool.name for tool in tools} == TOOL_NAMES
    for tool in tools:
        assert tool.outputSchema is not None
        assert tool.annotations.readOnlyHint is True
        assert tool.annotations.destructiveHint is False
        assert tool.annotations.idempotentHint is True
        assert tool.annotations.openWorldHint is False


def test_success_result_has_matching_text_and_structured_content():
    fake = FakeMetadataTools()

    result = asyncio.run(
        _server(fake).call_tool("list_tables", {"schema": "crypto_dbt"})
    )

    assert isinstance(result, CallToolResult)
    assert result.isError is False
    assert result.structuredContent["tables"][0]["table"] == TABLE
    assert json.loads(result.content[0].text) == result.structuredContent
    assert fake.calls == [
        ("list_tables", {"schema": "crypto_dbt", "tag": None})
    ]


def test_guardrail_failure_is_an_mcp_tool_error_with_exact_envelope():
    fake = FakeMetadataTools()
    fake.failure = GuardrailError(
        ErrorCode.TABLE_NOT_ALLOWED,
        "Outside the governed catalog.",
        hint="Call list_tables.",
    )

    result = asyncio.run(_server(fake).call_tool("get_model_docs", {"model": "hidden"}))

    assert isinstance(result, CallToolResult)
    assert result.isError is True
    assert result.structuredContent == {
        "code": "TABLE_NOT_ALLOWED",
        "message": "Outside the governed catalog.",
        "retryable": False,
        "hint": "Call list_tables.",
    }
    assert json.loads(result.content[0].text) == result.structuredContent


def test_tool_inputs_preserve_bounds_and_enums_in_mcp_schema():
    tools = {tool.name: tool for tool in asyncio.run(_server().list_tools())}

    snapshot_limit = tools["get_table_snapshots"].inputSchema["properties"]["limit"]
    lineage = tools["get_lineage"].inputSchema["properties"]

    assert snapshot_limit["minimum"] == 1
    assert snapshot_limit["maximum"] == 100
    assert lineage["direction"]["enum"] == ["upstream", "downstream"]
    assert lineage["depth"]["maximum"] == 5


@pytest.mark.parametrize(
    "settings",
    [
        {"host": "0.0.0.0"},
        {"port": 0},
        {"port": True},
        {"path": "mcp"},
        {"path": "/MCP"},
    ],
)
def test_http_settings_fail_closed(settings):
    with pytest.raises(ValueError):
        HttpSettings(**settings)


def test_http_server_is_stateless_json_and_dns_rebinding_protected():
    server = _server()

    assert server.settings.host == "127.0.0.1"
    assert server.settings.port == 8000
    assert server.settings.streamable_http_path == "/mcp"
    assert server.settings.stateless_http is True
    assert server.settings.json_response is True
    assert server.settings.transport_security.enable_dns_rebinding_protection is True
    assert server.settings.transport_security.allowed_hosts == [
        "127.0.0.1:*",
        "localhost:*",
        "[::1]:*",
    ]


def test_cli_defaults_to_stdio_and_accepts_streamable_http_overrides():
    assert parse_args([]).transport == "stdio"
    parsed = parse_args(
        [
            "--transport",
            "streamable-http",
            "--host",
            "localhost",
            "--port",
            "9000",
            "--path",
            "/catalog",
        ]
    )
    assert parsed.transport == "streamable-http"
    assert parsed.host == "localhost"
    assert parsed.port == 9000
    assert parsed.path == "/catalog"
