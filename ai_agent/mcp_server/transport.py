"""MCP frontends for governed metadata, planning, and capped row sampling."""

import argparse
import json
import os
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Annotated, Literal, TypeVar

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import BaseModel, Field, RootModel

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.audit import JsonlAuditLog
from ai_agent.mcp_server.budget import (
    MAX_REQUEST_ID_CHARS,
    REQUEST_ID_PATTERN,
    BudgetProfile,
    RequestBudgetManager,
)
from ai_agent.mcp_server.dbt_artifacts import DbtArtifactAdapter
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.metadata_models import (
    LineageResult,
    ModelDocs,
    TableSchema,
    TableSnapshots,
    TableSummary,
)
from ai_agent.mcp_server.metadata_tools import MetadataTools
from ai_agent.mcp_server.query_explainer import (
    MAX_SQL_CHARS,
    QueryExplainer,
    QueryExplanation,
)
from ai_agent.mcp_server.query_sampler import MAX_SAMPLE_ROWS, QuerySampler, SampleRows
from ai_agent.mcp_server.trino_metadata import (
    IcebergMetadataAdapter,
    TrinoDbApiRunner,
)

SERVER_NAME = "crypto-lakehouse-metadata"
# A fully qualified allow-listed name is short; bound the input so an oversized value
# cannot reach the sampler's audit sink from the MCP boundary.
MAX_TABLE_NAME_CHARS = 512
DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8000
DEFAULT_HTTP_PATH = "/mcp"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_HTTP_PATH = re.compile(r"^/[a-z0-9/_-]*$")
Transport = Literal["stdio", "streamable-http"]
PayloadT = TypeVar("PayloadT", bound=BaseModel)


class ToolErrorPayload(BaseModel):
    """The stable structured error exposed to MCP clients."""

    code: ErrorCode
    message: str
    retryable: bool
    hint: str | None = None


class TableListPayload(BaseModel):
    """Object wrapper required by MCP structuredContent."""

    tables: tuple[TableSummary, ...]


class ListTablesOutput(RootModel[TableListPayload | ToolErrorPayload]):
    """Advertised output schema for list_tables."""


class TableSchemaOutput(RootModel[TableSchema | ToolErrorPayload]):
    """Advertised output schema for get_table_schema."""


class TableSnapshotsOutput(RootModel[TableSnapshots | ToolErrorPayload]):
    """Advertised output schema for get_table_snapshots."""


class LineageOutput(RootModel[LineageResult | ToolErrorPayload]):
    """Advertised output schema for get_lineage."""


class ModelDocsOutput(RootModel[ModelDocs | ToolErrorPayload]):
    """Advertised output schema for get_model_docs."""


class ExplainQueryOutput(RootModel[QueryExplanation | ToolErrorPayload]):
    """Advertised output schema for explain_query."""


class SampleRowsOutput(RootModel[SampleRows | ToolErrorPayload]):
    """Advertised output schema for sample_rows."""


READ_ONLY_ANNOTATIONS = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)


@dataclass(frozen=True, slots=True)
class HttpSettings:
    """Validated local-only settings for Streamable HTTP."""

    host: str = DEFAULT_HTTP_HOST
    port: int = DEFAULT_HTTP_PORT
    path: str = DEFAULT_HTTP_PATH

    def __post_init__(self) -> None:
        if self.host not in _LOOPBACK_HOSTS:
            raise ValueError(
                "AI MCP HTTP currently supports loopback hosts only; remote exposure "
                "requires an authentication design."
            )
        if not isinstance(self.port, int) or isinstance(self.port, bool):
            raise ValueError("AI MCP HTTP port must be an integer.")
        if not 1 <= self.port <= 65535:
            raise ValueError("AI MCP HTTP port must be between 1 and 65535.")
        if not isinstance(self.path, str) or not _HTTP_PATH.fullmatch(self.path):
            raise ValueError(
                "AI MCP HTTP path must start with '/' and contain only lowercase "
                "letters, digits, '/', '_' or '-'."
            )

    @classmethod
    def from_env(cls) -> "HttpSettings":
        """Load non-secret local transport settings from environment variables."""
        try:
            port = int(os.getenv("AI_MCP_PORT", str(DEFAULT_HTTP_PORT)))
        except ValueError as exc:
            raise ValueError("AI_MCP_PORT must be an integer.") from exc
        return cls(
            host=os.getenv("AI_MCP_HOST", DEFAULT_HTTP_HOST),
            port=port,
            path=os.getenv("AI_MCP_PATH", DEFAULT_HTTP_PATH),
        )


def build_metadata_tools() -> MetadataTools:
    """Compose production adapters without starting either transport."""
    allow_list = TableAllowList.from_file()
    return MetadataTools(
        DbtArtifactAdapter.from_files(allow_list),
        IcebergMetadataAdapter(TrinoDbApiRunner.from_env(), allow_list),
    )


def build_query_tools() -> tuple[QueryExplainer, QuerySampler]:
    """Compose planning/sampling around one shared process-local budget."""
    allow_list = TableAllowList.from_file()
    budget = RequestBudgetManager()
    return (
        QueryExplainer(
            TrinoDbApiRunner.from_env(source="ai-explain-query"),
            allow_list,
            budget,
        ),
        QuerySampler(
            TrinoDbApiRunner.from_env(source="ai-sample-rows"),
            allow_list,
            budget,
            JsonlAuditLog.from_env(),
        ),
    )


def create_mcp_server(
    metadata_tools: MetadataTools | None = None,
    *,
    query_explainer: QueryExplainer | None = None,
    query_sampler: QuerySampler | None = None,
    http: HttpSettings | None = None,
) -> FastMCP:
    """Create one MCP server whose tools are identical on both transports."""
    tools = metadata_tools or build_metadata_tools()
    if (query_explainer is None) != (query_sampler is None):
        raise ValueError(
            "query_explainer and query_sampler must be supplied together so they "
            "share one request budget; pass both or neither."
        )
    if query_explainer is None:
        query_explainer, query_sampler = build_query_tools()
    settings = http or HttpSettings.from_env()
    transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*"],
        allowed_origins=[
            "http://127.0.0.1:*",
            "http://localhost:*",
            "http://[::1]:*",
        ],
    )
    server = FastMCP(
        SERVER_NAME,
        instructions=(
            "Read-only metadata, scan-free SQL planning, and capped audited samples "
            "for the explicitly allow-listed dbt Gold catalog. This server never "
            "executes caller-supplied SQL."
        ),
        host=settings.host,
        port=settings.port,
        streamable_http_path=settings.path,
        stateless_http=True,
        json_response=True,
        transport_security=transport_security,
    )
    register_metadata_tools(server, tools)
    register_query_tools(server, query_explainer, query_sampler)
    return server


def register_metadata_tools(server: FastMCP, tools: MetadataTools) -> None:
    """Register the five metadata methods once for every MCP transport."""

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def list_tables(
        schema: Annotated[
            str | None,
            Field(description="Optional schema or catalog.schema filter."),
        ] = None,
        tag: Annotated[
            str | None,
            Field(description="Optional exact dbt tag filter."),
        ] = None,
    ) -> Annotated[CallToolResult, ListTablesOutput]:
        """List allow-listed dbt Gold tables and their semantic descriptions."""
        return _invoke(
            lambda: TableListPayload(
                tables=tools.list_tables(schema=schema, tag=tag)
            )
        )

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def get_table_schema(
        table: Annotated[
            str,
            Field(min_length=1, description="Fully qualified allow-listed table."),
        ],
    ) -> Annotated[CallToolResult, TableSchemaOutput]:
        """Get authoritative live Iceberg columns, layout, stats, and dbt annotations."""
        return _invoke(lambda: tools.get_table_schema(table))

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def get_table_snapshots(
        table: Annotated[
            str,
            Field(min_length=1, description="Fully qualified allow-listed table."),
        ],
        limit: Annotated[
            int,
            Field(ge=1, le=100, description="Maximum recent snapshots to return."),
        ] = 10,
    ) -> Annotated[CallToolResult, TableSnapshotsOutput]:
        """Get bounded newest-first Iceberg snapshot history for freshness checks."""
        return _invoke(lambda: tools.get_table_snapshots(table, limit=limit))

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def get_lineage(
        model: Annotated[
            str,
            Field(min_length=1, description="Allow-listed dbt model or table name."),
        ],
        direction: Annotated[
            Literal["upstream", "downstream"],
            Field(description="Direction to traverse the dbt dependency graph."),
        ],
        depth: Annotated[
            int,
            Field(ge=1, le=5, description="Maximum dependency hops."),
        ] = 1,
    ) -> Annotated[CallToolResult, LineageOutput]:
        """Get bounded upstream or downstream dbt models and sources."""
        return _invoke(
            lambda: tools.get_lineage(
                model,
                direction=direction,
                depth=depth,
            )
        )

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def get_model_docs(
        model: Annotated[
            str,
            Field(min_length=1, description="Allow-listed dbt model or table name."),
        ],
    ) -> Annotated[CallToolResult, ModelDocsOutput]:
        """Get dbt model/column descriptions and declared tests."""
        return _invoke(lambda: tools.get_model_docs(model))


def register_query_tools(
    server: FastMCP,
    explainer: QueryExplainer,
    sampler: QuerySampler,
) -> None:
    """Register budgeted planning/sampling separately from future arbitrary execution."""

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def explain_query(
        sql: Annotated[
            str,
            Field(
                min_length=1,
                max_length=MAX_SQL_CHARS,
                description=(
                    "One fully qualified, allow-listed Trino SELECT to validate and "
                    "plan without execution."
                ),
            ),
        ],
        request_id: Annotated[
            str,
            Field(
                min_length=1,
                max_length=MAX_REQUEST_ID_CHARS,
                pattern=REQUEST_ID_PATTERN,
                description="Stable ID for one natural-language question.",
            ),
        ],
        profile: Annotated[
            BudgetProfile,
            Field(description="Per-request engine-call budget profile."),
        ],
    ) -> Annotated[CallToolResult, ExplainQueryOutput]:
        """Validate SQL and return a bounded distributed plan without scanning rows."""
        return _invoke(
            lambda: explainer.explain_query(
                sql,
                request_id=request_id,
                profile=profile,
            )
        )

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def sample_rows(
        table: Annotated[
            str,
            Field(
                min_length=1,
                max_length=MAX_TABLE_NAME_CHARS,
                description="Fully qualified allow-listed table.",
            ),
        ],
        n: Annotated[
            int,
            Field(
                ge=1,
                le=MAX_SAMPLE_ROWS,
                description="Number of rows to return (maximum 20).",
            ),
        ],
        request_id: Annotated[
            str,
            Field(
                min_length=1,
                max_length=MAX_REQUEST_ID_CHARS,
                pattern=REQUEST_ID_PATTERN,
                description="Stable ID for one natural-language question.",
            ),
        ],
        profile: Annotated[
            BudgetProfile,
            Field(description="Per-request engine-call budget profile."),
        ],
    ) -> Annotated[CallToolResult, SampleRowsOutput]:
        """Return at most 20 rows through a server-built, audited Gold SELECT."""
        return _invoke(
            lambda: sampler.sample_rows(
                table,
                n=n,
                request_id=request_id,
                profile=profile,
            )
        )


def _invoke(operation: Callable[[], PayloadT]) -> CallToolResult:
    try:
        return _result(operation(), is_error=False)
    except GuardrailError as exc:
        return _result(ToolErrorPayload(**exc.as_dict()), is_error=True)


def _result(payload: BaseModel, *, is_error: bool) -> CallToolResult:
    structured = payload.model_dump(mode="json")
    return CallToolResult(
        content=[
            TextContent(
                type="text",
                text=json.dumps(structured, sort_keys=True, separators=(",", ":")),
            )
        ],
        structuredContent=structured,
        isError=is_error,
    )


def run_server(
    transport: Transport,
    *,
    metadata_tools: MetadataTools | None = None,
    query_explainer: QueryExplainer | None = None,
    query_sampler: QuerySampler | None = None,
    http: HttpSettings | None = None,
) -> None:
    """Run the same registered server over stdio or local Streamable HTTP."""
    server = create_mcp_server(
        metadata_tools,
        query_explainer=query_explainer,
        query_sampler=query_sampler,
        http=http,
    )
    server.run(transport=transport)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the governed AI MCP server.")
    parser.add_argument(
        "--transport",
        choices=("stdio", "streamable-http"),
        default="stdio",
        help="MCP transport (default: stdio).",
    )
    parser.add_argument("--host", help="Loopback HTTP bind host override.")
    parser.add_argument("--port", type=int, help="HTTP port override.")
    parser.add_argument("--path", help="Streamable HTTP endpoint path override.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    defaults = HttpSettings.from_env()
    http = HttpSettings(
        host=args.host or defaults.host,
        port=args.port if args.port is not None else defaults.port,
        path=args.path or defaults.path,
    )
    run_server(args.transport, http=http)
