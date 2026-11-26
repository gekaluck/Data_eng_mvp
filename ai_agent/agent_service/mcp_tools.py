"""Typed client boundary from the owned loop to the governed MCP HTTP server."""

import os
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import Any, Protocol, TypeVar
from urllib.parse import urlparse

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from pydantic import BaseModel

from ai_agent.agent_service.tracing import AgentTrace, elapsed_ms
from ai_agent.mcp_server.budget import BudgetProfile
from ai_agent.mcp_server.metadata_models import (
    ModelDocs,
    TableSchema,
    TableSnapshots,
    TableSummary,
)
from ai_agent.mcp_server.query_executor import ExecutedQuery
from ai_agent.mcp_server.query_explainer import QueryExplanation
from ai_agent.mcp_server.query_sampler import SampleRows

DEFAULT_MCP_URL = "http://127.0.0.1:8000/mcp"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
ModelT = TypeVar("ModelT", bound=BaseModel)
ToolResultT = TypeVar("ToolResultT")


class ToolCallError(Exception):
    """Structured MCP error retained for loop retry/refusal decisions."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        hint: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.hint = hint


class AgentTools(Protocol):
    """Only the MCP operations needed by the current state machine."""

    async def list_tables(self) -> tuple[TableSummary, ...]: ...

    async def get_table_schema(self, table: str) -> TableSchema: ...

    async def get_table_snapshots(
        self, table: str, *, limit: int = 2
    ) -> TableSnapshots: ...

    async def get_model_docs(self, model: str) -> ModelDocs: ...

    async def sample_rows(
        self,
        table: str,
        *,
        n: int,
        request_id: str,
        profile: BudgetProfile,
    ) -> SampleRows: ...

    async def explain_query(
        self,
        sql: str,
        *,
        request_id: str,
        profile: BudgetProfile,
    ) -> QueryExplanation: ...

    async def execute_query(
        self,
        sql: str,
        *,
        request_id: str,
        profile: BudgetProfile,
        max_rows: int = 100,
    ) -> ExecutedQuery: ...


class ToolGateway(Protocol):
    """Open one initialized MCP session for a complete question."""

    def session(self) -> Any: ...


class McpToolSession:
    """Validate MCP success envelopes before converting them to typed models."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session

    async def list_tables(self) -> tuple[TableSummary, ...]:
        payload = await self._call("list_tables", {})
        tables = payload.get("tables")
        if not isinstance(tables, list):
            raise self._shape_error("list_tables")
        try:
            return tuple(TableSummary.model_validate(table) for table in tables)
        except Exception as exc:
            raise self._shape_error("list_tables") from exc

    async def get_table_schema(self, table: str) -> TableSchema:
        return self._parse(
            "get_table_schema",
            TableSchema,
            await self._call("get_table_schema", {"table": table}),
        )

    async def get_table_snapshots(
        self, table: str, *, limit: int = 2
    ) -> TableSnapshots:
        return self._parse(
            "get_table_snapshots",
            TableSnapshots,
            await self._call("get_table_snapshots", {"table": table, "limit": limit}),
        )

    async def get_model_docs(self, model: str) -> ModelDocs:
        return self._parse(
            "get_model_docs",
            ModelDocs,
            await self._call("get_model_docs", {"model": model}),
        )

    async def sample_rows(
        self,
        table: str,
        *,
        n: int,
        request_id: str,
        profile: BudgetProfile,
    ) -> SampleRows:
        return self._parse(
            "sample_rows",
            SampleRows,
            await self._call(
                "sample_rows",
                {
                    "table": table,
                    "n": n,
                    "request_id": request_id,
                    "profile": profile,
                },
            )
        )

    async def explain_query(
        self,
        sql: str,
        *,
        request_id: str,
        profile: BudgetProfile,
    ) -> QueryExplanation:
        return self._parse(
            "explain_query",
            QueryExplanation,
            await self._call(
                "explain_query",
                {"sql": sql, "request_id": request_id, "profile": profile},
            )
        )

    async def execute_query(
        self,
        sql: str,
        *,
        request_id: str,
        profile: BudgetProfile,
        max_rows: int = 100,
    ) -> ExecutedQuery:
        return self._parse(
            "execute_query",
            ExecutedQuery,
            await self._call(
                "execute_query",
                {
                    "sql": sql,
                    "request_id": request_id,
                    "profile": profile,
                    "max_rows": max_rows,
                },
            )
        )

    async def _call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        try:
            result = await self._session.call_tool(name, arguments=arguments)
        except ToolCallError:
            raise
        except Exception as exc:
            raise ToolCallError(
                "ENGINE_ERROR",
                f"MCP tool {name} could not be reached: {exc}",
                retryable=True,
            ) from exc
        payload = result.structuredContent
        if result.isError:
            if not isinstance(payload, dict):
                raise self._shape_error(name)
            raise ToolCallError(
                str(payload.get("code") or "ENGINE_ERROR"),
                str(payload.get("message") or f"MCP tool {name} failed."),
                retryable=payload.get("retryable") is True,
                hint=(
                    str(payload["hint"]) if payload.get("hint") is not None else None
                ),
            )
        if not isinstance(payload, dict):
            raise self._shape_error(name)
        return payload

    @staticmethod
    def _parse(name: str, model: type[ModelT], payload: dict[str, Any]) -> ModelT:
        try:
            return model.model_validate(payload)
        except Exception as exc:
            raise McpToolSession._shape_error(name) from exc

    @staticmethod
    def _shape_error(name: str) -> ToolCallError:
        return ToolCallError(
            "ENGINE_ERROR",
            f"MCP tool {name} returned an incompatible structured payload.",
            retryable=False,
        )


class TracingAgentTools:
    """Record every loop-visible MCP call without changing the governed tool client."""

    def __init__(self, tools: AgentTools, trace: AgentTrace) -> None:
        self._tools = tools
        self._trace = trace

    async def list_tables(self) -> tuple[TableSummary, ...]:
        return await self._call("list_tables", {}, self._tools.list_tables())

    async def get_table_schema(self, table: str) -> TableSchema:
        return await self._call(
            "get_table_schema",
            {"table": table},
            self._tools.get_table_schema(table),
        )

    async def get_table_snapshots(
        self, table: str, *, limit: int = 2
    ) -> TableSnapshots:
        return await self._call(
            "get_table_snapshots",
            {"table": table, "limit": limit},
            self._tools.get_table_snapshots(table, limit=limit),
        )

    async def get_model_docs(self, model: str) -> ModelDocs:
        return await self._call(
            "get_model_docs",
            {"model": model},
            self._tools.get_model_docs(model),
        )

    async def sample_rows(
        self,
        table: str,
        *,
        n: int,
        request_id: str,
        profile: BudgetProfile,
    ) -> SampleRows:
        arguments = {
            "table": table,
            "n": n,
            "request_id": request_id,
            "profile": profile,
        }
        return await self._call(
            "sample_rows",
            arguments,
            self._tools.sample_rows(table, n=n, request_id=request_id, profile=profile),
        )

    async def explain_query(
        self,
        sql: str,
        *,
        request_id: str,
        profile: BudgetProfile,
    ) -> QueryExplanation:
        arguments = {"sql": sql, "request_id": request_id, "profile": profile}
        return await self._call(
            "explain_query",
            arguments,
            self._tools.explain_query(sql, request_id=request_id, profile=profile),
        )

    async def execute_query(
        self,
        sql: str,
        *,
        request_id: str,
        profile: BudgetProfile,
        max_rows: int = 100,
    ) -> ExecutedQuery:
        arguments = {
            "sql": sql,
            "request_id": request_id,
            "profile": profile,
            "max_rows": max_rows,
        }
        return await self._call(
            "execute_query",
            arguments,
            self._tools.execute_query(
                sql,
                request_id=request_id,
                profile=profile,
                max_rows=max_rows,
            ),
        )

    async def _call(
        self,
        name: str,
        arguments: dict[str, Any],
        operation: Awaitable[ToolResultT],
    ) -> ToolResultT:
        started = self._trace.now()
        self._trace.event(
            "tool.started",
            status="started",
            stage="mcp",
            metadata={"tool": name},
            content={"arguments": arguments},
        )
        try:
            result = await operation
        except Exception as exc:
            metadata = {"tool": name, "error_type": type(exc).__name__}
            if isinstance(exc, ToolCallError):
                metadata.update(
                    {
                        "error_code": exc.code,
                        "retryable": exc.retryable,
                    }
                )
            self._trace.event(
                "tool.completed",
                status="failed",
                stage="mcp",
                duration_ms=elapsed_ms(started, self._trace.now),
                metadata=metadata,
                content={"arguments": arguments, "error": exc},
            )
            raise
        self._trace.event(
            "tool.completed",
            status="succeeded",
            stage="mcp",
            duration_ms=elapsed_ms(started, self._trace.now),
            metadata={"tool": name, **_tool_summary(result)},
            content={"arguments": arguments, "result": result},
        )
        return result


def _tool_summary(result: Any) -> dict[str, Any]:
    summary: dict[str, Any] = {"result_type": type(result).__name__}
    rows = getattr(result, "rows", None)
    if rows is not None:
        summary["row_count"] = len(rows)
    columns = getattr(result, "columns", None)
    if columns is not None:
        summary["column_count"] = len(columns)
    query_id = getattr(result, "query_id", None)
    if query_id:
        summary["query_id"] = query_id
    truncated = getattr(result, "truncated", None)
    if truncated is not None:
        summary["truncated"] = truncated
    if isinstance(result, tuple):
        summary["item_count"] = len(result)
    return summary


class McpHttpGateway:
    """Create per-question MCP sessions against the local Streamable HTTP endpoint."""

    def __init__(self, url: str = DEFAULT_MCP_URL) -> None:
        parsed = urlparse(url)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname not in _LOOPBACK_HOSTS
        ):
            raise ValueError(
                "AI agent MCP URL must use HTTP(S) on a loopback host; remote MCP "
                "requires a separate authentication design."
            )
        if not parsed.path:
            raise ValueError("AI agent MCP URL must include the MCP endpoint path.")
        self._url = url

    @classmethod
    def from_env(cls) -> "McpHttpGateway":
        return cls(os.getenv("AI_MCP_URL", DEFAULT_MCP_URL))

    @asynccontextmanager
    async def session(self) -> AsyncIterator[McpToolSession]:
        async with streamable_http_client(self._url) as streams:
            read_stream, write_stream, _ = streams
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                yield McpToolSession(session)
