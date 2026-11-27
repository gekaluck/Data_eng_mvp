"""Fault injection at the agent's MCP client boundary, plus an in-memory trace sink.

Some failure classes the golden set must cover cannot be produced on demand by a healthy
local stack: a transient engine error, a schema change landing mid-request, a metadata
call dying halfway through EXPLORE. The harness produces them by wrapping the *client*
side of the MCP connection, so the real loop, the real model and the real Trino are all
still in play — only the chosen call is replaced by the failure it would have returned.

Nothing here touches the MCP server or its guardrails. An injected error is recorded so
the grader can tell "the agent hit a fault we planted" from "the agent misbehaved".
"""

import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from ai_agent.agent_service.mcp_tools import AgentTools, ToolCallError, ToolGateway
from ai_agent.agent_service.tracing import AgentTraceEntry, TraceLevel
from ai_agent.eval.cases import (
    DropColumnAtExecuteFault,
    Fault,
    StaleDocsColumnFault,
    ToolErrorFault,
)
from ai_agent.mcp_server.metadata_models import ColumnDocs, ModelDocs


class MemoryTraceSink:
    """Collect one request's full trace in memory for scoring; never touches disk."""

    level: TraceLevel = "full"

    def __init__(self) -> None:
        self.entries: list[AgentTraceEntry] = []

    def write(self, entry: AgentTraceEntry) -> None:
        self.entries.append(entry)


@dataclass
class FaultLog:
    """What was injected during one request, for the grader."""

    injected: list[dict[str, Any]] = field(default_factory=list)

    @property
    def triggered(self) -> bool:
        return bool(self.injected)


class FaultInjectingGateway:
    """Wrap a real gateway; every session it opens applies one optional fault."""

    def __init__(self, gateway: ToolGateway, fault: Fault | None) -> None:
        self._gateway = gateway
        self._fault = fault
        self.log = FaultLog()

    @asynccontextmanager
    async def session(self):
        async with self._gateway.session() as tools:
            if self._fault is None:
                yield tools
            else:
                yield FaultInjectingTools(tools, self._fault, self.log)


class FaultInjectingTools:
    """AgentTools proxy that replaces selected calls with their planted failure."""

    def __init__(self, tools: AgentTools, fault: Fault, log: FaultLog) -> None:
        self._tools = tools
        self._fault = fault
        self._log = log
        self._calls: dict[str, int] = {}

    def _count(self, tool: str) -> int:
        self._calls[tool] = self._calls.get(tool, 0) + 1
        return self._calls[tool]

    def _maybe_tool_error(self, tool: str) -> None:
        call = self._count(tool)
        fault = self._fault
        if (
            isinstance(fault, ToolErrorFault)
            and fault.tool == tool
            and call in fault.on_calls
        ):
            self._log.injected.append({"tool": tool, "call": call, "code": fault.code})
            raise ToolCallError(fault.code, fault.message, retryable=fault.retryable)

    async def list_tables(self):
        self._maybe_tool_error("list_tables")
        return await self._tools.list_tables()

    async def get_table_schema(self, table: str):
        self._maybe_tool_error("get_table_schema")
        return await self._tools.get_table_schema(table)

    async def get_table_snapshots(self, table: str, *, limit: int = 2):
        self._maybe_tool_error("get_table_snapshots")
        return await self._tools.get_table_snapshots(table, limit=limit)

    async def get_model_docs(self, model: str) -> ModelDocs:
        self._maybe_tool_error("get_model_docs")
        docs = await self._tools.get_model_docs(model)
        fault = self._fault
        if (
            isinstance(fault, StaleDocsColumnFault)
            and docs.table.casefold() == fault.table.casefold()
        ):
            self._log.injected.append(
                {"tool": "get_model_docs", "stale_column": fault.column}
            )
            docs = docs.model_copy(
                update={
                    "columns": docs.columns
                    + (ColumnDocs(name=fault.column, description=fault.description),)
                }
            )
        return docs

    async def sample_rows(self, table: str, *, n: int, request_id: str, profile):
        self._maybe_tool_error("sample_rows")
        return await self._tools.sample_rows(
            table, n=n, request_id=request_id, profile=profile
        )

    async def explain_query(self, sql: str, *, request_id: str, profile):
        self._maybe_tool_error("explain_query")
        return await self._tools.explain_query(
            sql, request_id=request_id, profile=profile
        )

    async def execute_query(
        self, sql: str, *, request_id: str, profile, max_rows: int = 100
    ):
        self._maybe_tool_error("execute_query")
        fault = self._fault
        if isinstance(fault, DropColumnAtExecuteFault) and _mentions(sql, fault.column):
            self._log.injected.append(
                {"tool": "execute_query", "dropped_column": fault.column}
            )
            raise ToolCallError(
                "ENGINE_ERROR",
                f"COLUMN_NOT_FOUND: Column '{fault.column}' cannot be resolved",
                retryable=False,
            )
        return await self._tools.execute_query(
            sql, request_id=request_id, profile=profile, max_rows=max_rows
        )


def _mentions(sql: str, column: str) -> bool:
    return re.search(rf"\b{re.escape(column)}\b", sql, flags=re.IGNORECASE) is not None
