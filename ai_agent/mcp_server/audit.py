"""Append-only local JSONL audit records for business-data reads."""

import json
import os
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Literal, Protocol

from ai_agent.mcp_server.budget import BudgetProfile
from ai_agent.mcp_server.errors import ErrorCode
from ai_agent.mcp_server.metadata_models import MetadataModel

DEFAULT_AUDIT_PATH = Path("ai_agent/runtime/query-audit.jsonl")


class SampleAuditEntry(MetadataModel):
    """One sample_rows attempt; raw business-row values are never persisted."""

    timestamp: datetime
    client: str
    request_id: str
    profile: BudgetProfile
    tool: Literal["sample_rows"] = "sample_rows"
    table: str
    sql: str | None = None
    validation_verdict: Literal["allowed", "denied", "not_run"]
    outcome: Literal["success", "error"]
    columns: tuple[str, ...] = ()
    row_count: int = 0
    elapsed_ms: int
    failure_code: ErrorCode | None = None


class ExecuteAuditEntry(MetadataModel):
    """One execute_query attempt; raw business-row values are never persisted."""

    timestamp: datetime
    client: str
    request_id: str
    profile: BudgetProfile
    tool: Literal["execute_query"] = "execute_query"
    sql: str
    tables: tuple[str, ...] = ()
    validation_verdict: Literal["allowed", "denied", "not_run"]
    outcome: Literal["success", "error"]
    columns: tuple[str, ...] = ()
    row_count: int = 0
    truncated: bool = False
    query_id: str | None = None
    rows_read: int = 0
    bytes_read: int = 0
    elapsed_ms: int
    failure_code: ErrorCode | None = None


class AuditWriteError(Exception):
    """The required audit record could not be persisted."""


class AuditSink(Protocol):
    """Minimum append interface used by governed data-reading tools."""

    def write(self, entry: SampleAuditEntry | ExecuteAuditEntry) -> None: ...


class JsonlAuditLog:
    """Serialize each audit record as one compact append-only JSON line."""

    def __init__(self, path: str | Path = DEFAULT_AUDIT_PATH) -> None:
        self.path = Path(path)
        self._lock = Lock()

    @classmethod
    def from_env(cls) -> "JsonlAuditLog":
        return cls(os.getenv("AI_AUDIT_LOG_PATH", str(DEFAULT_AUDIT_PATH)))

    def write(self, entry: SampleAuditEntry | ExecuteAuditEntry) -> None:
        line = json.dumps(
            entry.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        try:
            with self._lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                    handle.write(line + "\n")
        except OSError as exc:
            raise AuditWriteError(
                f"Could not append required audit record to '{self.path}': {exc}"
            ) from exc
