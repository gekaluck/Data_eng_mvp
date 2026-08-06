"""Read live schema, snapshot, and file statistics from Trino/Iceberg metadata."""

import os
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.metadata_models import (
    ColumnSchema,
    TableSchema,
    TableSnapshot,
    TableSnapshots,
    TableStats,
)


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Small transport-neutral result returned by a metadata query runner."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


class QueryRunner(Protocol):
    """Minimum query interface needed by the Iceberg metadata adapter."""

    def execute(self, sql: str) -> QueryResult: ...


class TrinoDbApiRunner:
    """Trino DB-API runner pinned to the restricted `agent` identity."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        http_scheme: str = "http",
        request_timeout_seconds: float = 15.0,
    ) -> None:
        self._host = host
        self._port = port
        self._http_scheme = http_scheme
        self._request_timeout_seconds = request_timeout_seconds

    @classmethod
    def from_env(cls) -> "TrinoDbApiRunner":
        """Build local defaults overridable without changing the security identity."""
        try:
            port = int(os.getenv("AI_TRINO_PORT", "8081"))
            timeout = float(os.getenv("AI_TRINO_REQUEST_TIMEOUT_SECONDS", "15"))
        except ValueError as exc:
            raise ValueError("AI Trino port and timeout must be numeric.") from exc
        return cls(
            host=os.getenv("AI_TRINO_HOST", "localhost"),
            port=port,
            http_scheme=os.getenv("AI_TRINO_SCHEME", "http"),
            request_timeout_seconds=timeout,
        )

    def execute(self, sql: str) -> QueryResult:
        """Execute one adapter-owned statement and fully materialize its small result."""
        from trino.dbapi import connect

        connection = connect(
            host=self._host,
            port=self._port,
            user="agent",
            source="ai-metadata-tools",
            http_scheme=self._http_scheme,
            request_timeout=self._request_timeout_seconds,
        )
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            rows = tuple(tuple(row) for row in cursor.fetchall())
            columns = tuple(item[0] for item in (cursor.description or ()))
            return QueryResult(columns=columns, rows=rows)
        finally:
            connection.close()


class IcebergMetadataAdapter:
    """Issue only fixed-shape metadata queries for allow-listed Iceberg tables."""

    def __init__(self, runner: QueryRunner, allow_list: TableAllowList) -> None:
        self._runner = runner
        self._allow_list = allow_list

    def get_table_schema(self, table: str) -> TableSchema:
        """Return live columns, physical layout, and current-snapshot file stats."""
        normalized = self._allowed_table(table)
        relation = self._quoted_relation(normalized)
        columns_result = self._execute(f"DESCRIBE {relation}")
        files_relation = self._metadata_relation(normalized, "files")
        files_schema = self._execute(f"DESCRIBE {files_relation}")
        stats_result = self._execute(
            "SELECT coalesce(sum(CASE WHEN content = 0 THEN record_count "
            "ELSE -record_count END), 0) AS row_count, "
            "coalesce(sum(file_size_in_bytes), 0) AS size_bytes "
            f"FROM {files_relation}"
        )
        latest_result = self._execute(
            "SELECT committed_at "
            f"FROM {self._metadata_relation(normalized, 'snapshots')} "
            "ORDER BY committed_at DESC LIMIT 1"
        )
        sort_result = self._execute(
            f"SELECT DISTINCT sort_order_id FROM {files_relation} "
            "WHERE content = 0 ORDER BY sort_order_id"
        )

        with self._validated_response():
            columns = tuple(self._column(row) for row in columns_result.rows)
            partition_spec = self._partition_spec(files_schema)
            sort_ids = tuple(
                int(row[0]) for row in sort_result.rows if row and row[0] is not None
            )
            warnings: list[str] = []
            sort_order: tuple[str, ...] = ()
            if any(sort_id != 0 for sort_id in sort_ids):
                warnings.append(
                    "Iceberg reports a non-default sort-order ID, but Trino's read-only "
                    "metadata tables do not expose its column expressions."
                )

            row_count, size_bytes = self._single_stats_row(stats_result)
            last_updated = None
            if latest_result.rows and latest_result.rows[0]:
                last_updated = latest_result.rows[0][0]
            return TableSchema(
                table=normalized,
                columns=columns,
                partition_spec=partition_spec,
                sort_order=sort_order,
                stats=TableStats(
                    row_count=row_count,
                    size_bytes=size_bytes,
                    last_updated=last_updated,
                ),
                warnings=tuple(warnings),
            )

    def get_table_snapshots(self, table: str, *, limit: int = 10) -> TableSnapshots:
        """Return a bounded newest-first snapshot history."""
        normalized = self._allowed_table(table)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                "Snapshot limit must be an integer from 1 through 100.",
                hint="Request only the recent history needed for the freshness check.",
            )
        result = self._execute(
            "SELECT snapshot_id, committed_at, operation, summary "
            f"FROM {self._metadata_relation(normalized, 'snapshots')} "
            f"ORDER BY committed_at DESC LIMIT {limit}"
        )
        with self._validated_response():
            snapshots = tuple(
                TableSnapshot(
                    snapshot_id=int(row[0]),
                    committed_at=row[1],
                    operation=str(row[2]),
                    summary=self._summary(row[3]),
                )
                for row in result.rows
            )
            return TableSnapshots(table=normalized, snapshots=snapshots)

    def _execute(self, sql: str) -> QueryResult:
        try:
            return self._runner.execute(sql)
        except GuardrailError:
            raise
        except Exception as exc:
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                f"Trino metadata query failed: {exc}",
                retryable=True,
                hint="Check Trino health, the restricted agent access, and Iceberg metadata.",
            ) from exc

    @staticmethod
    @contextmanager
    def _validated_response() -> Iterator[None]:
        """Turn incompatible engine result shapes into the structured tool contract."""
        try:
            yield
        except GuardrailError:
            raise
        except Exception as exc:
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                f"Trino returned incompatible metadata: {exc}",
                retryable=False,
                hint="Check the Trino/Iceberg version and metadata-table schema.",
            ) from exc

    def _allowed_table(self, table: str) -> str:
        if not isinstance(table, str) or not table.strip():
            raise GuardrailError(ErrorCode.PARSE_ERROR, "Table must be a non-empty string.")
        normalized = table.strip().casefold()
        if not self._allow_list.allows(normalized):
            raise GuardrailError(
                ErrorCode.TABLE_NOT_ALLOWED,
                f"Table '{table}' is not in the explicit Gold allow-list.",
                hint="Call list_tables and use a fully qualified returned table name.",
            )
        return normalized

    @staticmethod
    def _quoted_relation(table: str) -> str:
        return ".".join(f'"{part}"' for part in table.split("."))

    @classmethod
    def _metadata_relation(cls, table: str, suffix: str) -> str:
        catalog, schema, name = table.split(".")
        return f'"{catalog}"."{schema}"."{name}${suffix}"'

    @staticmethod
    def _column(row: Sequence[Any]) -> ColumnSchema:
        if len(row) < 2:
            raise ValueError("DESCRIBE returned a malformed column row.")
        extra = str(row[2] or "") if len(row) > 2 else ""
        nullable = False if "not null" in extra.casefold() else None
        comment = str(row[3]) if len(row) > 3 and row[3] is not None else None
        return ColumnSchema(
            name=str(row[0]),
            type=str(row[1]),
            comment=comment,
            nullable=nullable,
        )

    @staticmethod
    def _single_stats_row(result: QueryResult) -> tuple[int, int]:
        if len(result.rows) != 1 or len(result.rows[0]) < 2:
            raise ValueError("Iceberg file statistics returned an unexpected shape.")
        return int(result.rows[0][0]), int(result.rows[0][1])

    @staticmethod
    def _partition_spec(files_schema: QueryResult) -> tuple[str, ...]:
        partition_type = next(
            (
                str(row[1])
                for row in files_schema.rows
                if len(row) >= 2 and str(row[0]).casefold() == "partition"
            ),
            None,
        )
        if partition_type is None or not partition_type.casefold().startswith("row("):
            return ()
        body = partition_type[4:-1]
        fields = IcebergMetadataAdapter._split_top_level(body)
        names: list[str] = []
        for field in fields:
            match = re.match(r'^\s*(?:"((?:[^"]|"")+)"|([^\s]+))\s+', field)
            if match:
                names.append((match.group(1) or match.group(2)).replace('""', '"'))
        return tuple(names)

    @staticmethod
    def _split_top_level(value: str) -> tuple[str, ...]:
        depth = 0
        quoted = False
        start = 0
        parts: list[str] = []
        index = 0
        while index < len(value):
            char = value[index]
            if char == '"':
                if quoted and index + 1 < len(value) and value[index + 1] == '"':
                    index += 2
                    continue
                quoted = not quoted
            elif not quoted:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                elif char == "," and depth == 0:
                    parts.append(value[start:index])
                    start = index + 1
            index += 1
        if value[start:].strip():
            parts.append(value[start:])
        return tuple(parts)

    @staticmethod
    def _summary(value: Any) -> dict[str, str]:
        if value is None:
            return {}
        if isinstance(value, Mapping):
            return {str(key): str(item) for key, item in value.items()}
        return {"raw": str(value)}
