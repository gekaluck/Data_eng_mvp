"""Guarded analytical query execution with row, scan, time, budget, and audit caps."""

import os
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic, perf_counter
from typing import Any, Literal, Protocol

from sqlglot import exp, parse_one
from trino.exceptions import TrinoUserError

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.audit import AuditSink, AuditWriteError, ExecuteAuditEntry
from ai_agent.mcp_server.budget import BudgetProfile, RequestBudgetManager
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.metadata_models import MetadataModel
from ai_agent.mcp_server.sql_validator import validate_sql

DEFAULT_MAX_ROWS = 100
MAX_QUERY_ROWS = 500
QUERY_TIMEOUT_SECONDS = 15.0
MAX_SCAN_BYTES = 100 * 1024 * 1024
POLL_INTERVAL_SECONDS = 0.05
MAX_AUDIT_SQL_CHARS = 20_000

DATA_CAVEATS = (
    "Gold history is sparse across calendar dates; use "
    "gold.crypto_dbt.data_availability_daily to distinguish missing and partial days.",
    "Null change, volume, or VWAP values can be intentional after coverage gaps or on "
    "history-repaired dates; inspect model docs and snapshots before answering.",
)


class QueryExecutionStats(MetadataModel):
    """Final Trino work counters exposed to callers and audit records."""

    rows_read: int
    bytes_read: int
    elapsed_ms: int


class ExecutedQuery(MetadataModel):
    """A bounded analytical result with explicit completeness and data caveats."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    truncated: bool
    tables: tuple[str, ...]
    stats: QueryExecutionStats
    query_id: str
    caveats: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RunnerResult:
    """Transport-neutral result from the cancellable Trino runner."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    query_id: str
    rows_read: int
    bytes_read: int
    elapsed_ms: int


class RunnerFailure(Exception):
    """Execution failure carrying the last observable Trino statistics."""

    def __init__(
        self,
        kind: Literal["timeout", "scan_limit", "engine"],
        message: str,
        *,
        query_id: str | None = None,
        rows_read: int = 0,
        bytes_read: int = 0,
        elapsed_ms: int = 0,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.query_id = query_id
        self.rows_read = rows_read
        self.bytes_read = bytes_read
        self.elapsed_ms = elapsed_ms
        self.retryable = retryable


class CappedQueryRunner(Protocol):
    """Execute one already-validated and row-bounded statement."""

    def execute(
        self,
        sql: str,
        *,
        fetch_rows: int,
        timeout_seconds: float,
        max_scan_bytes: int,
    ) -> RunnerResult: ...


class TrinoCappedRunner:
    """Run DB-API work in a monitored thread so active queries can be cancelled."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        http_scheme: str = "http",
        request_timeout_seconds: float = 15.0,
        source: str = "ai-execute-query",
        poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
        timer: Callable[[], float] = monotonic,
    ) -> None:
        self._host = host
        self._port = port
        self._http_scheme = http_scheme
        self._request_timeout_seconds = request_timeout_seconds
        self._source = source
        self._poll_interval_seconds = poll_interval_seconds
        self._timer = timer

    @classmethod
    def from_env(cls) -> "TrinoCappedRunner":
        """Build connection settings without making safety limits configurable."""
        try:
            port = int(os.getenv("AI_TRINO_PORT", "8081"))
            request_timeout = float(os.getenv("AI_TRINO_REQUEST_TIMEOUT_SECONDS", "15"))
        except ValueError as exc:
            raise ValueError("AI Trino port and timeout must be numeric.") from exc
        return cls(
            host=os.getenv("AI_TRINO_HOST", "localhost"),
            port=port,
            http_scheme=os.getenv("AI_TRINO_SCHEME", "http"),
            request_timeout_seconds=request_timeout,
        )

    def execute(
        self,
        sql: str,
        *,
        fetch_rows: int,
        timeout_seconds: float,
        max_scan_bytes: int,
    ) -> RunnerResult:
        """Poll stats and cancel when elapsed time or processed bytes crosses a cap."""
        from trino.dbapi import connect

        connection = connect(
            host=self._host,
            port=self._port,
            user="agent",
            source=self._source,
            http_scheme=self._http_scheme,
            request_timeout=self._request_timeout_seconds,
        )
        cursor = connection.cursor()
        completed: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=1)
        started = self._timer()

        def run_query() -> None:
            try:
                cursor.execute(sql)
                rows = tuple(tuple(row) for row in cursor.fetchmany(fetch_rows))
                columns = tuple(item[0] for item in (cursor.description or ()))
                completed.put(("result", (columns, rows)))
            except Exception as exc:
                completed.put(("error", exc))

        worker = threading.Thread(
            target=run_query,
            name="ai-execute-query",
            daemon=True,
        )
        worker.start()
        try:
            while True:
                try:
                    outcome, payload = completed.get(
                        timeout=self._poll_interval_seconds
                    )
                except queue.Empty:
                    failure = self._limit_failure(
                        cursor,
                        started=started,
                        timeout_seconds=timeout_seconds,
                        max_scan_bytes=max_scan_bytes,
                    )
                    if failure is not None:
                        self._cancel(cursor)
                        raise failure
                    continue

                elapsed_ms = self._elapsed_ms(started)
                query_id, rows_read, bytes_read = self._stats(cursor)
                failure = self._limit_failure(
                    cursor,
                    started=started,
                    timeout_seconds=timeout_seconds,
                    max_scan_bytes=max_scan_bytes,
                )
                if failure is not None:
                    self._cancel(cursor)
                    raise failure
                if outcome == "error":
                    error = payload
                    assert isinstance(error, Exception)
                    raise RunnerFailure(
                        "engine",
                        str(error),
                        query_id=query_id,
                        rows_read=rows_read,
                        bytes_read=bytes_read,
                        elapsed_ms=elapsed_ms,
                        retryable=not isinstance(error, TrinoUserError),
                    ) from error

                columns, rows = payload
                if not isinstance(columns, tuple) or not isinstance(rows, tuple):
                    raise RunnerFailure(
                        "engine",
                        "Trino returned an incompatible execution result.",
                        query_id=query_id,
                        rows_read=rows_read,
                        bytes_read=bytes_read,
                        elapsed_ms=elapsed_ms,
                    )
                if not query_id:
                    raise RunnerFailure(
                        "engine",
                        "Trino completed without a query identifier.",
                        rows_read=rows_read,
                        bytes_read=bytes_read,
                        elapsed_ms=elapsed_ms,
                    )
                return RunnerResult(
                    columns=columns,
                    rows=rows,
                    query_id=query_id,
                    rows_read=rows_read,
                    bytes_read=bytes_read,
                    elapsed_ms=elapsed_ms,
                )
        finally:
            connection.close()

    def _limit_failure(
        self,
        cursor: Any,
        *,
        started: float,
        timeout_seconds: float,
        max_scan_bytes: int,
    ) -> RunnerFailure | None:
        elapsed_ms = self._elapsed_ms(started)
        query_id, rows_read, bytes_read = self._stats(cursor)
        if elapsed_ms > round(timeout_seconds * 1000):
            return RunnerFailure(
                "timeout",
                "Trino query exceeded the "
                f"{timeout_seconds:g}-second wall-clock limit.",
                query_id=query_id,
                rows_read=rows_read,
                bytes_read=bytes_read,
                elapsed_ms=elapsed_ms,
            )
        if bytes_read > max_scan_bytes:
            return RunnerFailure(
                "scan_limit",
                f"Trino query exceeded the {max_scan_bytes}-byte processed-data limit.",
                query_id=query_id,
                rows_read=rows_read,
                bytes_read=bytes_read,
                elapsed_ms=elapsed_ms,
            )
        return None

    @staticmethod
    def _cancel(cursor: Any) -> None:
        try:
            cursor.cancel()
        except Exception:
            # The original cap failure remains authoritative. Closing the connection is
            # the final cleanup path if Trino finished or cancellation itself fails.
            pass

    def _elapsed_ms(self, started: float) -> int:
        return max(0, round((self._timer() - started) * 1000))

    @staticmethod
    def _stats(cursor: Any) -> tuple[str | None, int, int]:
        stats = cursor.stats or {}
        return (
            cursor.query_id,
            TrinoCappedRunner._nonnegative_int(stats.get("processedRows")),
            TrinoCappedRunner._nonnegative_int(stats.get("processedBytes")),
        )

    @staticmethod
    def _nonnegative_int(value: object) -> int:
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError):
            return 0
        return max(0, parsed)


class QueryExecutor:
    """Validate caller SQL, apply hard limits, spend budget, execute, and audit."""

    def __init__(
        self,
        runner: CappedQueryRunner,
        allow_list: TableAllowList,
        budget: RequestBudgetManager,
        audit: AuditSink,
        *,
        timeout_seconds: float = QUERY_TIMEOUT_SECONDS,
        max_scan_bytes: int = MAX_SCAN_BYTES,
        client: str = "local-mcp",
        clock: Callable[[], datetime] | None = None,
        timer: Callable[[], float] = perf_counter,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive.")
        if max_scan_bytes < 1:
            raise ValueError("max_scan_bytes must be positive.")
        self._runner = runner
        self._allow_list = allow_list
        self._budget = budget
        self._audit = audit
        self._timeout_seconds = timeout_seconds
        self._max_scan_bytes = max_scan_bytes
        self._client = client
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._timer = timer

    def execute_query(
        self,
        sql: str,
        *,
        request_id: str,
        profile: BudgetProfile,
        max_rows: int = DEFAULT_MAX_ROWS,
    ) -> ExecutedQuery:
        """Return a safely bounded result or a structured, audited failure."""
        request_id, profile = self._budget.validate_request(request_id, profile)
        started = self._timer()
        audit_sql = self._audit_sql(sql)
        tables: tuple[str, ...] = ()
        validation_verdict = "not_run"
        runner_result: RunnerResult | None = None

        try:
            self._validate_max_rows(max_rows)
            if isinstance(sql, str) and len(sql) > MAX_AUDIT_SQL_CHARS:
                raise GuardrailError(
                    ErrorCode.PARSE_ERROR,
                    f"SQL exceeds the {MAX_AUDIT_SQL_CHARS}-character execution limit.",
                    hint="Shorten the statement before executing it.",
                )
            validated = validate_sql(sql, self._allow_list)
            tables = validated.tables
            bounded_sql = self._bounded_sql(validated.sql, max_rows)
            audit_sql = bounded_sql
            validation_verdict = "allowed"
            self._budget.charge(request_id, profile)
            runner_result = self._runner.execute(
                bounded_sql,
                # The SQL can produce only max_rows + 1. Asking DB-API for one more
                # forces it to consume the terminal protocol response and final stats.
                fetch_rows=max_rows + 2,
                timeout_seconds=self._timeout_seconds,
                max_scan_bytes=self._max_scan_bytes,
            )
            result = self._result(runner_result, tables=tables, max_rows=max_rows)
        except RunnerFailure as exc:
            error = self._runner_error(exc)
            self._write_audit(
                request_id=request_id,
                profile=profile,
                sql=audit_sql,
                tables=tables,
                validation_verdict=validation_verdict,
                outcome="error",
                query_id=exc.query_id,
                rows_read=exc.rows_read,
                bytes_read=exc.bytes_read,
                elapsed_ms=exc.elapsed_ms,
                failure_code=error.code,
            )
            raise error from exc
        except GuardrailError as exc:
            if validation_verdict == "not_run":
                validation_verdict = "denied"
            self._write_audit(
                request_id=request_id,
                profile=profile,
                sql=audit_sql,
                tables=tables,
                validation_verdict=validation_verdict,
                outcome="error",
                query_id=runner_result.query_id if runner_result else None,
                rows_read=runner_result.rows_read if runner_result else 0,
                bytes_read=runner_result.bytes_read if runner_result else 0,
                elapsed_ms=(
                    runner_result.elapsed_ms
                    if runner_result
                    else self._elapsed_ms(started)
                ),
                failure_code=exc.code,
            )
            raise

        self._write_audit(
            request_id=request_id,
            profile=profile,
            sql=audit_sql,
            tables=tables,
            validation_verdict=validation_verdict,
            outcome="success",
            columns=result.columns,
            row_count=len(result.rows),
            truncated=result.truncated,
            query_id=result.query_id,
            rows_read=result.stats.rows_read,
            bytes_read=result.stats.bytes_read,
            elapsed_ms=result.stats.elapsed_ms,
        )
        return result

    @staticmethod
    def _validate_max_rows(max_rows: int) -> None:
        if (
            not isinstance(max_rows, int)
            or isinstance(max_rows, bool)
            or not 1 <= max_rows <= MAX_QUERY_ROWS
        ):
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                f"max_rows must be an integer from 1 through {MAX_QUERY_ROWS}.",
                hint=(
                    "Lower the result bound; the server never returns more than "
                    "500 rows."
                ),
            )

    @staticmethod
    def _bounded_sql(sql: str, max_rows: int) -> str:
        statement = parse_one(sql, read="trino")
        limit = statement.args.get("limit")
        existing = QueryExecutor._literal_limit(limit)
        if existing is not None and existing <= max_rows:
            return statement.sql(dialect="trino")
        return statement.limit(max_rows + 1, copy=False).sql(dialect="trino")

    @staticmethod
    def _literal_limit(limit: exp.Expression | None) -> int | None:
        if limit is None:
            return None
        expression = (
            limit.args.get("count")
            if isinstance(limit, exp.Fetch)
            else limit.args.get("expression")
        )
        if isinstance(expression, exp.Literal) and expression.is_int:
            return int(expression.this)
        return None

    @staticmethod
    def _result(
        runner_result: RunnerResult,
        *,
        tables: tuple[str, ...],
        max_rows: int,
    ) -> ExecutedQuery:
        if not runner_result.columns or any(
            not isinstance(column, str) or not column
            for column in runner_result.columns
        ):
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                "Trino returned incompatible query columns.",
                hint="Check the pinned Trino client and execution result contract.",
            )
        if len(runner_result.rows) > max_rows + 1 or any(
            len(row) != len(runner_result.columns) for row in runner_result.rows
        ):
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                "Trino returned an incompatible or over-limit query result.",
                hint="Check the injected LIMIT and pinned Trino client result shape.",
            )
        truncated = len(runner_result.rows) > max_rows
        return ExecutedQuery(
            columns=runner_result.columns,
            rows=runner_result.rows[:max_rows],
            truncated=truncated,
            tables=tables,
            stats=QueryExecutionStats(
                rows_read=runner_result.rows_read,
                bytes_read=runner_result.bytes_read,
                elapsed_ms=runner_result.elapsed_ms,
            ),
            query_id=runner_result.query_id,
            caveats=DATA_CAVEATS,
        )

    @staticmethod
    def _runner_error(exc: RunnerFailure) -> GuardrailError:
        if exc.kind == "timeout":
            return GuardrailError(
                ErrorCode.TIMEOUT,
                exc.message,
                hint=(
                    "Simplify the query or refuse; do not retry past the request "
                    "budget."
                ),
            )
        if exc.kind == "scan_limit":
            return GuardrailError(
                ErrorCode.SCAN_LIMIT_EXCEEDED,
                exc.message,
                hint=(
                    "Reduce the scanned date range or aggregation scope before "
                    "retrying."
                ),
            )
        return GuardrailError(
            ErrorCode.ENGINE_ERROR,
            f"Trino analytical query failed: {exc.message}",
            retryable=exc.retryable,
            hint="Check the SQL, Trino health, and restricted agent access.",
        )

    @staticmethod
    def _audit_sql(sql: object) -> str:
        text = sql if isinstance(sql, str) else repr(sql)
        if len(text) > MAX_AUDIT_SQL_CHARS:
            return text[:MAX_AUDIT_SQL_CHARS] + "...(truncated)"
        return text

    def _elapsed_ms(self, started: float) -> int:
        return max(0, round((self._timer() - started) * 1000))

    def _write_audit(
        self,
        *,
        request_id: str,
        profile: BudgetProfile,
        sql: str,
        tables: tuple[str, ...],
        validation_verdict: str,
        outcome: str,
        elapsed_ms: int,
        columns: tuple[str, ...] = (),
        row_count: int = 0,
        truncated: bool = False,
        query_id: str | None = None,
        rows_read: int = 0,
        bytes_read: int = 0,
        failure_code: ErrorCode | None = None,
    ) -> None:
        entry = ExecuteAuditEntry(
            timestamp=self._clock(),
            client=self._client,
            request_id=request_id,
            profile=profile,
            sql=sql,
            tables=tables,
            validation_verdict=validation_verdict,
            outcome=outcome,
            columns=columns,
            row_count=row_count,
            truncated=truncated,
            query_id=query_id,
            rows_read=rows_read,
            bytes_read=bytes_read,
            elapsed_ms=elapsed_ms,
            failure_code=failure_code,
        )
        try:
            self._audit.write(entry)
        except AuditWriteError as exc:
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                "Required query audit write failed.",
                retryable=False,
                hint=str(exc),
            ) from exc
