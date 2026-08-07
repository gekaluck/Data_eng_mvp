"""Capped, allow-listed row sampling with budget and audit enforcement."""

from collections.abc import Callable
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from trino.exceptions import TrinoUserError

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.audit import (
    AuditSink,
    AuditWriteError,
    SampleAuditEntry,
)
from ai_agent.mcp_server.budget import BudgetProfile, RequestBudgetManager
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.metadata_models import MetadataModel
from ai_agent.mcp_server.trino_metadata import QueryResult, QueryRunner

MAX_SAMPLE_ROWS = 20


class SampleRows(MetadataModel):
    """A bounded table sample; column order matches every returned row tuple."""

    table: str
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


class QuerySampler:
    """Construct one fixed SELECT and enforce its budget/audit boundary."""

    def __init__(
        self,
        runner: QueryRunner,
        allow_list: TableAllowList,
        budget: RequestBudgetManager,
        audit: AuditSink,
        *,
        client: str = "local-mcp",
        clock: Callable[[], datetime] | None = None,
        timer: Callable[[], float] = perf_counter,
    ) -> None:
        self._runner = runner
        self._allow_list = allow_list
        self._budget = budget
        self._audit = audit
        self._client = client
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._timer = timer

    def sample_rows(
        self,
        table: str,
        *,
        n: int,
        request_id: str,
        profile: BudgetProfile,
    ) -> SampleRows:
        """Return at most 20 rows, recording every accepted tool attempt."""
        request_id, profile = self._budget.validate_request(request_id, profile)
        started = self._timer()
        audit_table = table if isinstance(table, str) else repr(table)
        normalized_table: str | None = None
        sql: str | None = None
        validation_verdict = "not_run"

        try:
            normalized_table = self._allowed_table(table)
            audit_table = normalized_table
            self._validate_limit(n)
            sql = self._sample_sql(normalized_table, n)
            validation_verdict = "allowed"
            self._budget.charge(request_id, profile)
            sample = self._execute(sql, normalized_table, n)
        except GuardrailError as exc:
            if validation_verdict == "not_run":
                validation_verdict = "denied"
            self._write_audit(
                request_id=request_id,
                profile=profile,
                table=audit_table,
                sql=sql,
                validation_verdict=validation_verdict,
                outcome="error",
                elapsed_ms=self._elapsed_ms(started),
                failure_code=exc.code,
            )
            raise

        self._write_audit(
            request_id=request_id,
            profile=profile,
            table=normalized_table,
            sql=sql,
            validation_verdict=validation_verdict,
            outcome="success",
            columns=sample.columns,
            row_count=len(sample.rows),
            elapsed_ms=self._elapsed_ms(started),
        )
        return sample

    def _allowed_table(self, table: str) -> str:
        if not isinstance(table, str) or not table.strip():
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                "Table must be a non-empty string.",
                hint="Call list_tables and pass one fully qualified returned table.",
            )
        normalized = table.strip().casefold()
        if not self._allow_list.allows(normalized):
            raise GuardrailError(
                ErrorCode.TABLE_NOT_ALLOWED,
                f"Table '{table}' is not in the explicit Gold allow-list.",
                hint="Call list_tables and use a fully qualified returned table name.",
            )
        return normalized

    @staticmethod
    def _validate_limit(n: int) -> None:
        if not isinstance(n, int) or isinstance(n, bool) or not 1 <= n <= MAX_SAMPLE_ROWS:
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                f"Sample size must be an integer from 1 through {MAX_SAMPLE_ROWS}.",
                hint="Request only the few rows needed to inspect value formats.",
            )

    @staticmethod
    def _sample_sql(table: str, n: int) -> str:
        relation = ".".join(f'"{part}"' for part in table.split("."))
        return f"SELECT * FROM {relation} LIMIT {n}"

    def _execute(self, sql: str, table: str, n: int) -> SampleRows:
        try:
            result = self._runner.execute(sql)
        except GuardrailError:
            raise
        except TrinoUserError as exc:
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                f"Trino rejected the fixed sample query: {exc.message}",
                retryable=False,
                hint="Check the allow-listed table and restricted agent access.",
            ) from exc
        except Exception as exc:
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                f"Trino sample query failed: {exc}",
                retryable=True,
                hint="Check Trino health and retry within the same request budget.",
            ) from exc
        return self._sample_result(result, table, n)

    @staticmethod
    def _sample_result(result: QueryResult, table: str, n: int) -> SampleRows:
        if not result.columns or any(not isinstance(column, str) for column in result.columns):
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                "Trino returned incompatible sample columns.",
                retryable=False,
                hint="Check the pinned Trino client and result-shape contract.",
            )
        if len(result.rows) > n or any(
            len(row) != len(result.columns) for row in result.rows
        ):
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                "Trino returned an incompatible or over-limit sample result.",
                retryable=False,
                hint="Check the fixed LIMIT and pinned Trino client result shape.",
            )
        return SampleRows(table=table, columns=result.columns, rows=result.rows)

    def _elapsed_ms(self, started: float) -> int:
        return max(0, round((self._timer() - started) * 1000))

    def _write_audit(
        self,
        *,
        request_id: str,
        profile: BudgetProfile,
        table: str,
        sql: str | None,
        validation_verdict: str,
        outcome: str,
        elapsed_ms: int,
        columns: tuple[str, ...] = (),
        row_count: int = 0,
        failure_code: ErrorCode | None = None,
    ) -> None:
        entry = SampleAuditEntry(
            timestamp=self._clock(),
            client=self._client,
            request_id=request_id,
            profile=profile,
            table=table,
            sql=sql,
            validation_verdict=validation_verdict,
            outcome=outcome,
            columns=columns,
            row_count=row_count,
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
