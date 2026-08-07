"""Scan-free Trino planning for validated, allow-listed SELECT statements."""

from trino.exceptions import TrinoUserError

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.metadata_models import MetadataModel
from ai_agent.mcp_server.sql_validator import validate_sql
from ai_agent.mcp_server.trino_metadata import QueryResult, QueryRunner


MAX_SQL_CHARS = 20_000
MAX_PLAN_CHARS = 12_000


class QueryDiagnostic(MetadataModel):
    """A semantic validation failure reported by Trino's planner."""

    code: str
    message: str
    line: int | None = None
    column: int | None = None


class QueryExplanation(MetadataModel):
    """Bounded plan text plus the deterministic and engine validation verdict."""

    sql: str
    tables: tuple[str, ...]
    valid: bool
    plan_summary: str | None = None
    plan_truncated: bool = False
    diagnostic: QueryDiagnostic | None = None


class QueryExplainer:
    """Validate caller SQL, then ask Trino to plan it without executing it."""

    def __init__(
        self,
        runner: QueryRunner,
        allow_list: TableAllowList,
        *,
        max_plan_chars: int = MAX_PLAN_CHARS,
    ) -> None:
        if not isinstance(max_plan_chars, int) or isinstance(max_plan_chars, bool):
            raise ValueError("max_plan_chars must be an integer.")
        if max_plan_chars < 1:
            raise ValueError("max_plan_chars must be positive.")
        self._runner = runner
        self._allow_list = allow_list
        self._max_plan_chars = max_plan_chars

    def explain_query(self, sql: str) -> QueryExplanation:
        """Return a distributed plan or a semantic diagnostic, never query rows."""
        if isinstance(sql, str) and len(sql) > MAX_SQL_CHARS:
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                f"SQL exceeds the {MAX_SQL_CHARS}-character explain limit.",
                hint="Shorten the statement before asking Trino to plan it.",
            )
        validated = validate_sql(sql, self._allow_list)

        try:
            result = self._runner.execute(
                f"EXPLAIN (TYPE DISTRIBUTED) {validated.sql}"
            )
        except TrinoUserError as exc:
            if exc.error_name == "PERMISSION_DENIED":
                raise GuardrailError(
                    ErrorCode.ENGINE_ERROR,
                    "Trino denied the restricted agent while explaining an "
                    "allow-listed query.",
                    retryable=False,
                    hint="Check the agent access rules and Trino configuration.",
                ) from exc
            location = exc.error_location
            return QueryExplanation(
                sql=validated.sql,
                tables=validated.tables,
                valid=False,
                diagnostic=QueryDiagnostic(
                    code=exc.error_name or "TRINO_USER_ERROR",
                    message=exc.message,
                    line=location[0] if location else None,
                    column=location[1] if location else None,
                ),
            )
        except GuardrailError:
            raise
        except Exception as exc:
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                f"Trino query planning failed: {exc}",
                retryable=True,
                hint="Check Trino health and retry the scan-free explain.",
            ) from exc

        plan = self._plan_text(result)
        truncated = len(plan) > self._max_plan_chars
        return QueryExplanation(
            sql=validated.sql,
            tables=validated.tables,
            valid=True,
            plan_summary=plan[: self._max_plan_chars],
            plan_truncated=truncated,
        )

    @staticmethod
    def _plan_text(result: QueryResult) -> str:
        if (
            len(result.columns) != 1
            or len(result.rows) != 1
            or len(result.rows[0]) != 1
            or not isinstance(result.rows[0][0], str)
        ):
            raise GuardrailError(
                ErrorCode.ENGINE_ERROR,
                "Trino returned an incompatible EXPLAIN result shape.",
                retryable=False,
                hint="Check the pinned Trino version and EXPLAIN output contract.",
            )
        return result.rows[0][0]
