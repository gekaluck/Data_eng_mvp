"""Fail-closed SQL validation for the MCP query tools."""

from dataclasses import dataclass

from sqlglot import exp, parse
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.scope import traverse_scope

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError


@dataclass(frozen=True, slots=True)
class ValidatedQuery:
    """A single read-only statement and its normalized physical table dependencies."""

    sql: str
    tables: tuple[str, ...]


def validate_sql(sql: str, allow_list: TableAllowList) -> ValidatedQuery:
    """Validate one Trino SELECT and every physical table it references.

    This function only establishes the first deterministic guardrails. Row caps,
    query budgets, scan polling, and execution timeouts remain separate controls.
    """
    if not isinstance(sql, str) or not sql.strip():
        raise GuardrailError(
            ErrorCode.PARSE_ERROR,
            "SQL must be a non-empty string.",
            hint="Provide one Trino SELECT statement.",
        )

    try:
        statements = parse(sql, read="trino")
    except SqlglotError as exc:
        raise GuardrailError(
            ErrorCode.PARSE_ERROR,
            "SQL could not be parsed as Trino SQL.",
            hint=str(exc),
        ) from exc

    if len(statements) != 1 or statements[0] is None:
        raise GuardrailError(
            ErrorCode.NOT_READ_ONLY,
            "Exactly one SQL statement is allowed.",
            hint="Remove every statement except one SELECT.",
        )

    statement = statements[0]
    if not isinstance(statement, exp.Select) or statement.find(exp.Into):
        # A read-only query root that is not a single SELECT — UNION/INTERSECT/EXCEPT or a
        # parenthesized `(SELECT ...)` — is intentionally out of scope (D036), not a write.
        # Report it as such: the agent loop reflects on this error to redraft, and calling
        # a read-only query "not read-only" steers that retry down the wrong path.
        if isinstance(statement, exp.Query) and not statement.find(exp.Into):
            raise GuardrailError(
                ErrorCode.NOT_READ_ONLY,
                "Only a single top-level SELECT is supported; set operations and "
                "parenthesized query roots are not accepted yet.",
                hint=(
                    "UNION/INTERSECT/EXCEPT and a parenthesized (SELECT ...) are "
                    "read-only but outside the current single-SELECT scope (D036). "
                    "Rewrite the query as one top-level SELECT."
                ),
            )
        raise GuardrailError(
            ErrorCode.NOT_READ_ONLY,
            "Only a single SELECT statement is allowed.",
            hint="DDL, DML, session commands, procedures, and SELECT INTO are blocked.",
        )

    physical_tables = _physical_table_names(statement)
    disallowed = [table for table in physical_tables if not allow_list.allows(table)]
    if disallowed:
        raise GuardrailError(
            ErrorCode.TABLE_NOT_ALLOWED,
            "Query references a table outside the explicit allow-list.",
            hint="Blocked table(s): " + ", ".join(disallowed),
        )

    return ValidatedQuery(sql=sql, tables=physical_tables)


def _physical_table_names(statement: exp.Select) -> tuple[str, ...]:
    """Extract base tables while excluding CTE and derived-table references."""
    physical_tables: set[str] = set()

    for scope in traverse_scope(statement):
        for table in scope.tables:
            if not isinstance(table, exp.Table) or table.name in scope.cte_sources:
                continue

            parts = (table.catalog, table.db, table.name)
            if not all(parts):
                rendered = table.sql(dialect="trino")
                raise GuardrailError(
                    ErrorCode.TABLE_NOT_ALLOWED,
                    "Every physical table must be fully qualified.",
                    hint=(
                        f"Use catalog.schema.table for '{rendered}' and add it to the "
                        "allow-list if it is intentionally exposed."
                    ),
                )

            physical_tables.add(".".join(part.casefold() for part in parts))

    return tuple(sorted(physical_tables))
