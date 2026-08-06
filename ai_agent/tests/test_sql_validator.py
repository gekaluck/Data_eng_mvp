"""Tests for AST-enforced read-only and table-scope guardrails."""

import pytest

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.sql_validator import validate_sql


@pytest.fixture
def allow_list():
    return TableAllowList(
        tables=frozenset(
            {
                "gold.crypto_dbt.daily_snapshot",
                "gold.crypto_dbt.latest_market_snapshot",
            }
        )
    )


@pytest.mark.parametrize(
    ("sql", "expected_tables"),
    [
        ("SELECT 1", ()),
        (
            "SELECT * FROM gold.crypto_dbt.daily_snapshot",
            ("gold.crypto_dbt.daily_snapshot",),
        ),
        (
            "SELECT * FROM gold.crypto_dbt.daily_snapshot FOR VERSION AS OF 123",
            ("gold.crypto_dbt.daily_snapshot",),
        ),
        (
            """
            WITH recent AS (
                SELECT id FROM gold.crypto_dbt.daily_snapshot
            )
            SELECT recent.id
            FROM recent
            JOIN gold.crypto_dbt.latest_market_snapshot latest
              ON recent.id = latest.id
            """,
            (
                "gold.crypto_dbt.daily_snapshot",
                "gold.crypto_dbt.latest_market_snapshot",
            ),
        ),
    ],
)
def test_accepts_single_selects_and_extracts_only_physical_tables(
    allow_list, sql, expected_tables
):
    result = validate_sql(sql, allow_list)

    assert result.sql == sql
    assert result.tables == expected_tables


@pytest.mark.parametrize("sql", ["", "   ", "SELECT * FROM (", "SELECT 'unterminated"])
def test_rejects_empty_or_malformed_sql_as_parse_error(allow_list, sql):
    with pytest.raises(GuardrailError) as raised:
        validate_sql(sql, allow_list)

    assert raised.value.code == ErrorCode.PARSE_ERROR
    assert raised.value.retryable is False


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; SELECT 2",
        "INSERT INTO gold.crypto_dbt.daily_snapshot VALUES (1)",
        "DELETE FROM gold.crypto_dbt.daily_snapshot",
        "CREATE TABLE gold.crypto_dbt.copy AS SELECT 1",
        "DROP TABLE gold.crypto_dbt.daily_snapshot",
        "CALL gold.system.expire_snapshots(schema_name => 'crypto_dbt', table_name => 'x')",
        "SET SESSION query_max_run_time = '1m'",
        "SHOW TABLES FROM gold.crypto_dbt",
        "EXPLAIN SELECT * FROM gold.crypto_dbt.daily_snapshot",
    ],
)
def test_rejects_every_non_select_statement(allow_list, sql):
    with pytest.raises(GuardrailError) as raised:
        validate_sql(sql, allow_list)

    assert raised.value.code == ErrorCode.NOT_READ_ONLY


@pytest.mark.parametrize(
    ("sql", "blocked_name"),
    [
        ("SELECT * FROM daily_snapshot", "daily_snapshot"),
        ("SELECT * FROM crypto_dbt.daily_snapshot", "crypto_dbt.daily_snapshot"),
        (
            "SELECT * FROM silver.crypto.price_snapshots",
            "silver.crypto.price_snapshots",
        ),
        (
            """
            SELECT *
            FROM gold.crypto_dbt.daily_snapshot allowed
            JOIN silver.crypto.price_snapshots blocked ON allowed.id = blocked.id
            """,
            "silver.crypto.price_snapshots",
        ),
        (
            "SELECT * FROM TABLE(gold.system.query(query => 'DELETE FROM x'))",
            "gold.system.query",
        ),
    ],
)
def test_rejects_unqualified_or_disallowed_tables(allow_list, sql, blocked_name):
    with pytest.raises(GuardrailError) as raised:
        validate_sql(sql, allow_list)

    assert raised.value.code == ErrorCode.TABLE_NOT_ALLOWED
    assert blocked_name in raised.value.hint


def test_cte_shadowing_does_not_hide_a_disallowed_physical_table(allow_list):
    sql = """
        WITH price_snapshots AS (
            SELECT * FROM silver.crypto.price_snapshots
        )
        SELECT * FROM price_snapshots
    """

    with pytest.raises(GuardrailError) as raised:
        validate_sql(sql, allow_list)

    assert raised.value.code == ErrorCode.TABLE_NOT_ALLOWED
    assert "silver.crypto.price_snapshots" in raised.value.hint
