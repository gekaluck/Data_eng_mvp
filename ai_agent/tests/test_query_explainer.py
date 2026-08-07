"""Tests for scan-free Trino validation and bounded plan output."""

import pytest
from trino.exceptions import TrinoUserError

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.query_explainer import MAX_SQL_CHARS, QueryExplainer
from ai_agent.mcp_server.trino_metadata import QueryResult


TABLE = "gold.crypto_dbt.daily_snapshot"


class FakeRunner:
    def __init__(self, result=None, failure=None):
        self.result = result or QueryResult(
            columns=("Query Plan",),
            rows=(("Fragment 0 [SINGLE]\nTableScan",),),
        )
        self.failure = failure
        self.calls = []

    def execute(self, sql):
        self.calls.append(sql)
        if self.failure:
            raise self.failure
        return self.result


@pytest.fixture
def allow_list():
    return TableAllowList(tables=frozenset({TABLE}))


def test_explains_validated_select_without_executing_caller_sql_directly(allow_list):
    runner = FakeRunner()
    sql = f"SELECT snapshot_date FROM {TABLE} LIMIT 1"

    result = QueryExplainer(runner, allow_list).explain_query(sql)

    assert result.valid is True
    assert result.sql == sql
    assert result.tables == (TABLE,)
    assert result.plan_summary == "Fragment 0 [SINGLE]\nTableScan"
    assert result.plan_truncated is False
    assert result.diagnostic is None
    assert runner.calls == [f"EXPLAIN (TYPE DISTRIBUTED) {sql}"]


def test_ast_denial_happens_before_trino(allow_list):
    runner = FakeRunner()

    with pytest.raises(GuardrailError) as raised:
        QueryExplainer(runner, allow_list).explain_query(
            "SELECT * FROM silver.crypto.price_snapshots"
        )

    assert raised.value.code == ErrorCode.TABLE_NOT_ALLOWED
    assert runner.calls == []


def test_truncates_large_plan_and_marks_it_explicitly(allow_list):
    runner = FakeRunner(
        result=QueryResult(columns=("Query Plan",), rows=(("abcdef",),))
    )

    result = QueryExplainer(
        runner,
        allow_list,
        max_plan_chars=4,
    ).explain_query("SELECT 1")

    assert result.plan_summary == "abcd"
    assert result.plan_truncated is True


def test_semantic_user_error_is_a_false_validation_verdict(allow_list):
    failure = TrinoUserError(
        {
            "errorCode": 47,
            "errorName": "COLUMN_NOT_FOUND",
            "errorType": "USER_ERROR",
            "message": "Column 'missing' cannot be resolved",
            "errorLocation": {"lineNumber": 1, "columnNumber": 8},
        },
        query_id="query-1",
    )

    result = QueryExplainer(
        FakeRunner(failure=failure),
        allow_list,
    ).explain_query(f"SELECT missing FROM {TABLE}")

    assert result.valid is False
    assert result.plan_summary is None
    assert result.plan_truncated is False
    assert result.diagnostic.model_dump() == {
        "code": "COLUMN_NOT_FOUND",
        "message": "Column 'missing' cannot be resolved",
        "line": 1,
        "column": 8,
    }


def test_permission_denial_is_an_engine_configuration_error(allow_list):
    failure = TrinoUserError(
        {
            "errorCode": 4,
            "errorName": "PERMISSION_DENIED",
            "errorType": "USER_ERROR",
            "message": "Access Denied",
        }
    )

    with pytest.raises(GuardrailError) as raised:
        QueryExplainer(FakeRunner(failure=failure), allow_list).explain_query(
            f"SELECT * FROM {TABLE}"
        )

    assert raised.value.code == ErrorCode.ENGINE_ERROR
    assert raised.value.retryable is False


def test_connection_failure_is_retryable_engine_error(allow_list):
    with pytest.raises(GuardrailError) as raised:
        QueryExplainer(
            FakeRunner(failure=OSError("connection refused")),
            allow_list,
        ).explain_query("SELECT 1")

    assert raised.value.code == ErrorCode.ENGINE_ERROR
    assert raised.value.retryable is True
    assert "connection refused" in raised.value.message


@pytest.mark.parametrize(
    "result",
    [
        QueryResult(columns=(), rows=()),
        QueryResult(columns=("one", "two"), rows=(("plan", "extra"),)),
        QueryResult(columns=("Query Plan",), rows=((123,),)),
    ],
)
def test_incompatible_explain_shape_fails_closed(allow_list, result):
    with pytest.raises(GuardrailError) as raised:
        QueryExplainer(FakeRunner(result=result), allow_list).explain_query("SELECT 1")

    assert raised.value.code == ErrorCode.ENGINE_ERROR
    assert raised.value.retryable is False


def test_rejects_unbounded_sql_before_parsing_or_planning(allow_list):
    runner = FakeRunner()

    with pytest.raises(GuardrailError) as raised:
        QueryExplainer(runner, allow_list).explain_query(" " * (MAX_SQL_CHARS + 1))

    assert raised.value.code == ErrorCode.PARSE_ERROR
    assert runner.calls == []


@pytest.mark.parametrize("max_plan_chars", [0, True, 1.5])
def test_plan_cap_configuration_must_be_a_positive_integer(
    allow_list, max_plan_chars
):
    with pytest.raises(ValueError):
        QueryExplainer(
            FakeRunner(),
            allow_list,
            max_plan_chars=max_plan_chars,
        )
