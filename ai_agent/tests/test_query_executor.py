"""Tests for governed analytical query execution."""

import json
from datetime import datetime, timezone

import pytest

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.audit import AuditWriteError, JsonlAuditLog
from ai_agent.mcp_server.budget import RequestBudgetManager
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.query_executor import (
    DATA_CAVEATS,
    MAX_AUDIT_SQL_CHARS,
    MAX_QUERY_ROWS,
    QueryExecutor,
    RunnerFailure,
    RunnerResult,
)

TABLE = "gold.crypto_dbt.daily_snapshot"
FIXED_TIME = datetime(2026, 8, 7, 21, 0, tzinfo=timezone.utc)


class FakeRunner:
    def __init__(self, result=None, failure=None):
        self.result = result or RunnerResult(
            columns=("symbol",),
            rows=(("BTC",), ("ETH",), ("SOL",)),
            query_id="query-1",
            rows_read=20,
            bytes_read=4096,
            elapsed_ms=125,
        )
        self.failure = failure
        self.calls = []

    def execute(self, sql, **limits):
        self.calls.append({"sql": sql, **limits})
        if self.failure:
            raise self.failure
        return self.result


class RecordingAudit:
    def __init__(self):
        self.entries = []

    def write(self, entry):
        self.entries.append(entry)


class FailingAudit:
    def write(self, entry):
        raise AuditWriteError("disk is read-only")


@pytest.fixture
def allow_list():
    return TableAllowList(tables=frozenset({TABLE}))


def make_executor(runner, allow_list, budget, audit):
    times = iter((10.0, 10.125, 20.0, 20.125))
    return QueryExecutor(
        runner,
        allow_list,
        budget,
        audit,
        clock=lambda: FIXED_TIME,
        timer=lambda: next(times),
    )


def test_executes_with_one_extra_row_for_exact_truncation_and_audits(allow_list):
    runner = FakeRunner()
    budget = RequestBudgetManager()
    audit = RecordingAudit()
    executor = make_executor(runner, allow_list, budget, audit)

    result = executor.execute_query(
        f"select symbol from {TABLE}",
        request_id="request-1",
        profile="fast",
        max_rows=2,
    )

    assert result.columns == ("symbol",)
    assert result.rows == (("BTC",), ("ETH",))
    assert result.truncated is True
    assert result.tables == (TABLE,)
    assert result.query_id == "query-1"
    assert result.stats.model_dump() == {
        "rows_read": 20,
        "bytes_read": 4096,
        "elapsed_ms": 125,
    }
    assert result.caveats == DATA_CAVEATS
    assert runner.calls == [
        {
            "sql": f"SELECT symbol FROM {TABLE} LIMIT 3",
            "fetch_rows": 4,
            "timeout_seconds": 15.0,
            "max_scan_bytes": 100 * 1024 * 1024,
        }
    ]
    assert budget.status("request-1", "fast").used == 1
    entry = audit.entries[0]
    assert entry.outcome == "success"
    assert entry.row_count == 2
    assert entry.truncated is True
    assert entry.query_id == "query-1"
    assert entry.rows_read == 20
    assert entry.bytes_read == 4096


def test_preserves_a_caller_limit_that_is_below_the_tool_cap(allow_list):
    runner = FakeRunner(
        result=RunnerResult(
            columns=("symbol",),
            rows=(("BTC",),),
            query_id="query-1",
            rows_read=1,
            bytes_read=100,
            elapsed_ms=5,
        )
    )
    executor = make_executor(
        runner,
        allow_list,
        RequestBudgetManager(),
        RecordingAudit(),
    )

    result = executor.execute_query(
        f"SELECT symbol FROM {TABLE} LIMIT 1",
        request_id="request-1",
        profile="fast",
        max_rows=20,
    )

    assert runner.calls[0]["sql"] == f"SELECT symbol FROM {TABLE} LIMIT 1"
    assert runner.calls[0]["fetch_rows"] == 22
    assert result.truncated is False


def test_preserves_a_lower_fetch_first_limit(allow_list):
    runner = FakeRunner(
        result=RunnerResult(
            columns=("symbol",),
            rows=(("BTC",),),
            query_id="query-1",
            rows_read=1,
            bytes_read=100,
            elapsed_ms=5,
        )
    )
    executor = make_executor(
        runner,
        allow_list,
        RequestBudgetManager(),
        RecordingAudit(),
    )

    executor.execute_query(
        f"SELECT symbol FROM {TABLE} FETCH FIRST 1 ROWS ONLY",
        request_id="request-1",
        profile="fast",
        max_rows=20,
    )

    assert runner.calls[0]["sql"].endswith("FETCH FIRST 1 ROWS ONLY")


@pytest.mark.parametrize(
    "caller_limit",
    ["LIMIT 1000", "FETCH FIRST 1000 ROWS ONLY"],
)
def test_replaces_an_oversized_caller_limit(allow_list, caller_limit):
    runner = FakeRunner()
    executor = make_executor(
        runner,
        allow_list,
        RequestBudgetManager(),
        RecordingAudit(),
    )

    executor.execute_query(
        f"SELECT symbol FROM {TABLE} {caller_limit}",
        request_id="request-1",
        profile="fast",
        max_rows=2,
    )

    assert runner.calls[0]["sql"] == f"SELECT symbol FROM {TABLE} LIMIT 3"


@pytest.mark.parametrize(
    ("sql", "max_rows", "code"),
    [
        (
            "SELECT * FROM silver.crypto.price_snapshots",
            10,
            ErrorCode.TABLE_NOT_ALLOWED,
        ),
        (f"DELETE FROM {TABLE}", 10, ErrorCode.NOT_READ_ONLY),
        (f"SELECT * FROM {TABLE}", 0, ErrorCode.PARSE_ERROR),
        (f"SELECT * FROM {TABLE}", MAX_QUERY_ROWS + 1, ErrorCode.PARSE_ERROR),
        ("x" * (MAX_AUDIT_SQL_CHARS + 1), 10, ErrorCode.PARSE_ERROR),
    ],
)
def test_local_denials_are_audited_without_spending_budget(
    allow_list, sql, max_rows, code
):
    runner = FakeRunner()
    budget = RequestBudgetManager()
    audit = RecordingAudit()
    executor = make_executor(runner, allow_list, budget, audit)

    with pytest.raises(GuardrailError) as raised:
        executor.execute_query(
            sql,
            request_id="request-1",
            profile="fast",
            max_rows=max_rows,
        )

    assert raised.value.code == code
    assert runner.calls == []
    assert budget.status("request-1", "fast").used == 0
    assert audit.entries[0].validation_verdict == "denied"
    assert audit.entries[0].failure_code == code
    assert len(audit.entries[0].sql) <= MAX_AUDIT_SQL_CHARS + len("...(truncated)")


def test_budget_denial_is_audited_before_execution(allow_list):
    runner = FakeRunner()
    budget = RequestBudgetManager()
    for _ in range(3):
        budget.charge("request-1", "fast")
    audit = RecordingAudit()
    executor = make_executor(runner, allow_list, budget, audit)

    with pytest.raises(GuardrailError) as raised:
        executor.execute_query(
            f"SELECT * FROM {TABLE}",
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == ErrorCode.BUDGET_EXCEEDED
    assert runner.calls == []
    assert audit.entries[0].validation_verdict == "allowed"
    assert audit.entries[0].failure_code == ErrorCode.BUDGET_EXCEEDED


@pytest.mark.parametrize(
    ("kind", "code", "retryable"),
    [
        ("timeout", ErrorCode.TIMEOUT, False),
        ("scan_limit", ErrorCode.SCAN_LIMIT_EXCEEDED, False),
        ("engine", ErrorCode.ENGINE_ERROR, True),
    ],
)
def test_runner_failures_spend_budget_and_audit_last_stats(
    allow_list, kind, code, retryable
):
    failure = RunnerFailure(
        kind,
        "stopped",
        query_id="query-1",
        rows_read=50,
        bytes_read=8192,
        elapsed_ms=250,
        retryable=retryable,
    )
    runner = FakeRunner(failure=failure)
    budget = RequestBudgetManager()
    audit = RecordingAudit()
    executor = make_executor(runner, allow_list, budget, audit)

    with pytest.raises(GuardrailError) as raised:
        executor.execute_query(
            f"SELECT * FROM {TABLE}",
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == code
    assert raised.value.retryable is retryable
    assert budget.status("request-1", "fast").used == 1
    entry = audit.entries[0]
    assert entry.failure_code == code
    assert entry.query_id == "query-1"
    assert entry.rows_read == 50
    assert entry.bytes_read == 8192
    assert entry.elapsed_ms == 250


def test_incompatible_result_fails_closed_with_execution_stats(allow_list):
    runner = FakeRunner(
        result=RunnerResult(
            columns=("one", "two"),
            rows=((1,),),
            query_id="query-1",
            rows_read=1,
            bytes_read=80,
            elapsed_ms=7,
        )
    )
    audit = RecordingAudit()
    executor = make_executor(
        runner,
        allow_list,
        RequestBudgetManager(),
        audit,
    )

    with pytest.raises(GuardrailError) as raised:
        executor.execute_query(
            f"SELECT * FROM {TABLE}",
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == ErrorCode.ENGINE_ERROR
    assert audit.entries[0].query_id == "query-1"
    assert audit.entries[0].bytes_read == 80


def test_success_is_not_returned_when_required_audit_write_fails(allow_list):
    executor = make_executor(
        FakeRunner(),
        allow_list,
        RequestBudgetManager(),
        FailingAudit(),
    )

    with pytest.raises(GuardrailError) as raised:
        executor.execute_query(
            f"SELECT symbol FROM {TABLE}",
            request_id="request-1",
            profile="fast",
            max_rows=2,
        )

    assert raised.value.code == ErrorCode.ENGINE_ERROR
    assert raised.value.retryable is False
    assert "disk is read-only" in raised.value.hint


def test_jsonl_execution_audit_omits_business_rows(tmp_path, allow_list):
    path = tmp_path / "audit.jsonl"
    executor = QueryExecutor(
        FakeRunner(),
        allow_list,
        RequestBudgetManager(),
        JsonlAuditLog(path),
        clock=lambda: FIXED_TIME,
    )

    executor.execute_query(
        f"SELECT symbol FROM {TABLE}",
        request_id="request-1",
        profile="fast",
        max_rows=2,
    )

    entry = json.loads(path.read_text(encoding="utf-8"))
    assert entry["tool"] == "execute_query"
    assert entry["row_count"] == 2
    assert entry["truncated"] is True
    assert "rows" not in entry
