"""Tests for capped samples, shared budgets, and fail-closed audit records."""

import json
from datetime import datetime, timezone

import pytest

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.audit import AuditWriteError, JsonlAuditLog
from ai_agent.mcp_server.budget import RequestBudgetManager
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.query_explainer import QueryExplainer
from ai_agent.mcp_server.query_sampler import QuerySampler
from ai_agent.mcp_server.trino_metadata import QueryResult

TABLE = "gold.crypto_dbt.daily_snapshot"
FIXED_TIME = datetime(2026, 8, 7, 20, 0, tzinfo=timezone.utc)


class FakeRunner:
    def __init__(self, result=None, failure=None):
        self.result = result or QueryResult(
            columns=("snapshot_date", "symbol"),
            rows=(("2026-08-07", "BTC"), ("2026-08-07", "ETH")),
        )
        self.failure = failure
        self.calls = []

    def execute(self, sql):
        self.calls.append(sql)
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


def make_sampler(runner, allow_list, budget, audit):
    times = iter((10.0, 10.125, 20.0, 20.125, 30.0, 30.125))
    return QuerySampler(
        runner,
        allow_list,
        budget,
        audit,
        clock=lambda: FIXED_TIME,
        timer=lambda: next(times),
    )


def test_samples_with_a_server_built_quoted_limit_and_audits_summary(allow_list):
    runner = FakeRunner()
    budget = RequestBudgetManager()
    audit = RecordingAudit()
    sampler = make_sampler(runner, allow_list, budget, audit)

    result = sampler.sample_rows(
        TABLE.upper(),
        n=2,
        request_id="request-1",
        profile="fast",
    )

    assert result.table == TABLE
    assert result.columns == ("snapshot_date", "symbol")
    assert result.rows == (("2026-08-07", "BTC"), ("2026-08-07", "ETH"))
    assert runner.calls == [
        'SELECT * FROM "gold"."crypto_dbt"."daily_snapshot" LIMIT 2'
    ]
    assert budget.status("request-1", "fast").used == 1
    assert audit.entries[0].model_dump(mode="json") == {
        "timestamp": "2026-08-07T20:00:00Z",
        "client": "local-mcp",
        "request_id": "request-1",
        "profile": "fast",
        "tool": "sample_rows",
        "table": TABLE,
        "sql": 'SELECT * FROM "gold"."crypto_dbt"."daily_snapshot" LIMIT 2',
        "validation_verdict": "allowed",
        "outcome": "success",
        "columns": ["snapshot_date", "symbol"],
        "row_count": 2,
        "elapsed_ms": 125,
        "failure_code": None,
    }


def test_jsonl_audit_is_append_only_and_omits_business_values(tmp_path, allow_list):
    path = tmp_path / "runtime" / "audit.jsonl"
    runner = FakeRunner()
    sampler = QuerySampler(
        runner,
        allow_list,
        RequestBudgetManager(),
        JsonlAuditLog(path),
        clock=lambda: FIXED_TIME,
    )

    sampler.sample_rows(TABLE, n=2, request_id="one", profile="fast")
    sampler.sample_rows(TABLE, n=2, request_id="two", profile="fast")

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    entries = [json.loads(line) for line in lines]
    assert [entry["request_id"] for entry in entries] == ["one", "two"]
    assert all("rows" not in entry for entry in entries)
    assert all(entry["row_count"] == 2 for entry in entries)


@pytest.mark.parametrize(
    ("table", "n", "code"),
    [
        ("silver.crypto.price_snapshots", 1, ErrorCode.TABLE_NOT_ALLOWED),
        (TABLE, 0, ErrorCode.PARSE_ERROR),
        (TABLE, 21, ErrorCode.PARSE_ERROR),
        (TABLE, True, ErrorCode.PARSE_ERROR),
    ],
)
def test_local_denial_is_audited_without_spending_budget(
    allow_list, table, n, code
):
    runner = FakeRunner()
    budget = RequestBudgetManager()
    audit = RecordingAudit()
    sampler = make_sampler(runner, allow_list, budget, audit)

    with pytest.raises(GuardrailError) as raised:
        sampler.sample_rows(
            table,
            n=n,
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == code
    assert runner.calls == []
    assert budget.status("request-1", "fast").used == 0
    assert audit.entries[0].validation_verdict == "denied"
    assert audit.entries[0].failure_code == code


def test_budget_denial_is_audited_before_trino(allow_list):
    runner = FakeRunner()
    budget = RequestBudgetManager()
    for _ in range(3):
        budget.charge("request-1", "fast")
    audit = RecordingAudit()
    sampler = make_sampler(runner, allow_list, budget, audit)

    with pytest.raises(GuardrailError) as raised:
        sampler.sample_rows(
            TABLE,
            n=2,
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == ErrorCode.BUDGET_EXCEEDED
    assert runner.calls == []
    assert audit.entries[0].validation_verdict == "allowed"
    assert audit.entries[0].failure_code == ErrorCode.BUDGET_EXCEEDED


def test_explain_and_sample_share_one_request_budget(allow_list):
    budget = RequestBudgetManager()
    plan_runner = FakeRunner(
        result=QueryResult(columns=("Query Plan",), rows=(("Fragment 0",),))
    )
    sample_runner = FakeRunner()
    audit = RecordingAudit()
    explainer = QueryExplainer(plan_runner, allow_list, budget)
    sampler = make_sampler(sample_runner, allow_list, budget, audit)

    for _ in range(2):
        explainer.explain_query(
            f"SELECT * FROM {TABLE}",
            request_id="request-1",
            profile="fast",
        )
    sampler.sample_rows(
        TABLE,
        n=2,
        request_id="request-1",
        profile="fast",
    )

    with pytest.raises(GuardrailError) as raised:
        sampler.sample_rows(
            TABLE,
            n=2,
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == ErrorCode.BUDGET_EXCEEDED
    assert len(plan_runner.calls) == 2
    assert len(sample_runner.calls) == 1
    assert [entry.outcome for entry in audit.entries] == ["success", "error"]


def test_engine_failure_spends_budget_and_is_audited(allow_list):
    runner = FakeRunner(failure=OSError("connection refused"))
    budget = RequestBudgetManager()
    audit = RecordingAudit()
    sampler = make_sampler(runner, allow_list, budget, audit)

    with pytest.raises(GuardrailError) as raised:
        sampler.sample_rows(
            TABLE,
            n=1,
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == ErrorCode.ENGINE_ERROR
    assert raised.value.retryable is True
    assert budget.status("request-1", "fast").used == 1
    assert audit.entries[0].failure_code == ErrorCode.ENGINE_ERROR


def test_success_is_not_returned_when_required_audit_write_fails(allow_list):
    runner = FakeRunner()
    budget = RequestBudgetManager()
    sampler = make_sampler(runner, allow_list, budget, FailingAudit())

    with pytest.raises(GuardrailError) as raised:
        sampler.sample_rows(
            TABLE,
            n=2,
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == ErrorCode.ENGINE_ERROR
    assert raised.value.retryable is False
    assert "disk is read-only" in raised.value.hint
    assert budget.status("request-1", "fast").used == 1


@pytest.mark.parametrize(
    "result",
    [
        QueryResult(columns=(), rows=()),
        QueryResult(columns=("one",), rows=((1,), (2,))),
        QueryResult(columns=("one", "two"), rows=((1,),)),
    ],
)
def test_incompatible_or_over_limit_result_fails_closed_and_is_audited(
    allow_list, result
):
    runner = FakeRunner(result=result)
    audit = RecordingAudit()
    sampler = make_sampler(runner, allow_list, RequestBudgetManager(), audit)

    with pytest.raises(GuardrailError) as raised:
        sampler.sample_rows(
            TABLE,
            n=1,
            request_id="request-1",
            profile="fast",
        )

    assert raised.value.code == ErrorCode.ENGINE_ERROR
    assert raised.value.retryable is False
    assert audit.entries[0].failure_code == ErrorCode.ENGINE_ERROR
