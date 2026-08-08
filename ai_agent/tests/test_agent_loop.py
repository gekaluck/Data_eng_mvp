"""Tests for bounded state transitions, retries, checks, and terminal envelopes."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from ai_agent.agent_service import loop as loop_module
from ai_agent.agent_service.contracts import (
    AgentRequest,
    AnswerDecision,
    CriticDecision,
    PlanDecision,
    SqlDraft,
)
from ai_agent.agent_service.loop import AgentLoop
from ai_agent.agent_service.mcp_tools import ToolCallError
from ai_agent.agent_service.provider import ProviderError
from ai_agent.mcp_server.metadata_models import (
    ColumnDocs,
    ColumnSchema,
    ModelDocs,
    TableSchema,
    TableSnapshot,
    TableSnapshots,
    TableStats,
    TableSummary,
)
from ai_agent.mcp_server.query_executor import (
    DATA_CAVEATS,
    ExecutedQuery,
    QueryExecutionStats,
)
from ai_agent.mcp_server.query_explainer import QueryDiagnostic, QueryExplanation
from ai_agent.mcp_server.query_sampler import SampleRows

TABLE = "gold.crypto_dbt.daily_snapshot"
NOW = datetime(2026, 8, 7, tzinfo=timezone.utc)


def plan(*, disposition="answer", tables=(TABLE,), samples=()):
    return PlanDecision(
        disposition=disposition,
        reason="supported" if disposition == "answer" else "not in Gold",
        tables=tables,
        sample_tables=samples,
    )


def draft(sql="SELECT symbol FROM gold.crypto_dbt.daily_snapshot"):
    return SqlDraft(
        sql=sql,
        rationale="select the requested symbol",
        expected_columns=("symbol",),
        expects_rows=True,
    )


def executed(*, rows=(("BTC",),), truncated=False):
    return ExecutedQuery(
        columns=("symbol",),
        rows=rows,
        truncated=truncated,
        tables=(TABLE,),
        stats=QueryExecutionStats(
            rows_read=20,
            bytes_read=4096,
            elapsed_ms=15,
        ),
        query_id="query-1",
        caveats=DATA_CAVEATS,
    )


class FakeProvider:
    def __init__(self, **stages):
        self.stages = {name: list(values) for name, values in stages.items()}
        self.calls = []

    def model_id(self, profile):
        return "claude-sonnet-5" if profile == "fast" else "claude-opus-5"

    async def generate(self, **kwargs):
        self.calls.append(kwargs)
        value = self.stages[kwargs["stage"]].pop(0)
        if isinstance(value, Exception):
            raise value
        assert isinstance(value, kwargs["output_type"])
        return value


class SlowProvider(FakeProvider):
    async def generate(self, **kwargs):
        await asyncio.sleep(0.02)
        return await super().generate(**kwargs)


class FakeTools:
    def __init__(self, *, explanations=None, executions=None, sample=None):
        self.calls = []
        self.explanations = list(
            explanations
            or [
                QueryExplanation(sql="SELECT 1", tables=(TABLE,), valid=True)
                for _ in range(4)
            ]
        )
        self.executions = list(executions or [executed()])
        self.sample = sample or SampleRows(
            table=TABLE,
            columns=("symbol",),
            rows=(("BTC",),),
        )

    async def list_tables(self):
        self.calls.append(("list_tables", None))
        return (
            TableSummary(
                table=TABLE,
                description="Daily ranked crypto market snapshot.",
            ),
        )

    async def get_model_docs(self, model):
        self.calls.append(("get_model_docs", model))
        return ModelDocs(
            model="daily_snapshot",
            table=TABLE,
            description="Daily ranked crypto market snapshot.",
            columns=(ColumnDocs(name="symbol", description="Ticker"),),
            tests=(),
        )

    async def get_table_schema(self, table):
        self.calls.append(("get_table_schema", table))
        return TableSchema(
            table=TABLE,
            columns=(ColumnSchema(name="symbol", type="varchar"),),
            stats=TableStats(row_count=20, size_bytes=4096, last_updated=NOW),
        )

    async def get_table_snapshots(self, table, *, limit=2):
        self.calls.append(("get_table_snapshots", (table, limit)))
        return TableSnapshots(
            table=TABLE,
            snapshots=(
                TableSnapshot(
                    snapshot_id=1,
                    committed_at=NOW,
                    operation="append",
                ),
            ),
        )

    async def sample_rows(self, table, **kwargs):
        self.calls.append(("sample_rows", (table, kwargs)))
        if isinstance(self.sample, Exception):
            raise self.sample
        return self.sample

    async def explain_query(self, sql, **kwargs):
        self.calls.append(("explain_query", (sql, kwargs)))
        value = self.explanations.pop(0)
        if isinstance(value, Exception):
            raise value
        return value.model_copy(update={"sql": sql})

    async def execute_query(self, sql, **kwargs):
        self.calls.append(("execute_query", (sql, kwargs)))
        value = self.executions.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class FakeGateway:
    def __init__(self, tools):
        self.tools = tools
        self.opened = 0

    @asynccontextmanager
    async def session(self):
        self.opened += 1
        yield self.tools


def run(loop, *, profile="fast", request_id="request-1"):
    return asyncio.run(
        loop.answer(
            AgentRequest(
                question="Which symbols are available?",
                profile=profile,
                request_id=request_id,
            )
        )
    )


def test_fast_profile_reaches_answer_without_a_critic():
    provider = FakeProvider(
        plan=[plan()],
        draft=[draft()],
        answer=[AnswerDecision(answer="BTC is available.", caveats=())],
    )
    tools = FakeTools()

    result = run(AgentLoop(provider, FakeGateway(tools)))

    assert result.answer == "BTC is available."
    assert result.refusal_reason is None
    assert result.sql == draft().sql
    assert result.tables_used == (TABLE,)
    assert result.result_stats.bytes_read == 4096
    assert result.model_id == "claude-sonnet-5"
    assert "critic_passed" not in result.confidence
    assert [call["stage"] for call in provider.calls] == ["plan", "draft", "answer"]
    assert [name for name, _ in tools.calls].count("explain_query") == 1
    assert [name for name, _ in tools.calls].count("execute_query") == 1


def test_thorough_critic_gets_one_refinement_then_passes():
    provider = FakeProvider(
        plan=[plan()],
        draft=[
            draft("SELECT symbol FROM gold.crypto_dbt.daily_snapshot LIMIT 1"),
            draft(),
        ],
        critic=[
            CriticDecision(verdict="fail", reason="LIMIT 1 is incomplete"),
            CriticDecision(verdict="pass", reason="matches the question"),
        ],
        answer=[AnswerDecision(answer="BTC is available.", caveats=())],
    )
    tools = FakeTools(executions=[executed(truncated=True), executed()])

    result = run(AgentLoop(provider, FakeGateway(tools)), profile="thorough")

    assert result.answer == "BTC is available."
    assert result.model_id == "claude-opus-5"
    assert "critic_passed" in result.confidence
    assert [call["stage"] for call in provider.calls] == [
        "plan",
        "draft",
        "critic",
        "draft",
        "critic",
        "answer",
    ]
    assert "CRITIC failed" in provider.calls[3]["prompt"]


def test_plan_can_refuse_without_drafting_or_touching_business_rows():
    provider = FakeProvider(plan=[plan(disposition="refuse", tables=())])
    tools = FakeTools()

    result = run(AgentLoop(provider, FakeGateway(tools)))

    assert result.answer is None
    assert result.refusal_reason == "not in Gold"
    assert result.sql is None
    assert [name for name, _ in tools.calls] == ["list_tables"]


def test_unknown_or_excessive_plan_is_refused_deterministically():
    unknown = FakeProvider(plan=[plan(tables=("gold.crypto_dbt.unknown",))])
    unknown_result = run(AgentLoop(unknown, FakeGateway(FakeTools())))
    assert "unknown or duplicate" in unknown_result.refusal_reason

    sampled = FakeProvider(plan=[plan(samples=(TABLE, TABLE))])
    sampled_result = run(AgentLoop(sampled, FakeGateway(FakeTools())))
    assert "invalid or excessive" in sampled_result.refusal_reason


def test_semantic_validation_failure_is_fed_to_one_fast_retry():
    invalid = QueryExplanation(
        sql="bad",
        tables=(TABLE,),
        valid=False,
        diagnostic=QueryDiagnostic(code="COLUMN_NOT_FOUND", message="missing"),
    )
    provider = FakeProvider(
        plan=[plan()],
        draft=[draft("SELECT missing FROM gold.crypto_dbt.daily_snapshot"), draft()],
        answer=[AnswerDecision(answer="BTC is available.", caveats=())],
    )
    tools = FakeTools(
        explanations=[
            invalid,
            QueryExplanation(sql="good", tables=(TABLE,), valid=True),
        ]
    )

    result = run(AgentLoop(provider, FakeGateway(tools)))

    assert result.answer == "BTC is available."
    assert "COLUMN_NOT_FOUND" in provider.calls[2]["prompt"]
    assert [name for name, _ in tools.calls].count("explain_query") == 2
    assert [name for name, _ in tools.calls].count("execute_query") == 1


def test_budget_exhaustion_forces_a_visible_refusal():
    provider = FakeProvider(plan=[plan()], draft=[draft()])
    tools = FakeTools(
        explanations=[
            ToolCallError(
                "BUDGET_EXCEEDED",
                "No engine tokens remain.",
                hint="Stop this request.",
            )
        ]
    )

    result = run(AgentLoop(provider, FakeGateway(tools)))

    assert result.answer is None
    assert "BUDGET_EXCEEDED" in result.refusal_reason
    assert result.sql == draft().sql
    assert [call["stage"] for call in provider.calls] == ["plan", "draft"]


def test_empty_required_results_exhaust_the_fast_retry_bound():
    provider = FakeProvider(
        plan=[plan()],
        draft=[draft(), draft()],
    )
    tools = FakeTools(executions=[executed(rows=()), executed(rows=())])

    result = run(AgentLoop(provider, FakeGateway(tools)))

    assert result.answer is None
    assert "returned no rows" in result.refusal_reason
    assert [name for name, _ in tools.calls].count("execute_query") == 2
    assert result.result_stats.bytes_read == 4096


def test_sampling_is_used_only_when_plan_requests_it():
    provider = FakeProvider(
        plan=[plan(samples=(TABLE,))],
        draft=[draft()],
        answer=[AnswerDecision(answer="BTC is available.", caveats=())],
    )
    tools = FakeTools()

    result = run(AgentLoop(provider, FakeGateway(tools)))

    assert result.answer is not None
    assert [name for name, _ in tools.calls].count("sample_rows") == 1
    assert '"sample"' in provider.calls[1]["prompt"]


def test_truncation_is_always_propagated_as_a_terminal_caveat():
    provider = FakeProvider(
        plan=[plan()],
        draft=[draft()],
        answer=[AnswerDecision(answer="A bounded list was returned.", caveats=())],
    )

    result = run(
        AgentLoop(
            provider,
            FakeGateway(FakeTools(executions=[executed(truncated=True)])),
        )
    )

    assert result.answer is not None
    assert any("truncated" in caveat for caveat in result.caveats)


def test_check_rejects_extra_columns_and_tableless_queries():
    extra_columns = executed().model_copy(
        update={"columns": ("symbol", "extra"), "rows": (("BTC", 1),)}
    )
    error, _ = AgentLoop._check_result(
        draft(), extra_columns, planned_tables=(TABLE,)
    )
    assert "exactly match" in error

    tableless = executed().model_copy(update={"tables": ()})
    error, _ = AgentLoop._check_result(draft(), tableless, planned_tables=(TABLE,))
    assert "outside the approved plan" in error


def test_provider_failure_becomes_a_terminal_refusal():
    provider = FakeProvider(plan=[ProviderError("provider unavailable")])

    result = run(AgentLoop(provider, FakeGateway(FakeTools())))

    assert result.answer is None
    assert result.refusal_reason == "provider unavailable"
    assert result.sql is None


def test_blank_answer_is_refused_after_preserving_execution_evidence():
    provider = FakeProvider(
        plan=[plan()],
        draft=[draft()],
        answer=[AnswerDecision(answer="   ", caveats=())],
    )

    result = run(AgentLoop(provider, FakeGateway(FakeTools())))

    assert result.answer is None
    assert "empty text" in result.refusal_reason
    assert result.sql == draft().sql
    assert result.result_stats.bytes_read == 4096


def test_profile_wall_clock_deadline_forces_refusal(monkeypatch):
    monkeypatch.setitem(loop_module._PROFILE_TIMEOUTS, "fast", 0.001)
    provider = SlowProvider(plan=[plan()])

    result = run(AgentLoop(provider, FakeGateway(FakeTools())))

    assert result.answer is None
    assert "wall-clock limit" in result.refusal_reason
    assert result.sql is None


def test_missing_request_id_is_generated_once_and_reused_by_tools():
    provider = FakeProvider(
        plan=[plan()],
        draft=[draft()],
        answer=[AnswerDecision(answer="BTC is available.", caveats=())],
    )
    tools = FakeTools()

    result = run(
        AgentLoop(provider, FakeGateway(tools)),
        request_id=None,
    )

    assert result.request_id.startswith("agent-")
    explained = next(value for name, value in tools.calls if name == "explain_query")
    executed_call = next(
        value for name, value in tools.calls if name == "execute_query"
    )
    assert explained[1]["request_id"] == result.request_id
    assert executed_call[1]["request_id"] == result.request_id
