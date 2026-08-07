"""Bounded state machine for one-shot natural-language lakehouse questions."""

import asyncio
from collections.abc import Callable
from time import monotonic
from typing import Any
from uuid import uuid4

from ai_agent.agent_service.contracts import (
    AgentRequest,
    AgentResponse,
    AnswerDecision,
    CatalogContext,
    CriticDecision,
    ExplorationContext,
    PlanDecision,
    SqlDraft,
)
from ai_agent.agent_service.mcp_tools import (
    AgentTools,
    ToolCallError,
    ToolGateway,
)
from ai_agent.agent_service.prompts import SYSTEM_PROMPT, prompt
from ai_agent.agent_service.provider import ProviderError, StructuredLlm
from ai_agent.mcp_server.budget import BudgetProfile
from ai_agent.mcp_server.query_executor import ExecutedQuery

_PROFILE_TIMEOUTS: dict[BudgetProfile, float] = {"fast": 30.0, "thorough": 120.0}
_PROFILE_ATTEMPTS: dict[BudgetProfile, int] = {"fast": 2, "thorough": 4}
_PROFILE_SAMPLE_CAPS: dict[BudgetProfile, int] = {"fast": 1, "thorough": 2}
MAX_PLAN_TABLES = 3
MAX_EXPECTED_COLUMNS = 20


class LoopDeadlineExceeded(Exception):
    """The profile wall-clock deadline expired between deterministic states."""


class AgentLoop:
    """Own loop transitions while the MCP layer continues to own enforcement."""

    def __init__(
        self,
        provider: StructuredLlm,
        gateway: ToolGateway,
        *,
        timer: Callable[[], float] = monotonic,
    ) -> None:
        self._provider = provider
        self._gateway = gateway
        self._timer = timer

    async def answer(self, request: AgentRequest) -> AgentResponse:
        """Run one cold question to ANSWER or REFUSE within its profile deadline."""
        request_id = request.request_id or f"agent-{uuid4()}"
        deadline = self._timer() + _PROFILE_TIMEOUTS[request.profile]
        confidence: list[str] = []
        caveats: list[str] = []
        last_sql: str | None = None
        tables_used: tuple[str, ...] = ()
        result: ExecutedQuery | None = None

        try:
            async with self._gateway.session() as tools:
                catalog = await self._within(deadline, tools.list_tables())
                plan = await self._llm(
                    deadline,
                    profile=request.profile,
                    stage="plan",
                    payload={
                        "question": request.question,
                        "catalog": CatalogContext(
                            tables=tuple(
                                table.model_dump(mode="json") for table in catalog
                            )
                        ).model_dump(mode="json"),
                    },
                    output_type=PlanDecision,
                )
                allowed = {table.table.casefold(): table.table for table in catalog}
                plan_error = self._plan_error(plan, allowed, request.profile)
                if plan_error:
                    return self._refusal(
                        request,
                        request_id,
                        plan_error,
                        confidence=confidence,
                    )
                if plan.disposition == "refuse":
                    return self._refusal(
                        request,
                        request_id,
                        plan.reason,
                        confidence=confidence,
                    )
                confidence.extend(
                    ("question_classified_answerable", "tables_allowed_by_catalog")
                )

                selected = tuple(allowed[table.casefold()] for table in plan.tables)
                exploration = await self._explore(
                    tools,
                    selected,
                    sample_tables={table.casefold() for table in plan.sample_tables},
                    request_id=request_id,
                    profile=request.profile,
                    deadline=deadline,
                )
                confidence.append("live_schema_loaded")

                feedback: str | None = None
                attempts = _PROFILE_ATTEMPTS[request.profile]
                for attempt in range(1, attempts + 1):
                    draft = await self._llm(
                        deadline,
                        profile=request.profile,
                        stage="draft",
                        payload={
                            "question": request.question,
                            "selected_tables": selected,
                            "exploration": exploration.model_dump(mode="json"),
                            "prior_failure": feedback,
                            "attempt": attempt,
                            "maximum_attempts": attempts,
                        },
                        output_type=SqlDraft,
                    )
                    last_sql = draft.sql.strip()
                    draft_error = self._draft_error(draft)
                    if draft_error:
                        feedback = draft_error
                        continue

                    try:
                        explained = await self._within(
                            deadline,
                            tools.explain_query(
                                last_sql,
                                request_id=request_id,
                                profile=request.profile,
                            ),
                        )
                    except ToolCallError as exc:
                        if not self._can_redraft(exc, attempt, attempts):
                            return self._tool_refusal(
                                request,
                                request_id,
                                exc,
                                sql=last_sql,
                                confidence=confidence,
                            )
                        feedback = self._tool_feedback("VALIDATE", exc)
                        continue
                    if not explained.valid:
                        diagnostic = explained.diagnostic
                        feedback = "VALIDATE failed: " + (
                            f"{diagnostic.code}: {diagnostic.message}"
                            if diagnostic
                            else "Trino rejected the query semantics."
                        )
                        if attempt == attempts:
                            break
                        continue
                    confidence.append("sql_planned_by_trino")

                    try:
                        result = await self._within(
                            deadline,
                            tools.execute_query(
                                last_sql,
                                request_id=request_id,
                                profile=request.profile,
                            ),
                        )
                    except ToolCallError as exc:
                        if not self._can_redraft(exc, attempt, attempts):
                            return self._tool_refusal(
                                request,
                                request_id,
                                exc,
                                sql=last_sql,
                                confidence=confidence,
                            )
                        feedback = self._tool_feedback("EXECUTE", exc)
                        continue

                    tables_used = result.tables
                    check_error, passed = self._check_result(
                        draft,
                        result,
                        planned_tables=selected,
                    )
                    confidence.extend(
                        check for check in passed if check not in confidence
                    )
                    if check_error:
                        feedback = check_error
                        if attempt == attempts:
                            break
                        continue
                    caveats = list(result.caveats)
                    if result.truncated:
                        caveats.insert(
                            0,
                            "The result was truncated at the agent's 100-row "
                            "response cap.",
                        )

                    if request.profile == "thorough":
                        critic = await self._llm(
                            deadline,
                            profile=request.profile,
                            stage="critic",
                            payload=self._result_payload(
                                request.question,
                                last_sql,
                                exploration,
                                result,
                            ),
                            output_type=CriticDecision,
                        )
                        if critic.verdict == "fail":
                            feedback = f"CRITIC failed: {critic.reason}"
                            if attempt == attempts:
                                break
                            continue
                        confidence.append("critic_passed")

                    answered = await self._llm(
                        deadline,
                        profile=request.profile,
                        stage="answer",
                        payload=self._result_payload(
                            request.question,
                            last_sql,
                            exploration,
                            result,
                        ),
                        output_type=AnswerDecision,
                    )
                    if not answered.answer.strip():
                        return self._refusal(
                            request,
                            request_id,
                            "ANSWER returned empty text and was rejected.",
                            sql=last_sql,
                            tables_used=tables_used,
                            result=result,
                            caveats=caveats,
                            confidence=confidence,
                        )
                    caveats.extend(
                        caveat for caveat in answered.caveats if caveat not in caveats
                    )
                    return AgentResponse(
                        answer=answered.answer,
                        sql=last_sql,
                        tables_used=tables_used,
                        result_stats=result.stats,
                        caveats=tuple(caveats),
                        confidence=tuple(confidence),
                        profile=request.profile,
                        request_id=request_id,
                        model_id=self._provider.model_id(request.profile),
                    )

                return self._refusal(
                    request,
                    request_id,
                    feedback or "The bounded retry limit was exhausted.",
                    sql=last_sql,
                    tables_used=tables_used,
                    result=result,
                    caveats=caveats,
                    confidence=confidence,
                )
        except LoopDeadlineExceeded:
            return self._refusal(
                request,
                request_id,
                f"The {request.profile} profile wall-clock limit was exhausted.",
                sql=last_sql,
                tables_used=tables_used,
                result=result,
                caveats=caveats,
                confidence=confidence,
            )
        except ToolCallError as exc:
            return self._tool_refusal(
                request,
                request_id,
                exc,
                sql=last_sql,
                confidence=confidence,
            )
        except ProviderError as exc:
            return self._refusal(
                request,
                request_id,
                str(exc),
                sql=last_sql,
                tables_used=tables_used,
                result=result,
                caveats=caveats,
                confidence=confidence,
            )
        except Exception as exc:
            return self._refusal(
                request,
                request_id,
                f"Agent infrastructure failed: {exc}",
                sql=last_sql,
                tables_used=tables_used,
                result=result,
                caveats=caveats,
                confidence=confidence,
            )

    async def _explore(
        self,
        tools: AgentTools,
        tables: tuple[str, ...],
        *,
        sample_tables: set[str],
        request_id: str,
        profile: BudgetProfile,
        deadline: float,
    ) -> ExplorationContext:
        explored: list[dict[str, Any]] = []
        for table in tables:
            docs = await self._within(deadline, tools.get_model_docs(table))
            schema = await self._within(deadline, tools.get_table_schema(table))
            snapshots = await self._within(
                deadline, tools.get_table_snapshots(table, limit=2)
            )
            sample = None
            if table.casefold() in sample_tables:
                sampled = await self._within(
                    deadline,
                    tools.sample_rows(
                        table,
                        n=3,
                        request_id=request_id,
                        profile=profile,
                    ),
                )
                sample = sampled.model_dump(mode="json")
            explored.append(
                {
                    "table": table,
                    "live_schema": schema.model_dump(mode="json"),
                    "dbt_docs": docs.model_dump(mode="json"),
                    "recent_snapshots": snapshots.model_dump(mode="json"),
                    "sample": sample,
                }
            )
        return ExplorationContext(tables=tuple(explored))

    async def _llm(
        self,
        deadline: float,
        *,
        profile: BudgetProfile,
        stage: str,
        payload: dict[str, Any],
        output_type: type[Any],
    ) -> Any:
        return await self._within(
            deadline,
            self._provider.generate(
                profile=profile,
                stage=stage,
                system=SYSTEM_PROMPT,
                prompt=prompt(stage, payload),
                output_type=output_type,
            ),
        )

    async def _within(self, deadline: float, operation: Any) -> Any:
        remaining = deadline - self._timer()
        if remaining <= 0:
            if hasattr(operation, "close"):
                operation.close()
            raise LoopDeadlineExceeded
        try:
            async with asyncio.timeout(remaining):
                return await operation
        except TimeoutError as exc:
            raise LoopDeadlineExceeded from exc

    @staticmethod
    def _plan_error(
        plan: PlanDecision,
        allowed: dict[str, str],
        profile: BudgetProfile,
    ) -> str | None:
        if not plan.reason.strip():
            return "PLAN returned an empty reason."
        if plan.disposition == "refuse":
            if plan.tables or plan.sample_tables:
                return "PLAN refusal included tables and was rejected as inconsistent."
            return None
        if not 1 <= len(plan.tables) <= MAX_PLAN_TABLES:
            return f"PLAN must select between one and {MAX_PLAN_TABLES} tables."
        normalized = [table.casefold() for table in plan.tables]
        if len(set(normalized)) != len(normalized) or any(
            table not in allowed for table in normalized
        ):
            return "PLAN selected an unknown or duplicate table."
        samples = [table.casefold() for table in plan.sample_tables]
        if (
            len(set(samples)) != len(samples)
            or any(table not in normalized for table in samples)
            or len(samples) > _PROFILE_SAMPLE_CAPS[profile]
        ):
            return "PLAN requested invalid or excessive row samples."
        return None

    @staticmethod
    def _draft_error(draft: SqlDraft) -> str | None:
        if not draft.sql.strip():
            return "DRAFT returned empty SQL."
        if not 1 <= len(draft.expected_columns) <= MAX_EXPECTED_COLUMNS:
            return (
                "DRAFT must declare between one and "
                f"{MAX_EXPECTED_COLUMNS} expected columns."
            )
        normalized = [column.casefold() for column in draft.expected_columns]
        if len(set(normalized)) != len(normalized):
            return "DRAFT declared duplicate expected columns."
        return None

    @staticmethod
    def _check_result(
        draft: SqlDraft,
        result: ExecutedQuery,
        *,
        planned_tables: tuple[str, ...],
    ) -> tuple[str | None, tuple[str, ...]]:
        passed = ["execution_succeeded"]
        actual_columns = tuple(column.casefold() for column in result.columns)
        expected_columns = tuple(column.casefold() for column in draft.expected_columns)
        if expected_columns != actual_columns:
            return (
                "CHECK failed: result columns do not exactly match the declared "
                "expected columns.",
                tuple(passed),
            )
        passed.append("result_shape_matches_draft")
        planned = {table.casefold() for table in planned_tables}
        executed_tables = {table.casefold() for table in result.tables}
        if not executed_tables or not executed_tables.issubset(planned):
            return (
                "CHECK failed: execution used a table outside the approved plan.",
                tuple(passed),
            )
        passed.append("execution_matches_planned_tables")
        if draft.expects_rows and not result.rows:
            return (
                "CHECK failed: the question implied data should exist but execution "
                "returned no rows.",
                tuple(passed),
            )
        passed.append("result_nonempty" if result.rows else "empty_result_allowed")
        passed.append("truncation_disclosed")
        return None, tuple(passed)

    @staticmethod
    def _can_redraft(exc: ToolCallError, attempt: int, maximum: int) -> bool:
        if attempt >= maximum or exc.code == "BUDGET_EXCEEDED":
            return False
        if exc.code == "ENGINE_ERROR" and not exc.retryable:
            return False
        return True

    @staticmethod
    def _tool_feedback(state: str, exc: ToolCallError) -> str:
        detail = f"{state} failed with {exc.code}: {exc.message}"
        return detail + (f" Hint: {exc.hint}" if exc.hint else "")

    @staticmethod
    def _result_payload(
        question: str,
        sql: str,
        exploration: ExplorationContext,
        result: ExecutedQuery,
    ) -> dict[str, Any]:
        return {
            "question": question,
            "sql": sql,
            "result": result.model_dump(mode="json"),
            "schemas_used": exploration.model_dump(mode="json"),
        }

    def _tool_refusal(
        self,
        request: AgentRequest,
        request_id: str,
        exc: ToolCallError,
        *,
        sql: str | None,
        confidence: list[str],
    ) -> AgentResponse:
        return self._refusal(
            request,
            request_id,
            self._tool_feedback("MCP", exc),
            sql=sql,
            confidence=confidence,
        )

    def _refusal(
        self,
        request: AgentRequest,
        request_id: str,
        reason: str,
        *,
        sql: str | None = None,
        tables_used: tuple[str, ...] = (),
        result: ExecutedQuery | None = None,
        caveats: list[str] | None = None,
        confidence: list[str] | None = None,
    ) -> AgentResponse:
        return AgentResponse(
            refusal_reason=reason,
            sql=sql,
            tables_used=tables_used,
            result_stats=result.stats if result else None,
            caveats=tuple(caveats or ()),
            confidence=tuple(confidence or ()),
            profile=request.profile,
            request_id=request_id,
            model_id=self._provider.model_id(request.profile),
        )
