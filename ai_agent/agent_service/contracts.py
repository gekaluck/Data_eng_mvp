"""Typed contracts for the one-shot natural-language analytics service."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ai_agent.mcp_server.budget import (
    MAX_REQUEST_ID_CHARS,
    REQUEST_ID_PATTERN,
    BudgetProfile,
)
from ai_agent.mcp_server.query_executor import QueryExecutionStats

MAX_QUESTION_CHARS = 4_000
PROMPT_VERSION = "agent-loop-v1"
FAST_MODEL_ID = "claude-sonnet-5"
THOROUGH_MODEL_ID = "claude-opus-5"


class AgentModel(BaseModel):
    """Immutable base for public agent contracts and LLM decisions."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class AgentRequest(AgentModel):
    """One stateless question submitted to the owned agent loop."""

    question: str = Field(min_length=1, max_length=MAX_QUESTION_CHARS)
    profile: BudgetProfile = "fast"
    request_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_REQUEST_ID_CHARS,
        pattern=REQUEST_ID_PATTERN,
    )

    @field_validator("question")
    @classmethod
    def question_must_contain_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("question must contain non-whitespace text")
        return value.strip()


class AgentResponse(AgentModel):
    """Terminal envelope shared by answers and visible refusals."""

    answer: str | None = None
    refusal_reason: str | None = None
    sql: str | None = None
    tables_used: tuple[str, ...] = ()
    result_stats: QueryExecutionStats | None = None
    caveats: tuple[str, ...] = ()
    confidence: tuple[str, ...] = ()
    profile: BudgetProfile
    request_id: str
    model_id: str
    prompt_version: str = PROMPT_VERSION

    @field_validator("answer", "refusal_reason")
    @classmethod
    def terminal_text_must_contain_text(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("terminal outcome text must contain non-whitespace text")
        return value.strip() if value is not None else None

    @model_validator(mode="after")
    def exactly_one_terminal_outcome(self) -> "AgentResponse":
        if (self.answer is None) == (self.refusal_reason is None):
            raise ValueError("Exactly one of answer or refusal_reason must be present.")
        return self


class PlanDecision(AgentModel):
    """Structured PLAN output: scope verdict and a bounded table shortlist."""

    disposition: Literal["answer", "refuse"]
    reason: str
    tables: tuple[str, ...]
    sample_tables: tuple[str, ...]


class SqlDraft(AgentModel):
    """One candidate query plus the result shape used by deterministic checks."""

    sql: str
    rationale: str
    expected_columns: tuple[str, ...]
    expects_rows: bool


class CriticDecision(AgentModel):
    """Semantic confidence verdict used only by the thorough profile."""

    verdict: Literal["pass", "fail"]
    reason: str


class AnswerDecision(AgentModel):
    """Natural-language answer derived only from the bounded query result."""

    answer: str
    caveats: tuple[str, ...]


class CatalogContext(AgentModel):
    """Bounded catalog metadata supplied to PLAN."""

    tables: tuple[dict[str, Any], ...]


class ExplorationContext(AgentModel):
    """Live schema, dbt docs, freshness, and optional samples for DRAFT."""

    tables: tuple[dict[str, Any], ...]
