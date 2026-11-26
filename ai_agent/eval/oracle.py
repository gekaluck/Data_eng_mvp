"""A no-API provider that answers every case with its own reference SQL.

The oracle exists to test the *harness*, not the agent. Run against the live stack it
must score 100% on every conclusive case; anything less is a bug in a case, a reference,
a fault wiring, or a grader false negative — found without spending a single token.
"""

import json

from sqlglot import exp, parse_one

from ai_agent.agent_service.contracts import (
    FAST_MODEL_ID,
    THOROUGH_MODEL_ID,
    AnswerDecision,
    CriticDecision,
    PlanDecision,
    SqlDraft,
)
from ai_agent.agent_service.provider import LlmGeneration, LlmUsage
from ai_agent.eval.cases import GoldenCase

ORACLE_PREFIX = "oracle-no-api/"


def reference_tables(sql: str) -> tuple[str, ...]:
    """Fully qualified physical tables a reference query reads (CTE names excluded)."""
    tree = parse_one(sql, read="trino")
    ctes = {cte.alias_or_name.casefold() for cte in tree.find_all(exp.CTE)}
    tables = []
    for table in tree.find_all(exp.Table):
        if table.name.casefold() in ctes and not table.db:
            continue
        name = ".".join(part for part in (table.catalog, table.db, table.name) if part)
        if name.casefold() not in (t.casefold() for t in tables):
            tables.append(name.casefold())
    return tuple(tables)


def reference_columns(sql: str) -> tuple[str, ...]:
    return tuple(parse_one(sql, read="trino").named_selects)


class OracleProvider:
    """StructuredLlm that looks the question up in the golden set."""

    def __init__(self, cases: tuple[GoldenCase, ...]) -> None:
        self._by_question = {case.question: case for case in cases}

    def model_id(self, profile) -> str:
        return ORACLE_PREFIX + (FAST_MODEL_ID if profile == "fast" else THOROUGH_MODEL_ID)

    async def generate(self, *, stage, prompt, output_type, **kwargs):
        case = self._case_for(prompt)
        # A refusal case with a planted fault is attempted with its probe SQL, so the
        # fault is actually reached; otherwise the oracle refuses at PLAN.
        sql = case.reference_sql or case.probe_sql
        if stage == "plan":
            if sql is None:
                output = PlanDecision(
                    disposition="refuse",
                    reason="Oracle: the golden set marks this question unanswerable.",
                    tables=(),
                    sample_tables=(),
                )
            else:
                output = PlanDecision(
                    disposition="answer",
                    reason="Oracle: reference SQL tables.",
                    tables=reference_tables(sql),
                    sample_tables=(),
                )
        elif stage == "draft":
            output = SqlDraft(
                sql=sql,
                rationale="Oracle: the case's reference or probe SQL.",
                expected_columns=reference_columns(sql),
                expects_rows=True,
            )
        elif stage == "critic":
            output = CriticDecision(verdict="pass", reason="Oracle: reference SQL.")
        else:
            output = AnswerDecision(answer="Oracle answer from the reference result.", caveats=())
        if not isinstance(output, output_type):
            raise TypeError(f"Oracle produced {type(output).__name__} for {stage}.")
        return LlmGeneration(
            output=output,
            response_id=f"oracle-{stage}",
            stop_reason="end_turn",
            usage=LlmUsage(input_tokens=0, output_tokens=0),
        )

    def _case_for(self, prompt: str) -> GoldenCase:
        # The question is serialized inside INPUT_JSON; match on the exact text.
        for question, case in self._by_question.items():
            if _json_string(question) in prompt:
                return case
        raise LookupError("Oracle received a question that is not in the golden set.")


def _json_string(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)
