"""Versioned prompts for decisions inside the deterministic agent state machine."""

import json
from typing import Any

from ai_agent.agent_service.contracts import PROMPT_VERSION

SYSTEM_PROMPT = f"""You reason about SQL for a local crypto lakehouse agent.
Prompt version: {PROMPT_VERSION}.

The application, not you, owns control flow and safety. Follow these rules:
- Answer only from the supplied allow-listed Gold metadata and query results.
- Never invent tables, columns, values, dates, or successful tool outcomes.
- Draft exactly one fully qualified Trino SELECT. Do not use DDL, DML, procedures,
  session commands, multiple statements, SELECT INTO, or set-operation roots.
- Treat live schema as structural truth and dbt text as annotation.
- Treat sparse calendar coverage and intentional nulls as material limitations.
- Prefer refusal when the available Gold data cannot support the question.
- Keep structured fields concise. Do not include Markdown fences around SQL.
"""


def prompt(stage: str, payload: dict[str, Any]) -> str:
    """Serialize stage instructions and bounded context without string interpolation."""
    instruction = {
        "plan": (
            "Classify whether the question is answerable from this catalog. If it is, "
            "select one to three exact fully qualified table names. Request samples "
            "only when value formats cannot be inferred from schema/docs. Refuse "
            "unsupported entities, grains, or facts."
        ),
        "draft": (
            "Draft one fully qualified Trino SELECT that answers the question. Alias "
            "every returned expression with a stable descriptive column name, list "
            "those exact names in expected_columns, and state whether a correct result "
            "should contain at least one row. Use only supplied tables and columns."
        ),
        "critic": (
            "Judge whether the SQL and bounded result actually answer the question at "
            "the right grain and semantics. Fail on unsupported assumptions, wrong "
            "aggregation, missing filters, misleading handling of gaps/nulls, or "
            "undisclosed truncation."
        ),
        "answer": (
            "Write a concise answer using only the supplied bounded result. Preserve "
            "exact units and dates. Put limitations in caveats, especially truncation, "
            "sparse coverage, intentional nulls, or empty results. Do not claim "
            "completeness when truncated is true."
        ),
    }[stage]
    return (
        instruction
        + "\n\nINPUT_JSON:\n"
        + json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
    )
