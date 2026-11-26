"""Versioned golden-set cases: questions, reference SQL, expectations, and faults.

A golden set is two JSON files with the same version: `dev.json`, which may be read
while changing prompts or checks, and `heldout.json`, which is only ever *scored*.
Keeping them in separate files makes the rule mechanical: if a diff touches the agent and
the author opened `heldout.json` to decide what to change, the held-out number stops
meaning anything.
"""

import hashlib
import json
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

DEFAULT_CASES_DIR = Path(__file__).parent / "cases"
SPLITS = ("dev", "heldout")
Split = Literal["dev", "heldout"]
Category = Literal[
    "answerable",
    "business_trap",
    "unanswerable",
    "nonexistent_column",
    "permission_denial",
    "schema_drift",
    "query_limit",
    "retry",
    "partial_failure",
]
ToolName = Literal[
    "list_tables",
    "get_table_schema",
    "get_table_snapshots",
    "get_model_docs",
    "sample_rows",
    "explain_query",
    "execute_query",
]


class EvalModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ToolErrorFault(EvalModel):
    """Make chosen calls of one MCP tool fail with a structured error.

    Used for transient engine errors (retry), budget/scan exhaustion (query limits),
    engine-level access denial (permission), and a metadata tool dying mid-run
    (partial failure). `on_calls` counts calls of this tool within one request, from 1.
    """

    kind: Literal["tool_error"] = "tool_error"
    tool: ToolName
    code: str
    message: str
    retryable: bool = False
    on_calls: tuple[int, ...] = (1,)


class StaleDocsColumnFault(EvalModel):
    """dbt docs advertise a column that the live Iceberg schema does not have.

    This is the common real-world drift: someone renamed or dropped a column and the
    YAML was not updated. The agent is told live schema is structural truth.
    """

    kind: Literal["stale_docs_column"] = "stale_docs_column"
    table: str
    column: str
    description: str


class DropColumnAtExecuteFault(EvalModel):
    """The column existed during EXPLORE/VALIDATE but is gone when the query runs.

    Simulates a schema change landing mid-request. Any `execute_query` whose SQL
    references the column fails with Trino's COLUMN_NOT_FOUND as a non-retryable
    ENGINE_ERROR.
    """

    kind: Literal["drop_column_at_execute"] = "drop_column_at_execute"
    table: str
    column: str


Fault = Annotated[
    ToolErrorFault | StaleDocsColumnFault | DropColumnAtExecuteFault,
    Field(discriminator="kind"),
]


class GoldenCase(EvalModel):
    """One question and everything needed to score it without a human."""

    id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,63}$")
    split: Split
    category: Category
    question: str = Field(min_length=1)
    expected_outcome: Literal["answer", "refusal"]
    # Why the case exists and what a wrong answer looks like. Required: a case whose
    # purpose is not written down cannot be maintained.
    intent: str = Field(min_length=1)

    reference_sql: str | None = None
    acceptable_tables: tuple[str, ...] = ()
    ordered: bool = False
    float_rel_tol: float = Field(default=1e-3, ge=0)
    float_abs_tol: float = Field(default=0.01, ge=0)
    # Row-cap cases: when the agent's result is truncated, score it as correct if every
    # returned row is a reference row (the reference itself must fit under 500 rows).
    allow_truncated_subset: bool = False
    # Case-sensitive substrings the answer text must contain (advisory check).
    answer_must_mention: tuple[str, ...] = ()

    # Environment expectations. None means "not checked for this case".
    expect_retry: bool | None = None
    expect_truncated: bool | None = None
    expect_no_execution: bool | None = None
    expect_error_codes: tuple[str, ...] = ()

    fault: Fault | None = None
    # For refusal cases with a fault: the SQL a reasonable agent would *try* before the
    # fault stops it. Only the no-API oracle uses it, so the harness can prove the fault
    # is reachable; it is never ground truth.
    probe_sql: str | None = None

    @model_validator(mode="after")
    def answer_cases_need_ground_truth(self) -> "GoldenCase":
        if self.expected_outcome == "answer":
            if not self.reference_sql or not self.acceptable_tables:
                raise ValueError(
                    f"{self.id}: answer cases need reference_sql and acceptable_tables."
                )
            if self.probe_sql is not None:
                raise ValueError(f"{self.id}: answer cases use reference_sql, not probe_sql.")
        elif self.reference_sql is not None:
            raise ValueError(f"{self.id}: refusal cases must not carry reference_sql.")
        elif self.fault is not None and self.probe_sql is None and not (
            isinstance(self.fault, ToolErrorFault) and self.fault.tool == "list_tables"
        ):
            raise ValueError(f"{self.id}: refusal cases with a fault need probe_sql.")
        return self


class GoldenSetFile(EvalModel):
    version: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}\.\d+$")
    split: Split
    cases: tuple[GoldenCase, ...]

    @model_validator(mode="after")
    def cases_belong_to_split(self) -> "GoldenSetFile":
        wrong = [case.id for case in self.cases if case.split != self.split]
        if wrong:
            raise ValueError(f"{self.split}.json holds cases from another split: {wrong}")
        return self


class GoldenSet(EvalModel):
    """The loaded set plus the content hash that pins exactly which cases ran."""

    version: str
    content_sha256: str
    cases: tuple[GoldenCase, ...]

    def select(
        self,
        *,
        splits: tuple[str, ...] = SPLITS,
        case_ids: tuple[str, ...] = (),
    ) -> tuple[GoldenCase, ...]:
        chosen = tuple(case for case in self.cases if case.split in splits)
        if case_ids:
            unknown = set(case_ids) - {case.id for case in chosen}
            if unknown:
                raise ValueError(f"Unknown case ids for the chosen splits: {sorted(unknown)}")
            chosen = tuple(case for case in chosen if case.id in case_ids)
        return chosen


def load_golden_set(directory: str | Path = DEFAULT_CASES_DIR) -> GoldenSet:
    """Load both splits, refuse version skew or duplicate ids, and hash the content."""
    directory = Path(directory)
    files: list[GoldenSetFile] = []
    digest = hashlib.sha256()
    for split in SPLITS:
        raw = (directory / f"{split}.json").read_bytes()
        digest.update(split.encode() + b"\0" + raw + b"\0")
        files.append(GoldenSetFile.model_validate(json.loads(raw)))

    versions = {file.version for file in files}
    if len(versions) != 1:
        raise ValueError(f"dev and heldout versions differ: {sorted(versions)}")
    cases = tuple(case for file in files for case in file.cases)
    ids = [case.id for case in cases]
    duplicates = sorted({case_id for case_id in ids if ids.count(case_id) > 1})
    if duplicates:
        raise ValueError(f"Duplicate golden case ids: {duplicates}")
    return GoldenSet(
        version=versions.pop(),
        content_sha256=digest.hexdigest(),
        cases=cases,
    )
