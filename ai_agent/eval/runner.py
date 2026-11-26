"""Run golden cases through the real agent loop and collect pinned, graded results.

The harness composes `AgentLoop` in-process with the same provider and MCP HTTP gateway
the service uses (D044). In-process rather than over `POST /v1/questions` for two
reasons: the grader needs the executed rows, which only the full trace carries, and fault
injection has to sit on the MCP client boundary, which the HTTP route does not expose.
"""

import asyncio
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from ai_agent.agent_service.contracts import PROMPT_VERSION, AgentRequest
from ai_agent.agent_service.loop import AgentLoop
from ai_agent.agent_service.mcp_tools import ToolCallError, ToolGateway
from ai_agent.agent_service.provider import StructuredLlm
from ai_agent.eval.cases import GoldenCase, GoldenSet
from ai_agent.eval.faults import FaultInjectingGateway, MemoryTraceSink
from ai_agent.eval.grading import (
    GRADER_VERSION,
    ReferenceResult,
    TrialResult,
    grade_trial,
)
from ai_agent.mcp_server.allow_list import DEFAULT_ALLOW_LIST_PATH
from ai_agent.mcp_server.budget import BudgetProfile

REFERENCE_MAX_ROWS = 500


class RunPins(BaseModel):
    """Everything a result must be tied to before it counts as a result (§6)."""

    model_config = ConfigDict(frozen=True)

    run_id: str
    started_at: datetime
    profile: BudgetProfile
    model_id: str
    prompt_version: str
    allow_list_sha256: str
    golden_set_version: str
    golden_set_sha256: str
    grader_version: str
    git_commit: str
    git_dirty: bool
    table_snapshots: dict[str, int | None]
    trials: int
    splits: tuple[str, ...]
    case_ids: tuple[str, ...]


class PreflightFailure(BaseModel):
    model_config = ConfigDict(frozen=True)

    case_id: str
    reason: str


class EvalRun(BaseModel):
    model_config = ConfigDict(frozen=True)

    pins: RunPins
    preflight_failures: tuple[PreflightFailure, ...]
    trials: tuple[TrialResult, ...]


def allow_list_sha256(path: str | Path = DEFAULT_ALLOW_LIST_PATH) -> str:
    """Hash the allow-list by content, not bytes, so reformatting does not change it."""
    tables = json.loads(Path(path).read_text(encoding="utf-8"))["tables"]
    canonical = "\n".join(sorted(table.casefold() for table in tables))
    return hashlib.sha256(canonical.encode()).hexdigest()


def git_state() -> tuple[str, bool]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain", "--", "ai_agent"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        )
        return commit, dirty
    except (OSError, subprocess.CalledProcessError):
        return "unknown", True


async def table_snapshots(gateway: ToolGateway) -> dict[str, int | None]:
    """Current Iceberg snapshot id per allow-listed table: the data-version pin."""
    pins: dict[str, int | None] = {}
    async with gateway.session() as tools:
        for summary in await tools.list_tables():
            history = await tools.get_table_snapshots(summary.table, limit=1)
            pins[summary.table] = history.snapshots[0].snapshot_id if history.snapshots else None
    return pins


async def reference_results(
    gateway: ToolGateway,
    cases: tuple[GoldenCase, ...],
    *,
    run_id: str,
) -> tuple[dict[str, ReferenceResult], list[PreflightFailure]]:
    """Run each answer case's reference SQL through the same governed tool.

    A reference that errors, is empty, or hits the row cap is a broken *case* (often a
    date that fell into a coverage gap), so the case is skipped rather than scored.
    """
    references: dict[str, ReferenceResult] = {}
    failures: list[PreflightFailure] = []
    async with gateway.session() as tools:
        for case in cases:
            if case.reference_sql is None:
                continue
            try:
                result = await tools.execute_query(
                    case.reference_sql,
                    request_id=f"eval-ref-{run_id}-{case.id}",
                    profile="thorough",
                    max_rows=REFERENCE_MAX_ROWS,
                )
            except ToolCallError as exc:
                failures.append(PreflightFailure(case_id=case.id, reason=f"{exc.code}: {exc.message}"))
                continue
            if not result.rows:
                failures.append(PreflightFailure(case_id=case.id, reason="reference returned no rows"))
                continue
            if result.truncated:
                failures.append(PreflightFailure(case_id=case.id, reason="reference hit the row cap"))
                continue
            references[case.id] = ReferenceResult(
                columns=result.columns, rows=result.rows, truncated=result.truncated
            )
    return references, failures


async def run_cases(
    golden: GoldenSet,
    cases: tuple[GoldenCase, ...],
    *,
    provider: StructuredLlm,
    gateway: ToolGateway,
    profile: BudgetProfile,
    trials: int,
    splits: tuple[str, ...],
    progress=print,
) -> EvalRun:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    commit, dirty = git_state()
    snapshots = await table_snapshots(gateway)
    references, failures = await reference_results(gateway, cases, run_id=run_id)
    skipped = {failure.case_id for failure in failures}
    for failure in failures:
        progress(f"preflight skip {failure.case_id}: {failure.reason}")

    pins = RunPins(
        run_id=run_id,
        started_at=datetime.now(timezone.utc),
        profile=profile,
        model_id=provider.model_id(profile),
        prompt_version=PROMPT_VERSION,
        allow_list_sha256=allow_list_sha256(),
        golden_set_version=golden.version,
        golden_set_sha256=golden.content_sha256,
        grader_version=GRADER_VERSION,
        git_commit=commit,
        git_dirty=dirty,
        table_snapshots=snapshots,
        trials=trials,
        splits=splits,
        case_ids=tuple(case.id for case in cases),
    )

    results: list[TrialResult] = []
    for trial in range(1, trials + 1):
        for case in cases:
            if case.id in skipped:
                continue
            # A unique id per (case, trial, run) keeps every MCP budget cold (runbook).
            request_id = f"eval-{run_id}-{case.id}-t{trial}"
            sink = MemoryTraceSink()
            faulty = FaultInjectingGateway(gateway, case.fault)
            loop = AgentLoop(provider, faulty, trace_sink=sink)
            response = await loop.answer(
                AgentRequest(question=case.question, profile=profile, request_id=request_id)
            )
            result = grade_trial(
                case,
                trial=trial,
                response=response,
                entries=sink.entries,
                fault_log=faulty.log,
                reference=references.get(case.id),
            )
            results.append(result)
            progress(f"t{trial} {case.id}: {result.verdict}")
    return EvalRun(pins=pins, preflight_failures=tuple(failures), trials=tuple(results))


# ------------------------------------------------------------------ local MCP process


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@asynccontextmanager
async def local_mcp_server() -> AsyncIterator[str]:
    """Start a fresh MCP HTTP server for one run, so every budget starts empty."""
    port = _free_port()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "ai_agent.mcp_server",
        "--transport",
        "streamable-http",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        env=os.environ.copy(),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 20
        while True:
            if process.returncode is not None:
                raise RuntimeError(f"MCP server exited with code {process.returncode}.")
            try:
                await asyncio.to_thread(
                    lambda: socket.create_connection(("127.0.0.1", port), 0.2).close()
                )
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("MCP server did not start within 20 seconds.") from None
                await asyncio.sleep(0.2)
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()
            await process.wait()


def write_run(run: EvalRun, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = run.model_dump(mode="json")
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
