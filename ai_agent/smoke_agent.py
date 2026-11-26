"""Live agent-loop smoke check with real MCP/Trino and no hosted-model call."""

import asyncio
import json
import os
import socket
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory

from ai_agent.agent_service.contracts import (
    FAST_MODEL_ID,
    THOROUGH_MODEL_ID,
    AgentRequest,
    AnswerDecision,
    CriticDecision,
    PlanDecision,
    SqlDraft,
)
from ai_agent.agent_service.loop import AgentLoop
from ai_agent.agent_service.mcp_tools import McpHttpGateway
from ai_agent.agent_service.provider import LlmGeneration, LlmUsage
from ai_agent.agent_service.tracing import JsonlAgentTrace

TABLE = "gold.crypto_dbt.daily_snapshot"


class ScriptedSmokeProvider:
    """Deterministic decisions that verify orchestration without API spend."""

    def model_id(self, profile):
        pinned = FAST_MODEL_ID if profile == "fast" else THOROUGH_MODEL_ID
        return f"scripted-no-api/{pinned}"

    async def generate(self, *, stage, output_type, **kwargs):
        decisions = {
            "plan": PlanDecision(
                disposition="answer",
                reason="The allow-listed daily snapshot contains symbols.",
                tables=(TABLE,),
                sample_tables=(),
            ),
            "draft": SqlDraft(
                sql=f"SELECT symbol FROM {TABLE} ORDER BY symbol LIMIT 2",
                rationale="Return a deterministic bounded symbol list.",
                expected_columns=("symbol",),
                expects_rows=True,
            ),
            "critic": CriticDecision(
                verdict="pass",
                reason="The query and result match the smoke question.",
            ),
            "answer": AnswerDecision(
                answer="The governed agent path returned two symbols.",
                caveats=(),
            ),
        }
        decision = decisions[stage]
        if not isinstance(decision, output_type):
            raise TypeError(f"Unexpected smoke output type for {stage}.")
        return LlmGeneration(
            output=decision,
            response_id=f"scripted-{stage}",
            stop_reason="end_turn",
            usage=LlmUsage(input_tokens=0, output_tokens=0),
        )


async def smoke() -> dict[str, object]:
    """Start MCP, run both profiles, and return a compact verification report."""
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
        await _wait_for_port(process, port)
        with TemporaryDirectory(prefix="agent-smoke-trace-") as trace_directory:
            trace_path = Path(trace_directory) / "agent-trace.jsonl"
            loop = AgentLoop(
                ScriptedSmokeProvider(),
                McpHttpGateway(f"http://127.0.0.1:{port}/mcp"),
                trace_sink=JsonlAgentTrace(
                    trace_path,
                    level="full",
                    log_level="CRITICAL",
                ),
            )
            report: dict[str, object] = {"hosted_model_called": False}
            for profile in ("fast", "thorough"):
                response = await loop.answer(
                    AgentRequest(
                        question="Return two available crypto symbols.",
                        profile=profile,
                        request_id=f"agent-smoke-{profile}",
                    )
                )
                if response.answer is None or response.result_stats is None:
                    raise RuntimeError(f"Agent {profile} smoke refused: {response}")
                report[profile] = {
                    "answer": response.answer,
                    "sql": response.sql,
                    "tables": response.tables_used,
                    "stats": response.result_stats.model_dump(mode="json"),
                    "confidence": response.confidence,
                    "caveats": len(response.caveats),
                    "provider": response.model_id,
                }

            trace_entries = [
                json.loads(line)
                for line in trace_path.read_text(encoding="utf-8").splitlines()
            ]
            trace_report = _trace_report(trace_entries)
            if not all(
                trace_report[key]
                for key in (
                    "contains_full_question",
                    "contains_model_prompts",
                    "contains_returned_rows",
                    "contains_terminal_answers",
                )
            ):
                raise RuntimeError(f"Agent smoke trace is incomplete: {trace_report}")
            report["trace"] = trace_report
            return report
    finally:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()
            await process.wait()


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def _wait_for_port(process: asyncio.subprocess.Process, port: int) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.returncode is not None:
            raise RuntimeError(
                f"Streamable HTTP server exited with code {process.returncode}."
            )
        try:
            await asyncio.to_thread(_connect_to_port, port)
            return
        except OSError:
            await asyncio.sleep(0.1)
    raise TimeoutError("Streamable HTTP server did not start within 15 seconds.")


def _connect_to_port(port: int) -> None:
    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
        return


def _trace_report(entries: list[dict[str, object]]) -> dict[str, object]:
    request_ids = sorted({str(entry["request_id"]) for entry in entries})
    query_ids = sorted(
        {
            str(metadata["query_id"])
            for entry in entries
            if isinstance((metadata := entry.get("metadata")), dict)
            and metadata.get("query_id")
        }
    )
    return {
        "event_count": len(entries),
        "request_ids": request_ids,
        "query_ids": query_ids,
        "contains_full_question": any(
            entry.get("event") == "request.started"
            and "Return two available crypto symbols."
            in json.dumps(entry.get("content"))
            for entry in entries
        ),
        "contains_model_prompts": any(
            entry.get("event") == "llm.started"
            and "INPUT_JSON" in json.dumps(entry.get("content"))
            for entry in entries
        ),
        "contains_returned_rows": any(
            entry.get("event") == "tool.completed"
            and isinstance(entry.get("metadata"), dict)
            and entry["metadata"].get("tool") == "execute_query"
            and '"rows"' in json.dumps(entry.get("content"))
            for entry in entries
        ),
        "contains_terminal_answers": sum(
            entry.get("event") == "request.completed"
            and '"answer"' in json.dumps(entry.get("content"))
            for entry in entries
        )
        == 2,
    }


def main(argv: Sequence[str] | None = None) -> None:
    if argv:
        raise SystemExit("smoke_agent accepts no arguments")
    print(json.dumps(asyncio.run(smoke()), indent=2))


if __name__ == "__main__":
    main()
