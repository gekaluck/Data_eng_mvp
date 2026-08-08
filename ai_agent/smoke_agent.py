"""Live agent-loop smoke check with real MCP/Trino and no hosted-model call."""

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Sequence

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
            raise RuntimeError(f"Unexpected smoke output type for {stage}.")
        return decision


async def smoke() -> dict[str, object]:
    """Start MCP, run both profiles, and return a compact verification report."""
    port = _free_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ai_agent.mcp_server",
            "--transport",
            "streamable-http",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        env=os.environ.copy(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(process, port)
        loop = AgentLoop(
            ScriptedSmokeProvider(),
            McpHttpGateway(f"http://127.0.0.1:{port}/mcp"),
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
        return report
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _wait_for_port(process: subprocess.Popen[bytes], port: int) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"Streamable HTTP server exited with code {process.returncode}."
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("Streamable HTTP server did not start within 15 seconds.")


def main(argv: Sequence[str] | None = None) -> None:
    if argv:
        raise SystemExit("smoke_agent accepts no arguments")
    print(json.dumps(asyncio.run(smoke()), indent=2))


if __name__ == "__main__":
    main()
