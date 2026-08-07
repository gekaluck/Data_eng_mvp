"""Loopback HTTP frontend for the owned one-shot analytics agent."""

import argparse
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from ai_agent.agent_service.contracts import AgentRequest, AgentResponse
from ai_agent.agent_service.loop import AgentLoop
from ai_agent.agent_service.mcp_tools import McpHttpGateway
from ai_agent.agent_service.provider import AnthropicStructuredLlm

DEFAULT_AGENT_HOST = "127.0.0.1"
DEFAULT_AGENT_PORT = 8010
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


@dataclass(frozen=True, slots=True)
class AgentHttpSettings:
    """Validated local-only HTTP settings for the trusted-user threat model."""

    host: str = DEFAULT_AGENT_HOST
    port: int = DEFAULT_AGENT_PORT

    def __post_init__(self) -> None:
        if self.host not in _LOOPBACK_HOSTS:
            raise ValueError(
                "AI agent HTTP currently supports loopback hosts only; remote exposure "
                "requires authentication and a revised threat model."
            )
        if not isinstance(self.port, int) or isinstance(self.port, bool):
            raise ValueError("AI agent HTTP port must be an integer.")
        if not 1 <= self.port <= 65_535:
            raise ValueError("AI agent HTTP port must be between 1 and 65535.")

    @classmethod
    def from_env(cls) -> "AgentHttpSettings":
        try:
            port = int(os.getenv("AI_AGENT_PORT", str(DEFAULT_AGENT_PORT)))
        except ValueError as exc:
            raise ValueError("AI_AGENT_PORT must be an integer.") from exc
        return cls(host=os.getenv("AI_AGENT_HOST", DEFAULT_AGENT_HOST), port=port)


def build_agent_loop() -> AgentLoop:
    """Compose the production loop without starting a server or making API calls."""
    return AgentLoop(
        AnthropicStructuredLlm.from_env(),
        McpHttpGateway.from_env(),
    )


def create_app(agent: Any | None = None) -> FastAPI:
    """Create an injectable FastAPI app; one POST always reaches a terminal envelope."""
    service = agent if agent is not None else build_agent_loop()
    app = FastAPI(
        title="Crypto Lakehouse Agent",
        version="1.0.0",
        description=(
            "Local one-shot natural-language analytics over the governed "
            "Gold MCP layer."
        ),
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/questions", response_model=AgentResponse)
    async def answer_question(request: AgentRequest) -> AgentResponse:
        return await service.answer(request)

    return app


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    settings = AgentHttpSettings.from_env()
    parser = argparse.ArgumentParser(
        description="Run the local one-shot AI agent service."
    )
    parser.add_argument("--host", default=settings.host)
    parser.add_argument("--port", type=int, default=settings.port)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    settings = AgentHttpSettings(host=args.host, port=args.port)
    import uvicorn

    uvicorn.run(create_app(), host=settings.host, port=settings.port)
