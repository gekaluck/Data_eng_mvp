"""Pinned structured-output LLM provider used by the agent state machine."""

import json
import os
from typing import Protocol, TypeVar

from pydantic import BaseModel

from ai_agent.agent_service.contracts import (
    FAST_MODEL_ID,
    THOROUGH_MODEL_ID,
)
from ai_agent.mcp_server.budget import BudgetProfile

OutputT = TypeVar("OutputT", bound=BaseModel)


class ProviderError(Exception):
    """The hosted model could not return a usable structured decision."""


class StructuredLlm(Protocol):
    """Small provider seam used by production Claude and deterministic test fakes."""

    def model_id(self, profile: BudgetProfile) -> str: ...

    async def generate(
        self,
        *,
        profile: BudgetProfile,
        stage: str,
        system: str,
        prompt: str,
        output_type: type[OutputT],
    ) -> OutputT: ...


class AnthropicStructuredLlm:
    """Claude Messages API adapter with pinned models and constrained JSON output."""

    def __init__(self, client: object, *, fast_model: str, thorough_model: str) -> None:
        self._client = client
        self._models = {"fast": fast_model, "thorough": thorough_model}

    @classmethod
    def from_env(cls) -> "AnthropicStructuredLlm":
        """Build the provider only when an API key is explicitly configured."""
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key or api_key.startswith("REPLACE_WITH_"):
            raise ValueError(
                "ANTHROPIC_API_KEY is required to start the agent service. "
                "Keep the real key only in the gitignored .env file."
            )
        from anthropic import AsyncAnthropic

        return cls(
            AsyncAnthropic(api_key=api_key),
            fast_model=FAST_MODEL_ID,
            thorough_model=THOROUGH_MODEL_ID,
        )

    def model_id(self, profile: BudgetProfile) -> str:
        return self._models[profile]

    async def generate(
        self,
        *,
        profile: BudgetProfile,
        stage: str,
        system: str,
        prompt: str,
        output_type: type[OutputT],
    ) -> OutputT:
        """Return one validated decision; Claude refusals and truncation fail closed."""
        effort = "low" if profile == "fast" else "high"
        max_tokens = 4_096 if profile == "fast" else 12_000
        try:
            response = await self._client.messages.create(
                model=self.model_id(profile),
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": prompt}],
                thinking={"type": "adaptive"},
                output_config={
                    "effort": effort,
                    "format": {
                        "type": "json_schema",
                        "schema": output_type.model_json_schema(),
                    },
                },
            )
        except Exception as exc:
            raise ProviderError(f"Claude {stage} request failed: {exc}") from exc

        stop_reason = getattr(response, "stop_reason", None)
        if stop_reason in {"refusal", "max_tokens"}:
            raise ProviderError(f"Claude {stage} stopped with reason '{stop_reason}'.")
        text_blocks = [
            block.text
            for block in getattr(response, "content", ())
            if getattr(block, "type", None) == "text"
        ]
        if len(text_blocks) != 1:
            raise ProviderError(
                f"Claude {stage} returned {len(text_blocks)} text blocks; expected one."
            )
        try:
            return output_type.model_validate(json.loads(text_blocks[0]))
        except Exception as exc:
            raise ProviderError(
                f"Claude {stage} returned an invalid structured response."
            ) from exc
