"""Tests for pinned Claude structured-output configuration."""

import asyncio
import inspect
import json
from types import SimpleNamespace

import pytest

from ai_agent.agent_service.contracts import PlanDecision
from ai_agent.agent_service.provider import (
    AnthropicStructuredLlm,
    ProviderError,
)


class FakeMessages:
    def __init__(self, response=None, failure=None):
        self.response = response
        self.failure = failure
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        if self.failure:
            raise self.failure
        return self.response


def response(payload, *, stop_reason="end_turn"):
    return SimpleNamespace(
        stop_reason=stop_reason,
        content=(SimpleNamespace(type="text", text=json.dumps(payload)),),
    )


@pytest.mark.parametrize(
    ("profile", "model", "effort", "max_tokens"),
    [
        ("fast", "claude-sonnet-5", "low", 4096),
        ("thorough", "claude-opus-5", "high", 12000),
    ],
)
def test_provider_pins_models_effort_and_structured_output(
    profile, model, effort, max_tokens
):
    messages = FakeMessages(
        response(
            {
                "disposition": "answer",
                "reason": "supported",
                "tables": ["gold.crypto_dbt.daily_snapshot"],
                "sample_tables": [],
            }
        )
    )
    provider = AnthropicStructuredLlm(
        SimpleNamespace(messages=messages),
        fast_model="claude-sonnet-5",
        thorough_model="claude-opus-5",
    )

    decision = asyncio.run(
        provider.generate(
            profile=profile,
            stage="plan",
            system="system",
            prompt="prompt",
            output_type=PlanDecision,
        )
    )

    assert decision.disposition == "answer"
    call = messages.calls[0]
    assert call["model"] == model
    assert call["max_tokens"] == max_tokens
    assert call["thinking"] == {"type": "adaptive"}
    assert call["output_config"]["effort"] == effort
    assert call["output_config"]["format"]["type"] == "json_schema"
    assert "temperature" not in call


@pytest.mark.parametrize("stop_reason", ["refusal", "max_tokens"])
def test_provider_fails_closed_on_non_schema_terminal_reasons(stop_reason):
    messages = FakeMessages(response({}, stop_reason=stop_reason))
    provider = AnthropicStructuredLlm(
        SimpleNamespace(messages=messages),
        fast_model="fast-model",
        thorough_model="thorough-model",
    )

    with pytest.raises(ProviderError, match=stop_reason):
        asyncio.run(
            provider.generate(
                profile="fast",
                stage="plan",
                system="system",
                prompt="prompt",
                output_type=PlanDecision,
            )
        )


def test_provider_wraps_api_and_validation_failures():
    failed = AnthropicStructuredLlm(
        SimpleNamespace(messages=FakeMessages(failure=OSError("offline"))),
        fast_model="fast-model",
        thorough_model="thorough-model",
    )
    with pytest.raises(ProviderError, match="offline"):
        asyncio.run(
            failed.generate(
                profile="fast",
                stage="plan",
                system="system",
                prompt="prompt",
                output_type=PlanDecision,
            )
        )

    invalid = AnthropicStructuredLlm(
        SimpleNamespace(messages=FakeMessages(response({"bad": "shape"}))),
        fast_model="fast-model",
        thorough_model="thorough-model",
    )
    with pytest.raises(ProviderError, match="invalid structured"):
        asyncio.run(
            invalid.generate(
                profile="fast",
                stage="plan",
                system="system",
                prompt="prompt",
                output_type=PlanDecision,
            )
        )


def test_provider_requires_a_real_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
        AnthropicStructuredLlm.from_env()

    monkeypatch.setenv("ANTHROPIC_API_KEY", "REPLACE_WITH_ANTHROPIC_API_KEY")
    with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
        AnthropicStructuredLlm.from_env()


def test_installed_sdk_accepts_adaptive_thinking_and_output_config():
    from anthropic import AsyncAnthropic

    parameters = inspect.signature(
        AsyncAnthropic(api_key="not-a-real-key").messages.create
    ).parameters

    assert "thinking" in parameters
    assert "output_config" in parameters
