"""Tests for the loopback FastAPI agent frontend."""

import asyncio

from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from ai_agent.agent_service.app import AgentHttpSettings, create_app, parse_args
from ai_agent.agent_service.contracts import AgentResponse


class FakeAgent:
    def __init__(self):
        self.requests = []

    async def answer(self, request):
        self.requests.append(request)
        return AgentResponse(
            answer="BTC is available.",
            sql="SELECT 'BTC' AS symbol",
            confidence=("execution_succeeded",),
            profile=request.profile,
            request_id=request.request_id or "generated-id",
            model_id="claude-sonnet-5",
        )


async def request(app, method, path, **kwargs):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        return await client.request(method, path, **kwargs)


def test_health_and_question_endpoint_return_typed_contracts():
    agent = FakeAgent()
    app = create_app(agent)

    health, response = asyncio.run(_health_and_question(app))

    assert health.json() == {"status": "ok"}
    assert response.status_code == 200
    payload = response.json()
    assert payload["answer"] == "BTC is available."
    assert payload["refusal_reason"] is None
    assert payload["model_id"] == "claude-sonnet-5"
    assert agent.requests[0].question == "Which symbols are available?"


async def _health_and_question(app):
    health = await request(app, "GET", "/health")
    response = await request(
        app,
        "POST",
        "/v1/questions",
        json={
            "question": "Which symbols are available?",
            "profile": "fast",
            "request_id": "request-1",
        },
    )
    return health, response


def test_question_endpoint_rejects_invalid_profile_and_extra_fields():
    app = create_app(FakeAgent())

    invalid_profile, extra = asyncio.run(_invalid_requests(app))

    assert invalid_profile.status_code == 422
    assert extra.status_code == 422

    whitespace = asyncio.run(
        request(
            app,
            "POST",
            "/v1/questions",
            json={"question": "   ", "profile": "fast"},
        )
    )
    assert whitespace.status_code == 422


async def _invalid_requests(app):
    invalid_profile = await request(
        app,
        "POST",
        "/v1/questions",
        json={"question": "Question", "profile": "unbounded"},
    )
    extra = await request(
        app,
        "POST",
        "/v1/questions",
        json={"question": "Question", "unexpected": True},
    )
    return invalid_profile, extra


def test_http_settings_are_loopback_only(monkeypatch):
    assert AgentHttpSettings(host="localhost", port=8010).host == "localhost"

    for host in ("0.0.0.0", "example.com"):
        try:
            AgentHttpSettings(host=host)
        except ValueError as exc:
            assert "loopback" in str(exc)
        else:
            raise AssertionError(f"Expected {host} to be rejected")

    monkeypatch.setenv("AI_AGENT_HOST", "127.0.0.1")
    monkeypatch.setenv("AI_AGENT_PORT", "8123")
    assert AgentHttpSettings.from_env().port == 8123


def test_cli_overrides_are_revalidated(monkeypatch):
    monkeypatch.delenv("AI_AGENT_HOST", raising=False)
    monkeypatch.delenv("AI_AGENT_PORT", raising=False)

    args = parse_args(["--host", "localhost", "--port", "8123"])

    assert AgentHttpSettings(host=args.host, port=args.port).port == 8123


def test_terminal_envelope_rejects_blank_outcomes():
    for kwargs in ({"answer": "   "}, {"refusal_reason": "\t"}):
        try:
            AgentResponse(
                **kwargs,
                profile="fast",
                request_id="request-1",
                model_id="claude-sonnet-5",
            )
        except ValidationError:
            pass
        else:
            raise AssertionError("Expected blank terminal text to be rejected")
