"""Tests for the typed MCP client used by the owned loop."""

import asyncio
from types import SimpleNamespace

import pytest

from ai_agent.agent_service.mcp_tools import (
    McpHttpGateway,
    McpToolSession,
    ToolCallError,
)


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def call_tool(self, name, *, arguments):
        self.calls.append((name, arguments))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def result(payload, *, is_error=False):
    return SimpleNamespace(isError=is_error, structuredContent=payload)


def test_list_tables_parses_the_structured_success_payload():
    session = FakeSession(
        [
            result(
                {
                    "tables": [
                        {
                            "table": "gold.crypto_dbt.daily_snapshot",
                            "description": "Daily snapshot",
                            "tags": ["gold"],
                            "approx_rows": None,
                        }
                    ]
                }
            )
        ]
    )

    tables = asyncio.run(McpToolSession(session).list_tables())

    assert tables[0].table == "gold.crypto_dbt.daily_snapshot"
    assert session.calls == [("list_tables", {})]


def test_structured_tool_error_is_preserved_for_loop_policy():
    session = FakeSession(
        [
            result(
                {
                    "code": "BUDGET_EXCEEDED",
                    "message": "No tokens remain.",
                    "retryable": False,
                    "hint": "Stop.",
                },
                is_error=True,
            )
        ]
    )

    with pytest.raises(ToolCallError) as raised:
        asyncio.run(
            McpToolSession(session).explain_query(
                "SELECT 1",
                request_id="request-1",
                profile="fast",
            )
        )

    assert raised.value.code == "BUDGET_EXCEEDED"
    assert raised.value.retryable is False
    assert raised.value.hint == "Stop."


def test_network_and_payload_shape_failures_become_engine_errors():
    failed = McpToolSession(FakeSession([OSError("offline")]))
    with pytest.raises(ToolCallError, match="could not be reached") as network:
        asyncio.run(failed.list_tables())
    assert network.value.code == "ENGINE_ERROR"
    assert network.value.retryable is True

    malformed = McpToolSession(FakeSession([result({"tables": "wrong"})]))
    with pytest.raises(ToolCallError, match="incompatible") as shape:
        asyncio.run(malformed.list_tables())
    assert shape.value.retryable is False


@pytest.mark.parametrize(
    "url",
    [
        "http://0.0.0.0:8000/mcp",
        "http://example.com/mcp",
        "ftp://127.0.0.1/mcp",
        "http://127.0.0.1",
    ],
)
def test_gateway_rejects_non_loopback_or_pathless_urls(url):
    with pytest.raises(ValueError):
        McpHttpGateway(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8000/mcp",
        "http://localhost:8000/mcp",
        "https://[::1]:8000/mcp",
    ],
)
def test_gateway_accepts_explicit_loopback_urls(url):
    assert McpHttpGateway(url)._url == url
