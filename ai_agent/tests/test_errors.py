"""Tests for the stable structured-error envelope."""

from ai_agent.mcp_server.errors import ErrorCode, GuardrailError


def test_guardrail_error_serializes_the_tool_contract():
    error = GuardrailError(
        ErrorCode.TABLE_NOT_ALLOWED,
        "Blocked.",
        hint="Use Gold.",
    )

    assert str(error) == "Blocked."
    assert error.as_dict() == {
        "code": "TABLE_NOT_ALLOWED",
        "message": "Blocked.",
        "retryable": False,
        "hint": "Use Gold.",
    }
