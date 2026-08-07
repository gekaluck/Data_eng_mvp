"""Structured errors shared by MCP guardrails and future tool adapters."""

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    """Stable error codes from the Phase A tool contract."""

    PARSE_ERROR = "PARSE_ERROR"
    NOT_READ_ONLY = "NOT_READ_ONLY"
    TABLE_NOT_ALLOWED = "TABLE_NOT_ALLOWED"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    TIMEOUT = "TIMEOUT"
    SCAN_LIMIT_EXCEEDED = "SCAN_LIMIT_EXCEEDED"
    ENGINE_ERROR = "ENGINE_ERROR"


class GuardrailError(Exception):
    """A fail-closed validation error that can cross the MCP tool boundary."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        retryable: bool = False,
        hint: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.hint = hint

    def as_dict(self) -> dict[str, Any]:
        """Return the exact structured-error envelope promised by the design."""
        return {
            "code": self.code.value,
            "message": self.message,
            "retryable": self.retryable,
            "hint": self.hint,
        }
