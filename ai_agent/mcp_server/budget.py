"""Process-local query budgets scoped to one natural-language request."""

import re
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import Literal

from ai_agent.mcp_server.errors import ErrorCode, GuardrailError

BudgetProfile = Literal["fast", "thorough"]
MAX_REQUEST_ID_CHARS = 128
REQUEST_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
_REQUEST_ID = re.compile(REQUEST_ID_PATTERN)
_PROFILE_LIMITS: dict[BudgetProfile, int] = {"fast": 3, "thorough": 10}
DEFAULT_MAX_TRACKED_REQUESTS = 1_024


@dataclass(frozen=True, slots=True)
class BudgetStatus:
    """Budget state after one engine-call token has been charged."""

    request_id: str
    profile: BudgetProfile
    limit: int
    used: int
    remaining: int


class RequestBudgetManager:
    """Thread-safe in-memory LRU counter; one server process owns one budget map."""

    def __init__(
        self, *, max_tracked_requests: int = DEFAULT_MAX_TRACKED_REQUESTS
    ) -> None:
        if (
            not isinstance(max_tracked_requests, int)
            or isinstance(max_tracked_requests, bool)
            or max_tracked_requests < 1
        ):
            raise ValueError("max_tracked_requests must be a positive integer.")
        self._max_tracked_requests = max_tracked_requests
        self._requests: OrderedDict[str, tuple[BudgetProfile, int]] = OrderedDict()
        self._lock = Lock()

    @staticmethod
    def validate_request(
        request_id: str,
        profile: BudgetProfile,
    ) -> tuple[str, BudgetProfile]:
        """Validate identifiers without consuming a token."""
        if (
            not isinstance(request_id, str)
            or len(request_id) > MAX_REQUEST_ID_CHARS
            or not _REQUEST_ID.fullmatch(request_id)
        ):
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                "request_id must be 1-128 characters using letters, digits, '.', "
                "'_', ':' or '-'.",
                hint=(
                    "Use one stable request_id for the whole natural-language question."
                ),
            )
        if not isinstance(profile, str) or profile not in _PROFILE_LIMITS:
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                "Budget profile must be 'fast' or 'thorough'.",
                hint="Use the same profile for every engine call in one request.",
            )
        return request_id, profile

    def charge(
        self,
        request_id: str,
        profile: BudgetProfile,
    ) -> BudgetStatus:
        """Consume one engine-call token or fail before the engine is contacted."""
        request_id, profile = self.validate_request(request_id, profile)
        limit = _PROFILE_LIMITS[profile]
        with self._lock:
            existing = self._requests.get(request_id)
            if existing is not None and existing[0] != profile:
                raise GuardrailError(
                    ErrorCode.PARSE_ERROR,
                    f"Request '{request_id}' already uses the '{existing[0]}' profile.",
                    hint="A request cannot change budget profiles midway.",
                )
            used = existing[1] if existing else 0
            if used >= limit:
                raise GuardrailError(
                    ErrorCode.BUDGET_EXCEEDED,
                    f"Request '{request_id}' exhausted its {profile} query budget.",
                    hint=(
                        "Stop this request and return the partial work or a refusal; "
                        "do not create a new request_id to evade the limit."
                    ),
                )
            used += 1
            self._requests[request_id] = (profile, used)
            self._requests.move_to_end(request_id)
            while len(self._requests) > self._max_tracked_requests:
                self._requests.popitem(last=False)
            return BudgetStatus(
                request_id=request_id,
                profile=profile,
                limit=limit,
                used=used,
                remaining=limit - used,
            )

    def status(
        self,
        request_id: str,
        profile: BudgetProfile,
    ) -> BudgetStatus:
        """Inspect a budget without consuming a token.

        This is primarily for tests and diagnostics.
        """
        request_id, profile = self.validate_request(request_id, profile)
        with self._lock:
            existing = self._requests.get(request_id)
            if existing is not None and existing[0] != profile:
                raise GuardrailError(
                    ErrorCode.PARSE_ERROR,
                    f"Request '{request_id}' already uses the '{existing[0]}' profile.",
                    hint="A request cannot change budget profiles midway.",
                )
            used = existing[1] if existing else 0
            if existing is not None:
                self._requests.move_to_end(request_id)
        limit = _PROFILE_LIMITS[profile]
        return BudgetStatus(
            request_id=request_id,
            profile=profile,
            limit=limit,
            used=used,
            remaining=limit - used,
        )
