"""Tests for request-scoped engine-call budgets."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from ai_agent.mcp_server.budget import RequestBudgetManager
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError


@pytest.mark.parametrize(("profile", "limit"), [("fast", 3), ("thorough", 10)])
def test_profile_limit_is_enforced_before_an_extra_engine_call(profile, limit):
    budget = RequestBudgetManager()

    statuses = [budget.charge("request-1", profile) for _ in range(limit)]

    assert statuses[-1].used == limit
    assert statuses[-1].remaining == 0
    with pytest.raises(GuardrailError) as raised:
        budget.charge("request-1", profile)
    assert raised.value.code == ErrorCode.BUDGET_EXCEEDED
    assert raised.value.retryable is False


def test_request_cannot_change_profiles_midway():
    budget = RequestBudgetManager()
    budget.charge("request-1", "fast")

    with pytest.raises(GuardrailError) as raised:
        budget.charge("request-1", "thorough")

    assert raised.value.code == ErrorCode.PARSE_ERROR
    assert "already uses" in raised.value.message


@pytest.mark.parametrize(
    "request_id",
    ["", " with-space", "slash/value", "x" * 129, 123, None],
)
def test_request_id_is_bounded_and_log_safe(request_id):
    with pytest.raises(GuardrailError) as raised:
        RequestBudgetManager.validate_request(request_id, "fast")

    assert raised.value.code == ErrorCode.PARSE_ERROR


@pytest.mark.parametrize("profile", ["slow", "FAST", 1, None])
def test_profile_is_explicit(profile):
    with pytest.raises(GuardrailError) as raised:
        RequestBudgetManager.validate_request("request-1", profile)

    assert raised.value.code == ErrorCode.PARSE_ERROR


def test_status_does_not_consume_a_token():
    budget = RequestBudgetManager()

    status = budget.status("request-1", "fast")

    assert status.used == 0
    assert status.remaining == 3


def test_concurrent_charges_cannot_exceed_fast_limit():
    budget = RequestBudgetManager()

    def charge_once():
        try:
            budget.charge("request-1", "fast")
            return "charged"
        except GuardrailError as exc:
            return exc.code.value

    with ThreadPoolExecutor(max_workers=12) as pool:
        outcomes = list(pool.map(lambda _: charge_once(), range(12)))

    assert outcomes.count("charged") == 3
    assert outcomes.count("BUDGET_EXCEEDED") == 9
