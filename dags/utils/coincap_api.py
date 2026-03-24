"""Helpers for turning CoinCap request failures into actionable errors."""

from __future__ import annotations

import requests


def _response_detail(response: requests.Response) -> str:
    """Extract a compact error detail from an HTTP response body."""
    detail = ""

    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        for key in ("error", "message", "detail"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                detail = value.strip()
                break

        if not detail:
            detail = str(payload)
    elif response.text:
        detail = response.text.strip()

    detail = " ".join(detail.split())
    if len(detail) > 200:
        return f"{detail[:197]}..."
    return detail


def format_coincap_request_error(exc: requests.exceptions.RequestException, url: str) -> str:
    """Return a clearer, failure-specific CoinCap error message."""
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        status_code = exc.response.status_code
        detail = _response_detail(exc.response)
        detail_suffix = f" Response: {detail}." if detail else ""

        if status_code in (401, 403):
            return (
                f"CoinCap request was rejected with HTTP {status_code} (URL: {url}). "
                f"Check COINCAP_API_KEY and CoinCap account limits.{detail_suffix}"
            )

        if status_code == 429:
            return (
                f"CoinCap rate limit exceeded with HTTP 429 (URL: {url}). "
                f"Retry later or reduce request volume.{detail_suffix}"
            )

        if 500 <= status_code < 600:
            return (
                f"CoinCap upstream returned HTTP {status_code} (URL: {url}). "
                f"This is likely a provider-side issue.{detail_suffix}"
            )

        return f"CoinCap request failed with HTTP {status_code} (URL: {url}).{detail_suffix}"

    if isinstance(exc, requests.exceptions.Timeout):
        return (
            f"CoinCap request timed out (URL: {url}). "
            "Check outbound network access or retry later."
        )

    if isinstance(exc, requests.exceptions.ConnectionError):
        return (
            f"CoinCap request could not reach the upstream host (URL: {url}). "
            "Check host/container DNS and outbound network access."
        )

    return (
        f"CoinCap request failed before a valid response was received (URL: {url}). "
        "Check request configuration and outbound network access."
    )
