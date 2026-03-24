"""Unit tests for CoinCap request error formatting."""

import requests

from utils.coincap_api import format_coincap_request_error


def _http_error(status_code: int, body: bytes, content_type: str = "application/json"):
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://rest.coincap.io/v3/assets"
    response.headers["Content-Type"] = content_type
    response._content = body
    return requests.exceptions.HTTPError(
        f"{status_code} error",
        response=response,
    )


def test_403_mentions_api_key_and_account_limits():
    exc = _http_error(403, b'{"error":"Forbidden: Monthly usage limit exceeded"}')

    message = format_coincap_request_error(exc, "https://rest.coincap.io/v3/assets")

    assert "HTTP 403" in message
    assert "COINCAP_API_KEY" in message
    assert "account limits" in message
    assert "Monthly usage limit exceeded" in message


def test_500_is_classified_as_provider_side_issue():
    exc = _http_error(500, b'{"error":"Internal Server Error"}')

    message = format_coincap_request_error(exc, "https://rest.coincap.io/v3/assets")

    assert "HTTP 500" in message
    assert "provider-side issue" in message


def test_timeout_is_classified_as_connectivity_retry_issue():
    exc = requests.exceptions.Timeout("timed out")

    message = format_coincap_request_error(exc, "https://rest.coincap.io/v3/assets")

    assert "timed out" in message
    assert "retry later" in message


def test_connection_error_mentions_dns_and_outbound_network():
    exc = requests.exceptions.ConnectionError("dns failure")

    message = format_coincap_request_error(exc, "https://rest.coincap.io/v3/assets")

    assert "could not reach the upstream host" in message
    assert "DNS" in message
    assert "outbound network access" in message
