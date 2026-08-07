"""Tests for active Trino timeout/scan monitoring and cancellation."""

import threading

import pytest

from ai_agent.mcp_server.query_executor import RunnerFailure, TrinoCappedRunner


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


class SuccessfulCursor:
    query_id = "query-1"
    stats = {"processedRows": 25, "processedBytes": 2048}
    description = (("symbol",),)

    def __init__(self):
        self.sql = None
        self.fetch_size = None
        self.cancelled = False

    def execute(self, sql):
        self.sql = sql

    def fetchmany(self, size):
        self.fetch_size = size
        return [["BTC"], ["ETH"]]

    def cancel(self):
        self.cancelled = True


class BlockingCursor(SuccessfulCursor):
    def __init__(self, *, bytes_read=0):
        super().__init__()
        self.stats = {"processedRows": 10, "processedBytes": bytes_read}
        self._released = threading.Event()

    def execute(self, sql):
        self.sql = sql
        self._released.wait(timeout=1)

    def cancel(self):
        self.cancelled = True
        self._released.set()


class FailingCursor(SuccessfulCursor):
    def execute(self, sql):
        self.sql = sql
        raise OSError("connection reset")


def install_connection(monkeypatch, cursor):
    connection = FakeConnection(cursor)
    monkeypatch.setattr("trino.dbapi.connect", lambda **kwargs: connection)
    return connection


def runner():
    return TrinoCappedRunner(
        host="trino",
        port=8080,
        poll_interval_seconds=0.001,
    )


def test_returns_bounded_rows_and_final_stats(monkeypatch):
    cursor = SuccessfulCursor()
    connection = install_connection(monkeypatch, cursor)

    result = runner().execute(
        "SELECT 1",
        fetch_rows=3,
        timeout_seconds=1,
        max_scan_bytes=10_000,
    )

    assert result.columns == ("symbol",)
    assert result.rows == (("BTC",), ("ETH",))
    assert result.query_id == "query-1"
    assert result.rows_read == 25
    assert result.bytes_read == 2048
    assert cursor.fetch_size == 3
    assert connection.closed is True


def test_wall_clock_limit_actively_cancels_the_cursor(monkeypatch):
    cursor = BlockingCursor()
    connection = install_connection(monkeypatch, cursor)

    with pytest.raises(RunnerFailure) as raised:
        runner().execute(
            "SELECT 1",
            fetch_rows=2,
            timeout_seconds=0.005,
            max_scan_bytes=10_000,
        )

    assert raised.value.kind == "timeout"
    assert raised.value.query_id == "query-1"
    assert cursor.cancelled is True
    assert connection.closed is True


def test_scan_limit_actively_cancels_the_cursor(monkeypatch):
    cursor = BlockingCursor(bytes_read=101)
    connection = install_connection(monkeypatch, cursor)

    with pytest.raises(RunnerFailure) as raised:
        runner().execute(
            "SELECT 1",
            fetch_rows=2,
            timeout_seconds=1,
            max_scan_bytes=100,
        )

    assert raised.value.kind == "scan_limit"
    assert raised.value.bytes_read == 101
    assert cursor.cancelled is True
    assert connection.closed is True


def test_engine_failure_keeps_query_stats_and_is_retryable(monkeypatch):
    cursor = FailingCursor()
    install_connection(monkeypatch, cursor)

    with pytest.raises(RunnerFailure) as raised:
        runner().execute(
            "SELECT 1",
            fetch_rows=2,
            timeout_seconds=1,
            max_scan_bytes=10_000,
        )

    assert raised.value.kind == "engine"
    assert raised.value.retryable is True
    assert raised.value.query_id == "query-1"
    assert raised.value.bytes_read == 2048
