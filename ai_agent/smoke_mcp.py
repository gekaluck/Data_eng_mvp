"""End-to-end read-only smoke check for stdio and Streamable HTTP MCP."""

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, Literal

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client


EXPECTED_TOOLS = {
    "list_tables",
    "get_table_schema",
    "get_table_snapshots",
    "get_lineage",
    "get_model_docs",
}
Transport = Literal["stdio", "streamable-http"]


async def exercise_session(session: ClientSession) -> dict[str, Any]:
    """Call every metadata tool through a real MCP ClientSession."""
    await session.initialize()
    listed = await session.list_tools()
    names = {tool.name for tool in listed.tools}
    if names != EXPECTED_TOOLS:
        raise RuntimeError(f"Unexpected MCP tools: {sorted(names)}")

    tables_result = await session.call_tool("list_tables", arguments={})
    _require_success("list_tables", tables_result)
    tables = tables_result.structuredContent["tables"]
    if len(tables) != 5:
        raise RuntimeError(f"Expected five allow-listed tables, got {len(tables)}.")
    table = tables[0]["table"]

    calls = {
        "get_table_schema": {"table": table},
        "get_table_snapshots": {"table": table, "limit": 2},
        "get_lineage": {"model": table, "direction": "upstream", "depth": 2},
        "get_model_docs": {"model": table},
    }
    outputs: dict[str, dict[str, Any]] = {}
    for name, arguments in calls.items():
        result = await session.call_tool(name, arguments=arguments)
        _require_success(name, result)
        outputs[name] = result.structuredContent

    denied = await session.call_tool(
        "get_model_docs",
        arguments={"model": "not_allow_listed"},
    )
    if (
        denied.isError is not True
        or not isinstance(denied.structuredContent, dict)
        or denied.structuredContent.get("code") != "TABLE_NOT_ALLOWED"
    ):
        raise RuntimeError(f"Structured MCP denial contract failed: {denied}")

    return {
        "tools": sorted(names),
        "table": table,
        "columns": len(outputs["get_table_schema"]["columns"]),
        "schema_warnings": outputs["get_table_schema"]["warnings"],
        "recent_snapshots": len(outputs["get_table_snapshots"]["snapshots"]),
        "lineage_nodes": len(outputs["get_lineage"]["nodes"]),
        "documented_columns": len(outputs["get_model_docs"]["columns"]),
        "denial_code": denied.structuredContent["code"],
    }


def _require_success(name: str, result: Any) -> None:
    if result.isError or not isinstance(result.structuredContent, dict):
        raise RuntimeError(f"MCP tool {name} failed: {result}")


@asynccontextmanager
async def stdio_session() -> AsyncIterator[ClientSession]:
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "ai_agent.mcp_server", "--transport", "stdio"],
        env=os.environ.copy(),
    )
    async with stdio_client(params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            yield session


@asynccontextmanager
async def http_session() -> AsyncIterator[ClientSession]:
    port = _free_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ai_agent.mcp_server",
            "--transport",
            "streamable-http",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        env=os.environ.copy(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(process, port)
        async with streamable_http_client(f"http://127.0.0.1:{port}/mcp") as streams:
            read_stream, write_stream, _ = streams
            async with ClientSession(read_stream, write_stream) as session:
                yield session
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _wait_for_port(process: subprocess.Popen[bytes], port: int) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"Streamable HTTP server exited with code {process.returncode}."
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("Streamable HTTP server did not start within 15 seconds.")


async def smoke(transports: Sequence[Transport]) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for transport in transports:
        session_factory = stdio_session if transport == "stdio" else http_session
        async with session_factory() as session:
            report[transport] = await exercise_session(session)
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke-test live MCP metadata transports.")
    parser.add_argument(
        "--transport",
        choices=("stdio", "streamable-http", "both"),
        default="both",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    transports: tuple[Transport, ...] = (
        ("stdio", "streamable-http")
        if args.transport == "both"
        else (args.transport,)
    )
    print(json.dumps(asyncio.run(smoke(transports)), indent=2))


if __name__ == "__main__":
    main()
