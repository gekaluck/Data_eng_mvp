# `ai_agent/` — AI-Agent Layer (Phase A)

> **Status: runnable metadata MCP layer.** The same five governed catalog tools are exposed
> over MCP stdio and loopback streamable HTTP, backed by published dbt artifacts and live
> Trino/Iceberg metadata. Query execution caps, budgets/audit, and the agent loop remain
> next.

## Purpose

Add a **text-to-analytics** capability on top of the existing lakehouse: ask a
natural-language question, get a validated SQL answer over the **Gold layer** — with a
confidence gate that refuses rather than guessing when checks fail.

This module **extends, never modifies** the running Bronze/Silver/Gold platform. Its Python
dependencies live in `ai_agent/requirements.txt`, outside the Airflow image. The required
platform configuration is already present (D035).

## Source of truth

The full design — requirements, tool contracts, guardrail spec, agent loop, eval harness,
failure modes, and decision log — lives in
[`../docs/ai-agent-architecture.md`](../docs/ai-agent-architecture.md). **That document is
authoritative.** This README is a signpost; do not paraphrase the design here (it drifts).

## Structure (Phase A)

Mirrors the component boundaries in `ai-agent-architecture.md §2.2`:

| Subdir            | Responsibility                                                                 |
|-------------------|--------------------------------------------------------------------------------|
| `mcp_server/`     | Transport-agnostic MCP server: tool router (stdio + HTTP), guardrail layer (AST validation, allow-list, caps, budget, audit), and Trino / Iceberg / dbt adapters. LLM-agnostic. |
| `agent_service/`  | The owned agent loop: bounded state machine, budget manager, critic pass, and the pinned LLM provider client. Holds no enforcement power. |
| `eval/`           | Golden-set eval harness driving the agent service API; execution-accuracy scoring, pinned model IDs, versioned reports. |

Phases B (RAG over catalog docs) and C (Feast feature-store bridge) are sketches in the
design doc and remain **out of scope**.

## Test the current core

Use an isolated Python 3.12 environment rather than the Airflow image:

```bash
python -m pip install -r ai_agent/requirements-dev.txt
python -m pytest ai_agent/tests -v
ruff check --select E9,F63,F7,F82 ai_agent
```

The `AI guardrail tests` CI job runs the same dependency install, lint, compile, and pytest
checks with fixture-backed metadata adapters, without starting Docker services or contacting
Trino or an LLM provider. A separate opt-in, read-only live smoke command is documented in
the runbook.

## Run the metadata server

The default is MCP stdio, suitable for a local MCP host:

```bash
python -m ai_agent.mcp_server
```

For a local HTTP client, start the same tool registry over streamable HTTP:

```bash
python -m ai_agent.mcp_server --transport streamable-http
```

The endpoint is `http://127.0.0.1:8000/mcp`. The HTTP frontend deliberately rejects
non-loopback binds; remote or multi-user exposure requires a separate authentication and
threat-model decision. See the runbook for the official-client parity smoke check.

## Building here

See [`../CONTRIBUTING.md`](../CONTRIBUTING.md) for repo conventions (branching, commits,
and the rule that the existing pipeline stays untouched).
