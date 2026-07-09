# `ai_agent/` — AI-Agent Layer (Phase A skeleton)

> **Status: skeleton only.** This module is intentionally empty of implementation.
> It marks where the AI-agent layer will be built and reserves the structure agreed
> in the design doc.

## Purpose

Add a **text-to-analytics** capability on top of the existing lakehouse: ask a
natural-language question, get a validated SQL answer over the **Gold layer** — with a
confidence gate that refuses rather than guessing when checks fail.

This module **extends, never modifies** the running Bronze/Silver/Gold platform. No DAG,
transform, or dbt model changes are required to build it; the only platform-side additions
are configuration (a read-only Trino user + resource group, and an Airflow step that
publishes fresh dbt artifacts).

## Source of truth

The full design — requirements, tool contracts, guardrail spec, agent loop, eval harness,
failure modes, and decision log — lives in
[`../docs/new_ARCHITECTURE.md`](../docs/new_ARCHITECTURE.md). **That document is
authoritative.** This README is a signpost; do not paraphrase the design here (it drifts).

## Planned structure (Phase A)

Mirrors the component boundaries in `new_ARCHITECTURE.md §2.2`:

| Subdir            | Responsibility                                                                 |
|-------------------|--------------------------------------------------------------------------------|
| `mcp_server/`     | Transport-agnostic MCP server: tool router (stdio + HTTP), guardrail layer (AST validation, allow-list, caps, budget, audit), and Trino / Iceberg / dbt adapters. LLM-agnostic. |
| `agent_service/`  | The owned agent loop: bounded state machine, budget manager, critic pass, and the pinned LLM provider client. Holds no enforcement power. |
| `eval/`           | Golden-set eval harness driving the agent service API; execution-accuracy scoring, pinned model IDs, versioned reports. |

Phases B (RAG over catalog docs) and C (Feast feature-store bridge) are sketches in the
design doc and are **out of scope** for this skeleton.

## Building here

See [`../CONTRIBUTING.md`](../CONTRIBUTING.md) for repo conventions (branching, commits,
and the rule that the existing pipeline stays untouched).
