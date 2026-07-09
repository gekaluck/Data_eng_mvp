# `mcp_server/` — placeholder

Transport-agnostic MCP server. Owns everything between "tool call arrives" and "governed
result returns": tool router (stdio + streamable-HTTP frontends), the guardrail layer (AST
validation, Gold allow-list, row/scan/budget caps, audit log), and the Trino / Iceberg /
dbt adapters. LLM-agnostic — it knows nothing about questions or answers.

No implementation yet. Contracts and guardrail spec: [`../../docs/new_ARCHITECTURE.md`](../../docs/new_ARCHITECTURE.md) §3–§4.
