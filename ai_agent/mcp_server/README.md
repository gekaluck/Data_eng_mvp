# `mcp_server/` — transport-agnostic core

Transport-agnostic MCP server. Owns everything between "tool call arrives" and "governed
result returns": tool router (stdio + streamable-HTTP frontends), the guardrail layer (AST
validation, Gold allow-list, row/scan/budget caps, audit log), and the Trino / Iceberg /
dbt adapters. LLM-agnostic — it knows nothing about questions or answers.

Implemented now:

- strict loading and normalization of `config/ai-agent/allowed-tables.json`
- one-statement Trino parsing with SQLGlot
- a root-`SELECT` whitelist, including an explicit `SELECT INTO` rejection
- CTE-aware extraction of fully qualified physical tables
- exact table allow-list enforcement and structured `PARSE_ERROR`, `NOT_READ_ONLY`, and
  `TABLE_NOT_ALLOWED` failures
- typed contracts and transport-neutral implementations for `list_tables`,
  `get_table_schema`, `get_table_snapshots`, `get_lineage`, and `get_model_docs`
- fail-closed dbt artifact loading, allow-list-filtered docs/lineage, and live Iceberg
  columns, file statistics, and snapshots through fixed-shape read-only queries
- live-schema/dbt-doc reconciliation that keeps Iceberg authoritative and emits explicit
  drift warnings instead of inventing columns or nullability

Not implemented yet: MCP transports, query/sample/explain execution, row and scan caps,
budget accounting, or audit logging. Contracts and the full guardrail spec remain in
[`../../docs/ai-agent-architecture.md`](../../docs/ai-agent-architecture.md) §3–§4.
