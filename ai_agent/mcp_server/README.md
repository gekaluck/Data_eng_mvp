# `mcp_server/` — governed metadata MCP server

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
- one shared FastMCP tool registry exposed through stdio and stateless JSON streamable HTTP
- typed MCP output schemas, with successful results and the exact structured guardrail
  envelope available in both text and `structuredContent`
- read-only/idempotent tool annotations, bounded input schemas, and explicit
  `isError: true` tool failures that clients can inspect and recover from
- loopback-only HTTP binding with DNS-rebinding host/origin checks; stdio remains the
  default local-host transport
- scan-free `explain_query`: the existing AST/allow-list guardrail runs first, then Trino
  returns a bounded distributed plan or a typed semantic diagnostic without executing rows
- one bounded process-local request budget shared by `explain_query`, `sample_rows`, and
  `execute_query`: `fast` permits three Trino attempts, `thorough` permits ten,
  metadata/local denials are free, and the 1,024 least-recently-used IDs are retained
- capped `sample_rows`: accepts only an allow-listed table and `n <= 20`, constructs its own
  quoted statement, and gives callers no SQL/filter/order surface
- append-only JSONL sample auditing before return, with request metadata, verdict, timing,
  columns/row count, and failure code but no raw row values; audit failures fail closed
- governed `execute_query`: existing AST/allow-list checks, default 100 and hard 500-row
  bounds with exact truncation, a 15-second timeout, observed 100 MiB scan cancellation,
  query ID/work stats, and explicit data caveats
- append-only execution auditing with SQL, verdict, result shape, truncation, query ID,
  work stats, and failure code—but never returned business-row values

Not implemented yet: the owned natural-language agent loop, eval harness, or remote
authenticated HTTP exposure. Contracts and the full guardrail spec remain in
[`../../docs/ai-agent-architecture.md`](../../docs/ai-agent-architecture.md) §3–§4.
