# `agent_service/` — owned natural-language agent

The implemented agent loop is a bounded state machine (PLAN → EXPLORE → DRAFT → VALIDATE →
EXECUTE → CHECK → CRITIC → ANSWER/REFUSE) with fixed retry, sampling, and wall-clock limits.
It holds no enforcement power: every schema read, sample, plan, and analytical query goes
through the loopback MCP HTTP service.

`fast` uses pinned `claude-sonnet-5` with low effort, at most two drafts, one sampled table,
and no critic. `thorough` uses pinned `claude-opus-5` with high effort, at most four drafts,
two sampled tables, and a semantic critic. Structured model outputs are validated with
Pydantic; deterministic checks then require the exact declared result shape, only planned
tables, a non-empty result when promised, and explicit truncation disclosure.

The loop is served locally by FastAPI at `POST /v1/questions`. Every call ends in the same
typed envelope with exactly one of `answer` or `refusal_reason`, plus visible SQL, tables,
Trino statistics, caveats, passed checks, profile, request ID, model ID, and prompt version.
See [`../../docs/runbook.md`](../../docs/runbook.md) for startup and manual checks, and
[`../../docs/ai-agent-architecture.md`](../../docs/ai-agent-architecture.md) §5 for the
authoritative design.

Production requests also emit ordered JSONL events to
`ai_agent/runtime/agent-trace.jsonl`. The default `full` level captures complete local
debug/eval evidence, including prompts, structured decisions, token usage, MCP inputs and
outputs, SQL, returned rows, checks, retries, query IDs, and the terminal envelope. Use
`AI_TRACE_LEVEL=basic` to omit event content or `off` to disable tracing. This file is
gitignored but can contain questions and Gold values; it is not a credential or header log
and should remain local. Trace write failures are logged and fail open, while the MCP
query audit remains a separate fail-closed enforcement record.
