# `agent_service/` — placeholder

The owned agent loop: a bounded state machine (PLAN → EXPLORE → DRAFT → VALIDATE →
EXECUTE → CHECK → CRITIC → ANSWER/REFUSE) with a budget manager that owns termination, an
optional critic pass, and a pinned LLM provider client. Holds no enforcement power — it can
only ask the MCP server.

No implementation yet. Loop design and budget profiles: [`../../docs/new_ARCHITECTURE.md`](../../docs/new_ARCHITECTURE.md) §5.
