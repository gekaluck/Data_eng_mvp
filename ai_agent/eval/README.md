# `eval/` — placeholder

Golden-set eval harness. Drives the agent service API directly (not through an MCP host),
scores on **execution accuracy** over a hand-written NL→SQL→result golden set, and records
the five pins on every report: model ID, prompt version, allow-list hash, golden-set
version, profile. Includes unanswerable cases to test the refusal gate.

No implementation yet. Eval design: [`../../docs/new_ARCHITECTURE.md`](../../docs/new_ARCHITECTURE.md) §6.
