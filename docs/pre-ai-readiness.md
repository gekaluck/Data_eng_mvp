# Pre-AI-Layer Readiness

What the platform still needs before Phase A of the AI-agent layer
([`ai-agent-architecture.md`](ai-agent-architecture.md)) starts. That document is the design
authority; this one is a punch list against the *running system*, checked on 2026-08-06.

The design's own boundary (§2.2) is that the agent layer **extends, never modifies** the
pipeline, and that the only platform-side additions are configuration. Everything in §B
below is one of those additions — none of it exists yet.

---

## A. Settled — no action needed

| Item | Evidence |
|------|----------|
| Hardening complete | H1–H6 all landed; the open-items table in [`incidents.md`](incidents.md) is empty |
| Test suite green | 128 tests pass in Docker (`make test`), including the AI platform configuration checks |
| Gold semantic layer exists | All five dbt Gold models and their columns carry descriptions in `dbt/models/gold/schema.yml` — the source behind `list_tables` / `get_model_docs` |
| Read-only Trino access has a precedent | `config/trino/access-rules.json` already restricts the `superset` user to `SELECT` on `gold.crypto_dbt` (D030). The agent user is a copy of that block |
| Bronze provenance | Snapshots record when they were fetched (D033), so the I10/I17 class of defect is detectable rather than inferred |
| Data caveats are written down | Sparse coverage, by-design nulls, and lower-fidelity repaired days are documented in `schema.yml`, [`README.md`](README.md) and D032 |

**Fixed while checking**: the Trino container was reporting `unhealthy` for days while serving
queries normally. It predated the healthcheck in `docker-compose.yml` and was still running
the image's own (broken) `health-check` script — incident I8's stale-container trap.
`docker compose up -d --force-recreate trino` fixed it. Worth knowing before the agent layer
depends on Trino's health signal for anything.

---

## B. Platform configuration — complete (2026-08-06)

All four gaps below are now closed by D035:

| Control | Completion evidence |
|---------|---------------------|
| Read-only Trino identity | `config/trino/access-rules.json` limits `agent` to `SELECT` on `gold.crypto_dbt` |
| Resource group | `config/trino/resource-groups.json` gives `agent` a bounded lane and preserves a fallback for existing users |
| dbt artifacts | `gold_dbt_coincap_assets.publish_dbt_artifacts` writes fresh metadata to `dbt/artifacts/` after a successful build |
| Explicit allow-list | `config/ai-agent/allowed-tables.json` enumerates the five current dbt Gold relations |

---

## C. Operational

5. ~~**The pipeline is running a day behind, structurally.**~~ **Fixed** — the orchestrator
   moved from 01:30 to 05:30 UTC (D034, incident I20). GitHub's cron drift had grown to
   ~3.5h, so the orchestrator ran before the capture it was meant to follow and every layer
   trailed by a day with nothing failing. The first run on the new schedule should land two
   dates at once (07-31 and 08-01) and bring all three layers current; worth confirming
   before any agent answers a "latest price" question.

6. **Pin the eval dataset.** The golden set (§6) scores execution accuracy against expected
   results, but the tables it queries gain a row set every day, so any aggregate expectation
   silently rots. Iceberg time travel is available on this stack — record a snapshot id per
   golden-set version and query `FOR VERSION AS OF`, alongside the five pins the design
   already requires (model ID, prompt version, allow-list hash, golden-set version, profile).

---

## D. Decisions to make before writing code

7. **New dependencies** (needs approval — CLAUDE.md §5). Phase A needs at least an MCP server
   SDK, the Anthropic SDK, and a Trino-dialect SQL parser for the AST validator (sqlglot is
   the obvious candidate). Recommendation: they live with `ai_agent/`, in its own
   requirements file and its own container, never added to the Airflow image — that is what
   keeps "extends, never modifies" true in the build as well as in the design.
8. **Put the guardrail tests in CI.** CI today is static-only: ruff error rules, `compileall`,
   and `dbt parse`; the pytest suite runs only inside Docker. The AST validator and
   allow-list are pure Python with no stack dependency, and they are the components whose
   bugs have the widest blast radius (F5). A CI job running `pytest ai_agent/` is the
   cheapest guarantee in the whole plan.
9. **Pin the model IDs, and say where the key lives.** R8 requires model IDs in every eval
   report. Decide the pinned models for both profiles at implementation time (the current
   family is Claude Opus 5 / Sonnet 5 rather than anything the design doc names), and add
   `ANTHROPIC_API_KEY` to `.env.example` as a placeholder — never a real value in the repo.
10. **Carry the data caveats into tool output, not just the prompt.** R10 states the prompt
    layer enforces nothing, so sparse coverage and by-design nulls must reach the answer
    through `execute_query`'s result metadata and the freshness caveats from
    `get_table_snapshots` — not through instructions the model may ignore.

---

## Suggested order

**Current position (2026-08-06): B1-B4 are complete.** Next approve D7/D8 (isolated AI
dependencies plus pure-Python guardrail CI), then start the Phase A MCP server. C6 belongs
in the eval harness, D9 at the provider boundary, and D10 in tool result metadata.

The ordering below is retained as the original plan and completion trail.

B4 (allow-list) → B1/B2 (Trino user + resource group) → B3 (artifacts) → D7/D8 (deps + CI)
→ then Phase A code, with C6 and D9/D10 handled inside the eval and tool work respectively.

B1–B3 are configuration and can be done in one small PR before any agent code exists, which
also means the MCP server's first commit can be written against a platform that already
looks the way its design assumes.
