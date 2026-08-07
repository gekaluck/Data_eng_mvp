# Pre-AI-Layer Readiness

The platform prerequisites and implementation gates for Phase A of the AI-agent layer.
[`ai-agent-architecture.md`](ai-agent-architecture.md) is the design authority; this one is
the completion trail and remaining punch list against the *running system*, checked on
2026-08-07.

The design's own boundary (§2.2) is that the agent layer **extends, never modifies** the
pipeline, and that the only platform-side additions are configuration. The §B additions are
complete; this document now records the remaining operational and implementation gates.

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

## D. Implementation decisions

7. ~~**Approve and isolate new dependencies.**~~ **Complete (D036).** MCP, Anthropic,
   Pydantic, SQLGlot, and Trino client constraints live in `ai_agent/requirements.txt`,
   with test tools in `requirements-dev.txt`. Neither file is installed in the Airflow
   image; a dedicated runtime container comes with the runnable server rather than with an
   empty entry point.
8. ~~**Put the guardrail tests in CI.**~~ **Complete.** The isolated Python 3.12 job installs
   only the AI requirements and runs Ruff, `compileall`, and `pytest ai_agent/tests`. The
   current 86 cases cover the allow-list, statement type (including read-only set operations
   refused without being mislabelled as writes), CTE resolution, table scope, time travel,
   structured errors, dbt docs/lineage, live-metadata query shapes, and schema drift
   behavior without starting the lakehouse stack.
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

**Current position (2026-08-07): B1-B4 and D7/D8 are complete; the strict guardrail core,
all five metadata adapters/tools, their MCP stdio plus loopback streamable-HTTP frontends,
and scan-free `explain_query` are implemented (D036–D039).** Next add the shared
budget/audit foundation with capped `sample_rows`, then capped `execute_query`, before the
owned agent loop. C6 belongs in the eval harness, D9 at the provider boundary, and D10 in
query-result metadata.

The ordering below is retained as the original plan and completion trail.

B4 (allow-list) → B1/B2 (Trino user + resource group) → B3 (artifacts) → D7/D8 (deps + CI)
→ then Phase A code, with C6 and D9/D10 handled inside the eval and tool work respectively.

B1–B3 are configuration and can be done in one small PR before any agent code exists, which
also means the MCP server's first commit can be written against a platform that already
looks the way its design assumes.
