# Pre-AI-Layer Readiness

What the platform still needs before Phase A of the AI-agent layer
([`ai-agent-architecture.md`](ai-agent-architecture.md)) starts. That document is the design
authority; this one is a punch list against the *running system*, checked on 2026-07-31.

The design's own boundary (§2.2) is that the agent layer **extends, never modifies** the
pipeline, and that the only platform-side additions are configuration. Everything in §B
below is one of those additions — none of it exists yet.

---

## A. Settled — no action needed

| Item | Evidence |
|------|----------|
| Hardening complete | H1–H6 all landed; the open-items table in [`incidents.md`](incidents.md) is empty |
| Test suite green | 121 tests pass in Docker (`make test`), including the previously-failing `test_silver_history_backfill` |
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

## B. Platform configuration the design assumes but that doesn't exist

Each of these is small, and each maps to a control the design already specifies.

1. **Read-only `agent` Trino user** — guardrail §4's engine backstop, and F5's mitigation
   (the AST validator is code, and code has bugs). Nothing exists today except the
   `superset` user. Add an analogous block in `config/trino/access-rules.json`.
2. **Trino resource group** — F4's mitigation for runaway scans. `config/trino/config.properties`
   sets only `query.max-memory=512MB`; there is no `resource-groups.properties`. Without it,
   the tool-layer caps are the *only* limit.
3. **dbt artifact publishing** — the MCP server's dbt adapter reads `manifest.json` /
   `catalog.json`, and F6 assumes "Airflow refreshes artifacts post-run". No DAG runs
   `dbt docs generate` today; the only artifacts are whatever a local `dbt run` last left in
   `dbt/target/`, which is untracked. Add a step to the dbt Gold DAG that regenerates them
   into a known location.
4. **Decide the allow-list contents.** Recommendation: allow **`gold.crypto_dbt` only**. It
   is the canonical serving schema (D030), and exposing Spark Gold (`gold.crypto`) as well
   would let one question have two authoritative answers — a silent-wrongness source (F1)
   for no benefit. The two implementations should keep meeting in a dbt test, not in the
   agent's tool surface.

---

## C. Operational issue to settle first

5. **The pipeline is running a day behind, structurally.**

   As of 2026-08-01 00:30 UTC, Silver, Spark Gold and dbt Gold all end at **2026-07-30**
   (107 distinct dates), and Bronze's newest object is 07-30 — while the cloud capture for
   07-31 succeeded.

   The cause is drift, not failure. GitHub's scheduled runs are consistently ~3 hours late:
   the 00:30 UTC capture cron actually completed at **03:40 UTC** on 07-30 and **03:59 UTC**
   on 07-31. The orchestrator runs at 01:30 UTC (D027, chosen after I14 as "an hour after the
   capture"), so it now runs *before* the capture it was meant to follow, and each day's
   snapshot is picked up by the following day's run.

   No data is lost, and H3's freshness check correctly tolerates it — but every "latest" or
   "today" answer is a day stale, which is exactly the kind of thing an agent will state
   confidently. Either move the orchestrator to ~05:30 UTC (a revision to D027/I14), or have
   the capture workflow trigger the sync instead of guessing an offset. **This needs your
   call before the agent starts answering freshness questions**; the runbook's freshness
   caveats depend on it.

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

B4 (allow-list) → C5 (schedule fix) → B1/B2 (Trino user + resource group) → B3 (artifacts)
→ D7/D8 (deps + CI) → then Phase A code, with C6 and D9/D10 handled inside the eval and
tool work respectively.

B1–B3 are configuration and can be done in one small PR before any agent code exists, which
also means the MCP server's first commit can be written against a platform that already
looks the way its design assumes.
